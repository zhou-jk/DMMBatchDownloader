"""
DMM Batch Downloader - Multi-threaded downloader for DMM content
Downloads .dcv files from DMM platform with parallel processing support.
"""

import time
import json
import re
from pathlib import Path
from datetime import datetime
from typing import List, Tuple, Optional, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Third-party libraries
from bs4 import BeautifulSoup
from tqdm import tqdm

# Local imports
from shared_utils import (
    check_dependencies, load_configuration, setup_logging, 
    expand_user_path, make_request, ThreadSafeLogger,
    write_failed_id, remove_id_from_file, move_files,
    safe_getsize, safe_remove, check_disk_space,
    read_ids_from_file, setup_directories, CONFIG_FILE
)

# Progress tracking
download_progress_lock = threading.Lock()
download_stats = {
    'completed': 0,
    'failed': 0,
    'in_progress': 0,
    'total': 0
}

class DownloadTask:
    """Represents a single download task for an ID"""
    def __init__(self, cid: str, config, headers_main: Dict, headers_download: Dict, proxies: Optional[Dict] = None):
        self.cid = cid
        self.config = config
        self.headers_main = headers_main
        self.headers_download = headers_download
        self.proxies = proxies
        self.downloaded_files = []
        self.parts_count = 0
        
    def __str__(self):
        return f"DownloadTask({self.cid})"

def update_download_stats(status: str):
    """Update global download statistics (thread-safe)"""
    with download_progress_lock:
        if status == 'start':
            download_stats['in_progress'] += 1
        elif status == 'complete':
            download_stats['completed'] += 1
            download_stats['in_progress'] -= 1
        elif status == 'failed':
            download_stats['failed'] += 1
            download_stats['in_progress'] -= 1

def print_progress_summary():
    """Print current progress summary"""
    with download_progress_lock:
        total = download_stats['total']
        completed = download_stats['completed']
        failed = download_stats['failed']
        in_progress = download_stats['in_progress']
        
        print(f"\n=== Download Progress ===")
        print(f"Total IDs: {total}")
        print(f"Completed: {completed}")
        print(f"Failed: {failed}")
        print(f"In Progress: {in_progress}")
        print(f"Remaining: {total - completed - failed}")
        print("========================\n")

# ---------------- CORE DOWNLOAD FUNCTIONS ----------------

def get_movie_count(cid: str, headers: Dict, max_retries: int, retry_delay: int, proxies: Optional[Dict] = None) -> int:
    """Request the JSON playlist and return the number of parts. Returns -1 on failure."""
    url = 'https://www.dmm.co.jp/service/digitalapi/-/html5/'
    postdata = {
        "action": "playlist", "format": "json", "service": "monthly",
        "browser": "chrome", "shop_name": "premium", "product_id": cid,
        "adult_flag": "1"
    }
    cid_context = f"API movie count for {cid}"
    response = make_request("POST", url, headers, max_retries, retry_delay, cid_context, data=postdata, proxies=proxies)

    if response:
        try:
            data = response.json()
            count = len(data.get('list', {}).get('item', []))
            if count > 0:
                ThreadSafeLogger.debug(f"Found {count} parts via API for {cid}")
                return count
            else:
                ThreadSafeLogger.warning(f"API response for {cid} seems empty or invalid: {data}")
                return -1
        except json.JSONDecodeError as e:
            ThreadSafeLogger.error(f"Failed to decode JSON response for movie count {cid}: {e}")
            return -1
        except Exception as e:
            ThreadSafeLogger.error(f"Unexpected error processing API response for {cid}: {e}")
            return -1
    else:
        return -1

def get_online_cid(cid: str, headers: Dict, max_retries: int, retry_delay: int, proxies: Optional[Dict] = None) -> Optional[str]:
    """Retrieve the online ID ('item_variant') by parsing the video page JavaScript."""
    url = f'https://www.dmm.co.jp/monthly/premium/-/detail/=/cid={cid}/'
    cid_context = f"Online CID page for {cid}"
    response = make_request("GET", url, headers, max_retries, retry_delay, cid_context, proxies=proxies)

    if response:
        try:
            match = re.search(r'item_variant\s*:\s*"([^"]+)"', response.text)
            if match:
                item_variant = match.group(1)
                ThreadSafeLogger.debug(f"Found item_variant {item_variant} for {cid}")
                return item_variant
            else:
                ThreadSafeLogger.warning(f"item_variant not found in JavaScript for {cid}")
                return None
        except Exception as e:
            ThreadSafeLogger.error(f"Error parsing online CID page for {cid}: {e}")
            return None
    else:
        return None

def download_dcv_file(download_url: str, output_dir: str, cid_part_label: str,
                      headers_main: Dict, headers_download: Dict,
                      max_retries: int, retry_delay: int, proxies: Optional[Dict] = None) -> Optional[str]:
    """Get the redirect location and download the .dcv file with a progress bar."""
    ThreadSafeLogger.info(f"Requesting download location for {cid_part_label}...")
    location = None
    cid_context_loc = f"Download location for {cid_part_label}"

    response_loc = make_request("GET", download_url, headers_main, max_retries, retry_delay,
                                cid_context_loc, allow_redirects=False, timeout=60, proxies=proxies)

    if response_loc and 300 <= response_loc.status_code < 400:
        location = response_loc.headers.get('Location')
        if location:
            ThreadSafeLogger.debug(f"Redirect location found for {cid_part_label}: {location}")
        else:
            ThreadSafeLogger.error(f"Redirect status received for {cid_part_label}, but no 'Location' header found.")
            return None
    elif response_loc:
         ThreadSafeLogger.error(f"Unexpected status {response_loc.status_code} when getting download location for {cid_part_label}.")
         return None
    else:
        ThreadSafeLogger.error(f"Failed to get download location response for {cid_part_label}.")
        return None

    try:
        filename = Path(location.split("?")[0]).name
        if not filename.endswith(".dcv"):
            ThreadSafeLogger.warning(f"Extracted filename '{filename}' for {cid_part_label} does not end with .dcv. Using generic name.")
            filename = f"{cid_part_label.replace(' ', '_').replace(':', '_')}.dcv"

        output_file_path = Path(output_dir) / filename
        ThreadSafeLogger.info(f"Starting download: {filename} -> {output_file_path}")

        # Download from content delivery network
        cid_context_dl = f"File download for {cid_part_label}"
        r_dl = make_request("GET", location, headers_download, max_retries, retry_delay,
                            cid_context_dl, stream=True, timeout=300, proxies=proxies)
        if not r_dl:
            return None

        total_size_str = r_dl.headers.get('content-length')
        total_size = int(total_size_str) if total_size_str else None
        chunk_size = 8192

        try:
            with open(output_file_path, 'wb') as f_dl, \
                 tqdm(total=total_size, unit='B', unit_scale=True, unit_divisor=1024,
                      desc=filename, ascii=True, miniters=1, leave=False) as progress_bar:
                for chunk in r_dl.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f_dl.write(chunk)
                        progress_bar.update(len(chunk))

            final_size = safe_getsize(str(output_file_path))
            if final_size is None:
                ThreadSafeLogger.error(f"Download completed for {cid_part_label}, but couldn't get final file size.")
                safe_remove(str(output_file_path), "incomplete download")
                return None
            elif total_size is not None and final_size < total_size:
                ThreadSafeLogger.error(f"Download incomplete for {cid_part_label}: Expected {total_size} bytes, got {final_size} bytes.")
                safe_remove(str(output_file_path), "incomplete download")
                return None
            elif final_size == 0:
                ThreadSafeLogger.error(f"Download for {cid_part_label} resulted in an empty file: {output_file_path}")
                safe_remove(str(output_file_path), "empty download")
                return None
            else:
                ThreadSafeLogger.info(f"Download completed successfully for {cid_part_label}: {output_file_path} ({final_size} bytes)")
                return str(output_file_path)

        except OSError as e:
            ThreadSafeLogger.error(f"File system error during download for {cid_part_label}: {e}")
            safe_remove(str(output_file_path), "filesystem error")
            return None
        finally:
            r_dl.close()

    except Exception as e:
        ThreadSafeLogger.error(f"Unexpected error during file download or handling for {cid_part_label}: {e}")
        if 'output_file_path' in locals() and output_file_path.exists():
             safe_remove(str(output_file_path), "unexpected download error")
        return None

def fetch_and_download_parts(task: DownloadTask) -> Tuple[Optional[List[str]], Optional[int]]:
    """Fetch page details, determine parts, and download them for a single task."""
    cid = task.cid
    config = task.config
    headers_main = task.headers_main
    headers_download = task.headers_download
    proxies = task.proxies
    
    paths = config['Paths']
    settings = config['Settings']
    download_dir = expand_user_path(paths['download_dir'])
    max_retries = settings.getint('max_retries', fallback=3)
    retry_delay = settings.getint('retry_delay_seconds', fallback=5)

    page_url = f'https://www.dmm.co.jp/monthly/premium/-/detail/=/cid={cid}/'
    ThreadSafeLogger.info(f"Processing ID {cid}: Fetching page details from {page_url}")
    cid_context_page = f"Main page for {cid}"
    response_page = make_request("GET", page_url, headers_main, max_retries, retry_delay, cid_context_page, proxies=proxies)

    if not response_page:
        ThreadSafeLogger.error(f"Failed to access main page for ID {cid}. Skipping.")
        return None, None

    try:
        soup = BeautifulSoup(response_page.text, 'lxml')
        options = soup.select('select.js-downloadBitrate option')
        bitrate = None
        resolution_text = 'N/A'

        for opt in options:
            val = opt.get('value')
            text = opt.get_text().lower() if opt.get_text() else ''
            if val and '4k' in text:
                bitrate = val
                resolution_text = opt.get_text()
                ThreadSafeLogger.info(f"Found 4K option: {bitrate} ({resolution_text}) for ID {cid}")
                break

        if not bitrate:
            bitrates = []
            for opt in options:
                val = opt.get('value')
                if val and isinstance(val, str) and val.isdigit():
                    bitrates.append(int(val))
            if bitrates:
                max_bitrate = max(bitrates)
                bitrate = str(max_bitrate)
                max_option = soup.find('option', {'value': bitrate})
                resolution_text = max_option.get_text() if max_option else 'N/A'
                ThreadSafeLogger.info(f"Found max bitrate {bitrate} ({resolution_text}) for ID {cid}")

        if not bitrate:
            ThreadSafeLogger.error(f"No valid bitrate options found for ID: {cid}. Page structure might have changed.")
            body_start = soup.find('body')
            snippet = str(body_start)[:1000] if body_start else 'No body tag found'
            ThreadSafeLogger.debug(f"HTML snippet near bitrate for {cid}: {snippet}")
            return None, None
    except Exception as e:
        ThreadSafeLogger.error(f"Error parsing bitrate for ID {cid}: {e}")
        return None, None

    online_cid = get_online_cid(cid, headers_main, max_retries, retry_delay, proxies)
    if not online_cid:
        ThreadSafeLogger.warning(f"Could not determine online CID for {cid}. Using original ID {cid} for API calls.")
        online_cid = cid

    parts_count = get_movie_count(online_cid, headers_main, max_retries, retry_delay, proxies)
    if parts_count == -1:
        ThreadSafeLogger.error(f"Unable to retrieve the number of parts for ID: {cid} (using online ID: {online_cid}). Skipping download.")
        return None, None
    elif parts_count == 0:
        ThreadSafeLogger.warning(f"API reported 0 parts for ID: {cid} (using online ID: {online_cid}). Skipping download.")
        return [], 0
    else:
        ThreadSafeLogger.info(f"Expecting {parts_count} part(s) for ID {cid}")

    try:
        pid_suffix = None
        bitrate_log_val = bitrate

        if bitrate == '4k':
            pid_suffix = 'dl7'
        else:
            try:
                bitrate_val = int(str(bitrate))
                if bitrate_val >= 4000:
                    pid_suffix = 'dl6'
                else:
                    pid_suffix = 'dl'
            except ValueError:
                ThreadSafeLogger.error(f"Invalid non-'4k' bitrate value found for {cid}: {bitrate}. Cannot determine product ID.")
                return None, None

        if pid_suffix is None:
             ThreadSafeLogger.error(f"Could not determine pid_suffix for bitrate '{bitrate}' for ID {cid}.")
             return None, None

        product_id_for_dl = online_cid + pid_suffix
        ThreadSafeLogger.debug(f"Using product ID {product_id_for_dl} for download requests (Bitrate: {bitrate_log_val}).")

    except Exception as e:
        ThreadSafeLogger.error(f"Error determining product ID suffix for {cid} with bitrate {bitrate}: {e}")
        return None, None

    downloaded_files_list = []
    all_parts_downloaded = True
    for i in range(1, parts_count + 1):
        part_suffix = f" Part {i}" if parts_count > 1 else ""
        cid_part_label = f"{cid}{part_suffix}"

        base_download_url = f'https://www.dmm.co.jp/monthly/premium/-/proxy/=/product_id={product_id_for_dl}/transfer_type=download/rate={bitrate}/drm=1/ftype=dcv'
        part_download_url = base_download_url + (f'/part={i}' if parts_count > 1 else '')

        downloaded_file_path = download_dcv_file(
            part_download_url, download_dir, cid_part_label,
            headers_main, headers_download,
            max_retries, retry_delay, proxies
        )

        if downloaded_file_path:
            downloaded_files_list.append(downloaded_file_path)
        else:
            ThreadSafeLogger.error(f"Download failed for {cid_part_label}. Aborting downloads for this ID.")
            all_parts_downloaded = False
            break

    if all_parts_downloaded and len(downloaded_files_list) == parts_count:
        ThreadSafeLogger.info(f"Successfully downloaded all {parts_count} part(s) for ID {cid}.")
        return downloaded_files_list, parts_count
    else:
        expected_count_str = str(parts_count) if isinstance(parts_count, int) else "unknown"
        ThreadSafeLogger.error(f"Incomplete download for ID {cid}. Expected {expected_count_str}, got {len(downloaded_files_list)}.")
        if not all_parts_downloaded:
            ThreadSafeLogger.info(f"Cleaning up partially downloaded files for failed ID {cid}")
            for file_to_remove in downloaded_files_list:
                 safe_remove(file_to_remove, "partial download cleanup")
            return [], parts_count
        else:
            ThreadSafeLogger.warning(f"Logic error: Download reported success but part count mismatch for {cid}.")
            return downloaded_files_list, parts_count

def process_single_download(task: DownloadTask) -> bool:
    """Process a single download task (thread worker function)."""
    cid = task.cid
    config = task.config
    
    ThreadSafeLogger.info(f"=== Starting Download for ID: {cid} ===")
    update_download_stats('start')
    
    paths = config['Paths']
    download_dir = expand_user_path(paths['download_dir'])
    failed_dir = expand_user_path(paths['failed_dir'])
    failed_ids_file = paths['failed_ids_file']
    ids_file = paths['ids_file']
    
    success = False
    downloaded_files = []
    
    try:
        # Check disk space before starting download
        if not check_disk_space(download_dir, 30.0):
            ThreadSafeLogger.critical(f"Insufficient disk space in download directory: {download_dir} for ID {cid}")
            raise Exception("Insufficient disk space")
        
        # Perform the download
        downloaded_files, parts_count = fetch_and_download_parts(task)
        
        if downloaded_files is None:
            ThreadSafeLogger.error(f"Critical error fetching details for ID {cid}. Cannot proceed.")
        elif not isinstance(parts_count, int) or parts_count < 0:
             ThreadSafeLogger.error(f"Invalid parts_count ({parts_count}) received for ID {cid}. Cannot proceed.")
        elif parts_count == 0:
            ThreadSafeLogger.info(f"ID {cid} has 0 parts. Marking as processed.")
            success = True
        elif not downloaded_files or len(downloaded_files) != parts_count:
            ThreadSafeLogger.error(f"Download incomplete for ID {cid}. Expected {parts_count}, Got {len(downloaded_files)}.")
        else:
            ThreadSafeLogger.info(f"ID {cid}: Download phase complete. Got {len(downloaded_files)} files.")
            success = True
            task.downloaded_files = downloaded_files
            task.parts_count = parts_count
            
    except Exception as e:
        ThreadSafeLogger.error(f"Unexpected error processing ID {cid}: {e}")
    
    # Handle results
    if success:
        update_download_stats('complete')
        ThreadSafeLogger.info(f"=== Download Successful for ID: {cid} ===")
    else:
        update_download_stats('failed')
        write_failed_id(cid, failed_ids_file)
        if downloaded_files:
            move_files(downloaded_files, failed_dir, cid)
        ThreadSafeLogger.error(f"=== Download Failed for ID: {cid} ===")
    
    # Remove ID from processing list
    remove_id_from_file(cid, ids_file)
    return success

# ---------------- MAIN EXECUTION ----------------

def main():
    """Main function to initialize and start multi-threaded downloads."""
    start_time = datetime.now()
    print(f"DMM Batch Downloader started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Check dependencies
    if not check_dependencies():
        print("❌ Dependency check failed. Please install required components.")
        return 1
    
    # Load configuration
    config = load_configuration(CONFIG_FILE)
    paths = config['Paths']
    network = config['Network']
    settings = config['Settings']
    
    # Setup logging
    setup_logging(paths['log_file_all'], paths['log_file_errors'])
    ThreadSafeLogger.info("Logging initialized for batch downloader.")
    ThreadSafeLogger.info(f"Using configuration from: {CONFIG_FILE}")
    
    # Setup directories
    try:
        setup_directories(config)
    except Exception as e:
        print(f"❌ Failed to create required directories: {e}")
        return 1
    
    # Prepare headers
    headers_main = {
        'User-Agent': network['user_agent'],
        'cookie': network['cookie']
    }
    headers_download = {
        'User-Agent': network['user_agent']
    }
    
    # Setup proxy configuration
    proxies = {}
    if network.get('http_proxy', '').strip():
        proxies['http'] = network['http_proxy'].strip()
    if network.get('https_proxy', '').strip():
        proxies['https'] = network['https_proxy'].strip()
    if proxies:
        ThreadSafeLogger.info(f"Using proxy configuration: {proxies}")
    else:
        proxies = None
    
    # Read IDs to process
    ids_to_process = read_ids_from_file(paths['ids_file'])
    if not ids_to_process:
        ThreadSafeLogger.warning(f"No IDs found in '{paths['ids_file']}' or file is empty.")
        return 0
    
    ThreadSafeLogger.info(f"Found {len(ids_to_process)} IDs to download.")
    
    # Initialize download statistics
    download_stats['total'] = len(ids_to_process)
    print_progress_summary()
    
    # Get thread count from configuration
    download_threads = settings.getint('download_threads', fallback=3)
    ThreadSafeLogger.info(f"Starting download with {download_threads} concurrent threads.")
    
    # Create download tasks
    download_tasks = []
    for cid in ids_to_process:
        task = DownloadTask(cid, config, headers_main, headers_download, proxies)
        download_tasks.append(task)
    
    # Execute downloads with thread pool
    successful_downloads = []
    failed_downloads = []
    
    try:
        with ThreadPoolExecutor(max_workers=download_threads, thread_name_prefix="Downloader") as executor:
            # Submit all download tasks
            future_to_task = {executor.submit(process_single_download, task): task for task in download_tasks}
            
            # Process completed downloads
            for future in as_completed(future_to_task):
                task = future_to_task[future]
                try:
                    success = future.result()
                    if success:
                        successful_downloads.append(task.cid)
                        ThreadSafeLogger.info(f"✅ Download completed for ID: {task.cid}")
                    else:
                        failed_downloads.append(task.cid)
                        ThreadSafeLogger.error(f"❌ Download failed for ID: {task.cid}")
                except Exception as exc:
                    failed_downloads.append(task.cid)
                    ThreadSafeLogger.error(f"❌ Download generated exception for ID {task.cid}: {exc}")
                
                # Print periodic progress updates
                if (len(successful_downloads) + len(failed_downloads)) % 5 == 0:
                    print_progress_summary()
    
    except KeyboardInterrupt:
        ThreadSafeLogger.warning("Download process interrupted by user.")
        print("\n⚠️  Download process was interrupted!")
    except Exception as e:
        ThreadSafeLogger.error(f"Unexpected error in main download loop: {e}")
        print(f"❌ Unexpected error occurred: {e}")
    
    # Final summary
    end_time = datetime.now()
    duration = end_time - start_time
    
    print("\n" + "="*60)
    print("DOWNLOAD SUMMARY")
    print("="*60)
    print(f"Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"End Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Duration: {duration}")
    print(f"Total IDs: {len(ids_to_process)}")
    print(f"Successful: {len(successful_downloads)}")
    print(f"Failed: {len(failed_downloads)}")
    print("="*60)
    
    if failed_downloads:
        print("\nFailed Downloads:")
        for cid in failed_downloads:
            print(f"  - {cid}")
    
    ThreadSafeLogger.info(f"Batch download finished. Success: {len(successful_downloads)}, Failed: {len(failed_downloads)}")
    
    return 0 if len(failed_downloads) == 0 else 1

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)
