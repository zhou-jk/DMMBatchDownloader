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

def extract_download_links(soup: BeautifulSoup, cid: str) -> Dict[str, List[str]]:
    """Extract download links from HTML page.
    
    First tries to find <a> tags with download links.
    If not found, parses JavaScript data to construct download URLs.
    
    Returns a dict mapping bitrate to list of download URLs for each part.
    Example: {'4k': ['url_part1', 'url_part2'], '4000': ['url_part1', 'url_part2'], ...}
    """
    download_links: Dict[str, List[str]] = {}
    
    try:
        # Method 1: Find <a> tags with download links
        all_dcv_links = soup.find_all('a', href=re.compile(r'ftype=dcv'))
        ThreadSafeLogger.debug(f"Found {len(all_dcv_links)} <a> dcv links in HTML for {cid}")
        
        for link in all_dcv_links:
            href = link.get('href')
            if not href:
                continue
            
            href_str = str(href)
            rate_match = re.search(r'/rate=([^/]+)/', href_str)
            if rate_match:
                rate = rate_match.group(1)
                if rate not in download_links:
                    download_links[rate] = []
                
                if href_str.startswith('/'):
                    href_str = 'https://www.dmm.co.jp' + href_str
                
                if href_str not in download_links[rate]:
                    download_links[rate].append(href_str)
        
        if download_links:
            ThreadSafeLogger.info(f"Found download links in <a> tags for {cid}: {list(download_links.keys())}")
            return download_links
        
        # Method 2: Parse JavaScript data embedded in page
        # Look for patterns like: 3000 : { product_id : 'xxx', rate : 'yyy', volume : '2', ... }
        scripts = soup.find_all('script')
        for script in scripts:
            script_text = script.string if script.string else ''
            if not script_text:
                continue
            
            # Find all rate blocks: number : { ... }
            # Use non-greedy match to capture each block
            block_pattern = r'(\d+)\s*:\s*\{([^}]+)\}'
            blocks = re.findall(block_pattern, script_text, re.DOTALL)
            
            for block_id, block_content in blocks:
                # Extract product_id from block (handles both \u005f and _ formats)
                pid_match = re.search(r'product(?:\\u005f|_)id\s*:\s*[\'"]([^\'"]+)[\'"]', block_content)
                # Extract rate from block
                rate_match = re.search(r'(?<!_)rate\s*:\s*[\'"]([^\'"]+)[\'"]', block_content)
                # Extract volume (number of parts) from block
                volume_match = re.search(r'volume\s*:\s*[\'"]?(\d+)[\'"]?', block_content)
                
                if pid_match and rate_match:
                    product_id = pid_match.group(1)
                    rate = rate_match.group(1)
                    volume = int(volume_match.group(1)) if volume_match else 1
                    
                    if rate not in download_links:
                        download_links[rate] = []
                    
                    # Generate URLs for each part
                    for part_num in range(1, volume + 1):
                        if volume > 1:
                            # Multiple parts - add part parameter
                            download_url = f'https://www.dmm.co.jp/monthly/premium/-/proxy/=/product_id={product_id}/transfer_type=download/rate={rate}/part={part_num}/drm=1/ftype=dcv'
                        else:
                            # Single part - no part parameter needed
                            download_url = f'https://www.dmm.co.jp/monthly/premium/-/proxy/=/product_id={product_id}/transfer_type=download/rate={rate}/drm=1/ftype=dcv'
                        
                        if download_url not in download_links[rate]:
                            download_links[rate].append(download_url)
                            ThreadSafeLogger.debug(f"Constructed download URL for rate {rate}, part {part_num}: {download_url}")
            
            if download_links:
                break  # Found data, no need to check other scripts
        
        if download_links:
            ThreadSafeLogger.info(f"Extracted download links from JavaScript for {cid}: {list(download_links.keys())}")
            for rate, urls in download_links.items():
                ThreadSafeLogger.info(f"  Rate {rate}: {len(urls)} part(s)")
        else:
            ThreadSafeLogger.warning(f"No download links found in HTML for {cid}")
        
    except Exception as e:
        ThreadSafeLogger.error(f"Error extracting download links for {cid}: {e}")
    
    return download_links

def select_best_download_link(download_links: Dict[str, List[str]]) -> Tuple[Optional[str], Optional[str]]:
    """Select the best quality download link from available options.
    
    Priority: 4k > highest numeric bitrate (handles formats like '4000kb', '6000', etc.)
    Returns (url, bitrate) tuple.
    """
    if not download_links:
        return None, None
    
    # Priority 1: 4K (check various formats)
    for rate_key in download_links.keys():
        if '4k' in rate_key.lower():
            if download_links[rate_key]:
                return download_links[rate_key][0], rate_key
    
    # Priority 2: Highest numeric bitrate (extract number from formats like '4000kb', '6000', etc.)
    rate_values = []
    for rate in download_links.keys():
        # Extract numeric part from rate string (e.g., '4000kb' -> 4000, '6000' -> 6000)
        num_match = re.search(r'(\d+)', rate)
        if num_match:
            rate_values.append((int(num_match.group(1)), rate))
    
    if rate_values:
        # Sort by numeric value descending, pick highest
        rate_values.sort(key=lambda x: x[0], reverse=True)
        best_rate = rate_values[0][1]
        if download_links[best_rate]:
            return download_links[best_rate][0], best_rate
    
    # Fallback: return first available
    for rate, urls in download_links.items():
        if urls:
            return urls[0], rate
    
    return None, None

def download_dcv_file(download_url: str, output_dir: str, cid_part_label: str,
                      headers_main: Dict, headers_download: Dict,
                      max_retries: int, retry_delay: int, proxies: Optional[Dict] = None) -> Optional[str]:
    """Get the redirect location and download the .dcv file with a progress bar."""
    ThreadSafeLogger.info(f"Requesting download location for {cid_part_label}...")
    ThreadSafeLogger.info(f"Download URL: {download_url}")
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
        
        # Try to find download links directly from HTML
        download_links = extract_download_links(soup, cid)
        
        if download_links:
            # Found download links in HTML - use them directly
            ThreadSafeLogger.info(f"Found {len(download_links)} bitrate option(s) in HTML for ID {cid}")
            
            # Select best quality link
            best_link, selected_bitrate = select_best_download_link(download_links)
            if best_link and selected_bitrate:
                ThreadSafeLogger.info(f"Selected bitrate: {selected_bitrate} for ID {cid}")
                
                # Get all parts for selected bitrate
                part_links = download_links.get(selected_bitrate, [])
                parts_count = len(part_links)
                
                if parts_count == 0:
                    ThreadSafeLogger.error(f"No download links for selected bitrate {selected_bitrate} for ID {cid}")
                    return None, None
                
                downloaded_files = []
                for i, link in enumerate(part_links, 1):
                    part_suffix = f" Part {i}" if parts_count > 1 else ""
                    cid_part_label = f"{cid}{part_suffix}"
                    downloaded_file = download_dcv_file(
                        link, download_dir, cid_part_label,
                        headers_main, headers_download,
                        max_retries, retry_delay, proxies
                    )
                    if downloaded_file:
                        downloaded_files.append(downloaded_file)
                    else:
                        ThreadSafeLogger.error(f"Download failed for {cid_part_label}")
                        # Clean up partial downloads
                        for f in downloaded_files:
                            safe_remove(f, "partial download cleanup")
                        return None, None
                
                ThreadSafeLogger.info(f"Successfully downloaded all {parts_count} part(s) for ID {cid}")
                return downloaded_files, parts_count
            else:
                ThreadSafeLogger.error(f"Could not select best download link for ID {cid}")
                return None, None
        else:
            ThreadSafeLogger.error(f"No download links found in HTML for ID {cid}")
            return None, None
            
    except Exception as e:
        ThreadSafeLogger.error(f"Error parsing page for ID {cid}: {e}")
        return None, None

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
    # Add X-Forwarded-For header if configured
    x_forwarded_for = network.get('x_forwarded_for', '').strip()
    if x_forwarded_for:
        headers_main['X-Forwarded-For'] = x_forwarded_for
        ThreadSafeLogger.info(f"Using X-Forwarded-For: {x_forwarded_for}")
    headers_download = {
        'User-Agent': network['user_agent']
    }
    
    # Setup proxy configuration
    proxies = {}
    proxy_url = network.get('proxy', '').strip()
    if proxy_url:
        proxies['http'] = proxy_url
        proxies['https'] = proxy_url
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
