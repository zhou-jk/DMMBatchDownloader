"""
DMM Batch Checker - Check which IDs have missing downloads
Follows the same processing flow as batch_downloader.py but instead of
downloading, checks if .dcv files already exist and logs missing IDs.

Usage:
  1. Run on each server that has downloaded files
  2. Collect the missing_ids.txt output
  3. Use the combined missing IDs list for re-downloading on the new server
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

# Local imports
from shared_utils import (
    load_configuration, setup_logging, 
    expand_user_path, make_request, ThreadSafeLogger,
    read_ids_from_file, setup_directories, CONFIG_FILE
)

# Progress tracking
check_progress_lock = threading.Lock()
check_stats = {
    'completed': 0,
    'missing': 0,
    'existing': 0,
    'failed': 0,
    'in_progress': 0,
    'total': 0
}

# Thread-safe file writing lock
file_write_lock = threading.Lock()

class CheckTask:
    """Represents a single check task for an ID"""
    def __init__(self, cid: str, config, headers_main: Dict, headers_download: Dict, proxies: Optional[Dict] = None):
        self.cid = cid
        self.config = config
        self.headers_main = headers_main
        self.headers_download = headers_download
        self.proxies = proxies

    def __str__(self):
        return f"CheckTask({self.cid})"

def update_check_stats(status: str):
    """Update global check statistics (thread-safe)"""
    with check_progress_lock:
        if status == 'start':
            check_stats['in_progress'] += 1
        elif status == 'existing':
            check_stats['existing'] += 1
            check_stats['completed'] += 1
            check_stats['in_progress'] -= 1
        elif status == 'missing':
            check_stats['missing'] += 1
            check_stats['completed'] += 1
            check_stats['in_progress'] -= 1
        elif status == 'failed':
            check_stats['failed'] += 1
            check_stats['completed'] += 1
            check_stats['in_progress'] -= 1

def print_progress_summary():
    """Print current progress summary"""
    with check_progress_lock:
        total = check_stats['total']
        completed = check_stats['completed']
        existing = check_stats['existing']
        missing = check_stats['missing']
        failed = check_stats['failed']
        in_progress = check_stats['in_progress']

        print(f"\n=== Check Progress ===")
        print(f"Total IDs: {total}")
        print(f"Checked: {completed}")
        print(f"  Existing: {existing}")
        print(f"  Missing:  {missing}")
        print(f"  Failed:   {failed}")
        print(f"In Progress: {in_progress}")
        print(f"Remaining: {total - completed}")
        print("======================\n")

def write_id_to_file(cid: str, filepath: str):
    """Thread-safe write of an ID to a file"""
    with file_write_lock:
        with open(filepath, 'a', encoding='utf-8') as f:
            f.write(cid + '\n')

# ---------------- CORE CHECK FUNCTIONS ----------------

def extract_download_links(soup: BeautifulSoup, cid: str) -> Dict[str, List[str]]:
    """Extract download links from HTML page.
    
    First tries to find <a> tags with download links.
    If not found, parses JavaScript data to construct download URLs.
    
    Returns a dict mapping bitrate to list of download URLs for each part.
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
        scripts = soup.find_all('script')
        for script in scripts:
            script_text = script.string if script.string else ''
            if not script_text:
                continue

            block_pattern = r'(\d+)\s*:\s*\{([^}]+)\}'
            blocks = re.findall(block_pattern, script_text, re.DOTALL)

            for block_id, block_content in blocks:
                pid_match = re.search(r'product(?:\\u005f|_)id\s*:\s*[\'"]([^\'"]+)[\'"]', block_content)
                rate_match = re.search(r'(?<!_)rate\s*:\s*[\'"]([^\'"]+)[\'"]', block_content)
                volume_match = re.search(r'volume\s*:\s*[\'"]?(\d+)[\'"]?', block_content)

                if pid_match and rate_match:
                    product_id = pid_match.group(1)
                    rate = rate_match.group(1)
                    volume = int(volume_match.group(1)) if volume_match else 1

                    if rate not in download_links:
                        download_links[rate] = []

                    for part_num in range(1, volume + 1):
                        if volume > 1:
                            download_url = f'https://www.dmm.co.jp/monthly/premium/-/proxy/=/product_id={product_id}/transfer_type=download/rate={rate}/part={part_num}/drm=1/ftype=dcv'
                        else:
                            download_url = f'https://www.dmm.co.jp/monthly/premium/-/proxy/=/product_id={product_id}/transfer_type=download/rate={rate}/drm=1/ftype=dcv'

                        if download_url not in download_links[rate]:
                            download_links[rate].append(download_url)
                            ThreadSafeLogger.debug(f"Constructed download URL for rate {rate}, part {part_num}: {download_url}")

            if download_links:
                break

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
    Priority: 4k > highest numeric bitrate
    Returns (url, bitrate) tuple (first URL only, for identification).
    """
    if not download_links:
        return None, None

    for rate_key in download_links.keys():
        if '4k' in rate_key.lower():
            if download_links[rate_key]:
                return download_links[rate_key][0], rate_key

    rate_values = []
    for rate in download_links.keys():
        num_match = re.search(r'(\d+)', rate)
        if num_match:
            rate_values.append((int(num_match.group(1)), rate))

    if rate_values:
        rate_values.sort(key=lambda x: x[0], reverse=True)
        best_rate = rate_values[0][1]
        if download_links[best_rate]:
            return download_links[best_rate][0], best_rate

    for rate, urls in download_links.items():
        if urls:
            return urls[0], rate

    return None, None

def check_dcv_file(download_url: str, download_dir: str, cid_part_label: str,
                   headers_main: Dict, max_retries: int, retry_delay: int,
                   proxies: Optional[Dict] = None) -> Tuple[Optional[str], bool]:
    """Follow redirects to resolve the final .dcv filename and check if it exists.
    
    Returns (filename, exists) tuple.
    - filename: the resolved .dcv filename (or None if redirect failed)
    - exists: True if the file already exists in download_dir
    """
    ThreadSafeLogger.info(f"Checking download location for {cid_part_label}...")
    ThreadSafeLogger.debug(f"Download URL: {download_url}")

    # Redirect #1: dmm.co.jp/proxy/... -> str.dmm.com/...
    cid_context_r1 = f"Redirect #1 for {cid_part_label}"
    response_r1 = make_request("GET", download_url, headers_main, max_retries, retry_delay,
                               cid_context_r1, allow_redirects=False, timeout=60, proxies=proxies)

    if not response_r1 or not (300 <= response_r1.status_code < 400):
        status = response_r1.status_code if response_r1 else 'No response'
        ThreadSafeLogger.error(f"Redirect #1 failed for {cid_part_label}: {status}")
        return None, False

    url_r1 = response_r1.headers.get('Location')
    if not url_r1:
        ThreadSafeLogger.error(f"Redirect #1 for {cid_part_label}: no 'Location' header found.")
        return None, False
    ThreadSafeLogger.debug(f"Redirect #1 for {cid_part_label}: {url_r1[:120]}...")

    # Redirect #2: str.dmm.com/... -> stcXXX.dmm.com/...
    cid_context_r2 = f"Redirect #2 for {cid_part_label}"
    response_r2 = make_request("GET", url_r1, headers_main, max_retries, retry_delay,
                               cid_context_r2, allow_redirects=False, timeout=60, proxies=proxies)

    if not response_r2 or not (300 <= response_r2.status_code < 400):
        status = response_r2.status_code if response_r2 else 'No response'
        ThreadSafeLogger.error(f"Redirect #2 failed for {cid_part_label}: {status}")
        return None, False

    final_url = response_r2.headers.get('Location')
    if not final_url:
        ThreadSafeLogger.error(f"Redirect #2 for {cid_part_label}: no 'Location' header found.")
        return None, False
    ThreadSafeLogger.debug(f"Final URL for {cid_part_label}: {final_url[:120]}...")

    # Extract filename from final URL
    try:
        filename = Path(final_url.split("?")[0]).name
        if not filename.endswith(".dcv"):
            ThreadSafeLogger.warning(f"Extracted filename '{filename}' for {cid_part_label} does not end with .dcv.")
            filename = f"{cid_part_label.replace(' ', '_').replace(':', '_')}.dcv"
    except Exception as e:
        ThreadSafeLogger.error(f"Error extracting filename for {cid_part_label}: {e}")
        return None, False

    # Check if file exists in download directory
    file_path = Path(download_dir) / filename
    exists = file_path.exists() and file_path.stat().st_size > 0

    if exists:
        size = file_path.stat().st_size
        ThreadSafeLogger.info(f"EXISTS: {filename} ({size:,} bytes) for {cid_part_label}")
    else:
        ThreadSafeLogger.info(f"MISSING: {filename} for {cid_part_label}")

    return filename, exists

def check_parts(task: CheckTask) -> Tuple[str, Optional[List[str]], Optional[List[str]]]:
    """Fetch page details, determine parts, and check existence for a single task.
    
    Returns:
        (status, existing_files, missing_files)
        status: 'all_exist' | 'some_missing' | 'all_missing' | 'error'
    """
    cid = task.cid
    config = task.config
    headers_main = task.headers_main
    proxies = task.proxies

    paths = config['Paths']
    settings = config['Settings']
    download_dir = expand_user_path(paths['download_dir'])
    max_retries = settings.getint('max_retries', fallback=3)
    retry_delay = settings.getint('retry_delay_seconds', fallback=5)

    page_url = f'https://www.dmm.co.jp/monthly/premium/-/detail/=/cid={cid}/'
    ThreadSafeLogger.info(f"Checking ID {cid}: Fetching page details from {page_url}")
    cid_context_page = f"Main page for {cid}"
    response_page = make_request("GET", page_url, headers_main, max_retries, retry_delay, cid_context_page, proxies=proxies)

    if not response_page:
        ThreadSafeLogger.error(f"Failed to access main page for ID {cid}. Skipping.")
        return 'error', None, None

    try:
        soup = BeautifulSoup(response_page.text, 'lxml')

        # Extract download links from HTML
        download_links = extract_download_links(soup, cid)

        if not download_links:
            ThreadSafeLogger.error(f"No download links found in HTML for ID {cid}")
            return 'error', None, None

        ThreadSafeLogger.info(f"Found {len(download_links)} bitrate option(s) for ID {cid}")

        # Select best quality link
        _, selected_bitrate = select_best_download_link(download_links)
        if not selected_bitrate:
            ThreadSafeLogger.error(f"Could not select best download link for ID {cid}")
            return 'error', None, None

        ThreadSafeLogger.info(f"Selected bitrate: {selected_bitrate} for ID {cid}")

        # Get all parts for selected bitrate
        part_links = download_links.get(selected_bitrate, [])
        parts_count = len(part_links)

        if parts_count == 0:
            ThreadSafeLogger.error(f"No download links for selected bitrate {selected_bitrate} for ID {cid}")
            return 'error', None, None

        # Check each part
        existing_files = []
        missing_files = []

        for i, link in enumerate(part_links, 1):
            part_suffix = f" Part {i}" if parts_count > 1 else ""
            cid_part_label = f"{cid}{part_suffix}"

            filename, exists = check_dcv_file(
                link, download_dir, cid_part_label,
                headers_main, max_retries, retry_delay, proxies
            )

            if filename is None:
                # Redirect failed - treat as missing
                missing_files.append(f"{cid_part_label} (redirect failed)")
            elif exists:
                existing_files.append(filename)
            else:
                missing_files.append(filename)

        # Determine overall status
        if not missing_files:
            status = 'all_exist'
        elif not existing_files:
            status = 'all_missing'
        else:
            status = 'some_missing'

        return status, existing_files, missing_files

    except Exception as e:
        ThreadSafeLogger.error(f"Error parsing page for ID {cid}: {e}")
        return 'error', None, None

def process_single_check(task: CheckTask, missing_ids_file: str, existing_ids_file: str) -> str:
    """Process a single check task (thread worker function).
    
    Returns: 'existing' | 'missing' | 'failed'
    """
    cid = task.cid

    ThreadSafeLogger.info(f"=== Checking ID: {cid} ===")
    update_check_stats('start')

    try:
        status, existing_files, missing_files = check_parts(task)

        if status == 'all_exist':
            ThreadSafeLogger.info(f"ALL EXIST: ID {cid} - {len(existing_files)} file(s) found")
            write_id_to_file(cid, existing_ids_file)
            update_check_stats('existing')
            return 'existing'

        elif status in ('some_missing', 'all_missing'):
            msg = f"MISSING: ID {cid}"
            if existing_files:
                msg += f" - {len(existing_files)} exist, {len(missing_files)} missing"
            else:
                msg += f" - all {len(missing_files)} file(s) missing"
            ThreadSafeLogger.info(msg)
            
            if missing_files:
                for mf in missing_files:
                    ThreadSafeLogger.info(f"  Missing: {mf}")
            
            write_id_to_file(cid, missing_ids_file)
            update_check_stats('missing')
            return 'missing'

        else:  # 'error'
            ThreadSafeLogger.error(f"ERROR: Could not check ID {cid}")
            write_id_to_file(cid, missing_ids_file)  # Treat errors as missing (needs re-check)
            update_check_stats('failed')
            return 'failed'

    except Exception as e:
        ThreadSafeLogger.error(f"Unexpected error checking ID {cid}: {e}")
        write_id_to_file(cid, missing_ids_file)
        update_check_stats('failed')
        return 'failed'

# ---------------- MAIN EXECUTION ----------------

def main():
    """Main function to initialize and start multi-threaded checks."""
    start_time = datetime.now()
    print(f"DMM Batch Checker started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("This script checks which IDs have missing downloads (no actual downloading).\n")

    # Load configuration
    config = load_configuration(CONFIG_FILE)
    paths = config['Paths']
    network = config['Network']
    settings = config['Settings']

    # Setup logging
    setup_logging(paths['log_file_all'], paths['log_file_errors'])
    ThreadSafeLogger.info("Logging initialized for batch checker.")
    ThreadSafeLogger.info(f"Using configuration from: {CONFIG_FILE}")

    # Setup directories
    try:
        setup_directories(config)
    except Exception as e:
        print(f"Failed to create required directories: {e}")
        return 1

    # Output files for results
    missing_ids_file = 'missing_ids.txt'
    existing_ids_file = 'existing_ids.txt'

    # Clear output files at start
    for f in [missing_ids_file, existing_ids_file]:
        with open(f, 'w', encoding='utf-8') as fh:
            fh.write('')  # Truncate

    download_dir = expand_user_path(paths['download_dir'])
    print(f"Checking download directory: {download_dir}")

    if not Path(download_dir).exists():
        print(f"WARNING: Download directory does not exist: {download_dir}")
        print("All IDs will be reported as missing.\n")

    # Prepare headers
    headers_main = {
        'User-Agent': network['user_agent'],
        'cookie': network['cookie']
    }
    x_forwarded_for = network.get('x_forwarded_for', '').strip()
    if x_forwarded_for:
        headers_main['X-Forwarded-For'] = x_forwarded_for
        ThreadSafeLogger.info(f"Using X-Forwarded-For: {x_forwarded_for}")
    headers_download = {
        'User-Agent': network['user_agent']
    }
    if x_forwarded_for:
        headers_download['X-Forwarded-For'] = x_forwarded_for

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
        print("No IDs to check.")
        return 0

    ThreadSafeLogger.info(f"Found {len(ids_to_process)} IDs to check.")
    print(f"Found {len(ids_to_process)} IDs to check.\n")

    # Initialize check statistics
    check_stats['total'] = len(ids_to_process)
    print_progress_summary()

    # Get thread count from configuration
    check_threads = settings.getint('download_threads', fallback=3)
    ThreadSafeLogger.info(f"Starting check with {check_threads} concurrent threads.")

    # Create check tasks
    check_tasks = []
    for cid in ids_to_process:
        task = CheckTask(cid, config, headers_main, headers_download, proxies)
        check_tasks.append(task)

    # Execute checks with thread pool
    existing_ids = []
    missing_ids = []
    failed_ids = []

    try:
        with ThreadPoolExecutor(max_workers=check_threads, thread_name_prefix="Checker") as executor:
            future_to_task = {
                executor.submit(process_single_check, task, missing_ids_file, existing_ids_file): task
                for task in check_tasks
            }

            for future in as_completed(future_to_task):
                task = future_to_task[future]
                try:
                    result = future.result()
                    if result == 'existing':
                        existing_ids.append(task.cid)
                    elif result == 'missing':
                        missing_ids.append(task.cid)
                    else:
                        failed_ids.append(task.cid)
                except Exception as exc:
                    failed_ids.append(task.cid)
                    ThreadSafeLogger.error(f"Check generated exception for ID {task.cid}: {exc}")
                    write_id_to_file(task.cid, missing_ids_file)

                # Print periodic progress updates
                total_done = len(existing_ids) + len(missing_ids) + len(failed_ids)
                if total_done % 10 == 0:
                    print_progress_summary()

    except KeyboardInterrupt:
        ThreadSafeLogger.warning("Check process interrupted by user.")
        print("\nCheck process was interrupted!")
    except Exception as e:
        ThreadSafeLogger.error(f"Unexpected error in main check loop: {e}")
        print(f"Unexpected error occurred: {e}")

    # Final summary
    end_time = datetime.now()
    duration = end_time - start_time

    print("\n" + "=" * 60)
    print("CHECK SUMMARY")
    print("=" * 60)
    print(f"Start Time:  {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"End Time:    {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Duration:    {duration}")
    print(f"Total IDs:   {len(ids_to_process)}")
    print(f"Existing:    {len(existing_ids)}")
    print(f"Missing:     {len(missing_ids)}")
    print(f"Failed:      {len(failed_ids)}")
    print("=" * 60)
    print(f"\nResults saved to:")
    print(f"  Missing IDs -> {missing_ids_file}")
    print(f"  Existing IDs -> {existing_ids_file}")

    if missing_ids:
        print(f"\nMissing IDs ({len(missing_ids)}):")
        for cid in missing_ids:
            print(f"  - {cid}")

    if failed_ids:
        print(f"\nFailed to check ({len(failed_ids)}):")
        for cid in failed_ids:
            print(f"  - {cid}")

    ThreadSafeLogger.info(f"Batch check finished. Existing: {len(existing_ids)}, Missing: {len(missing_ids)}, Failed: {len(failed_ids)}")

    return 0

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)
