#!/usr/bin/env python3
"""
DMM Integrated Processor - Complete workflow for DMM video processing
Downloads, decrypts, and uploads DMM content with multi-threading support.

This script combines the functionality of:
- batch_downloader.py: Downloads .dcv files from DMM
- batch_decryptor.py: Decrypts .dcv files to .mkv format
- rclone upload: Uploads decrypted files to remote storage

Features:
- Multi-threaded processing pipeline
- Complete workflow automation
- Comprehensive error handling and retry logic
- Progress tracking and monitoring
- Disk space management
- Failed task recovery
"""

import os
import sys
import time
import json
import re
import shutil
import subprocess
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Tuple, Optional, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from tempfile import TemporaryDirectory
from dataclasses import dataclass
from enum import Enum

# Third-party libraries
from bs4 import BeautifulSoup, Tag
from tqdm import tqdm

# Local imports
from shared_utils import (
    check_dependencies, load_configuration, setup_logging, 
    expand_user_path, make_request, ThreadSafeLogger,
    write_failed_id, remove_id_from_file, move_files,
    safe_getsize, safe_remove, check_disk_space,
    read_ids_from_file, setup_directories, CONFIG_FILE,
    has_conformance_errors, get_media_duration, DECRYPT_CONN_ERROR_MSG
)

# Task status enumeration
class TaskStatus(Enum):
    PENDING = "pending"
    DOWNLOADING = "downloading"
    DECRYPTING = "decrypting"
    UPLOADING = "uploading"
    COMPLETED = "completed"
    FAILED = "failed"

@dataclass
class ProcessingStats:
    """Global processing statistics"""
    total: int = 0
    completed: int = 0
    failed: int = 0
    downloading: int = 0
    decrypting: int = 0
    uploading: int = 0
    
    def get_remaining(self) -> int:
        return self.total - self.completed - self.failed

# Global stats and lock
stats_lock = threading.Lock()
processing_stats = ProcessingStats()

class IntegratedTask:
    """Represents a complete processing task for a single video ID"""
    
    def __init__(self, cid: str, config, headers_main: Dict, headers_download: Dict, 
                 decrypt_tool_path: str, rclone_config: Optional[Dict] = None, proxies: Optional[Dict] = None,
                 output_dir: Optional[str] = None):
        self.cid = cid
        self.config = config
        self.headers_main = headers_main
        self.headers_download = headers_download
        self.decrypt_tool_path = decrypt_tool_path
        self.rclone_config = rclone_config
        self.proxies = proxies
        self.output_dir = output_dir  # Local output directory when rclone is not configured
        
        # Task state
        self.status = TaskStatus.PENDING
        self.downloaded_files = []
        self.decrypted_files = []
        self.uploaded_files = []
        self.parts_count = 0
        self.error_message = ""
        self.start_time = None
        self.end_time = None
        
    def __str__(self):
        return f"IntegratedTask({self.cid}, {self.status.value})"

def update_processing_stats(status_change: str):
    """Update global processing statistics (thread-safe)"""
    with stats_lock:
        if status_change == 'start_download':
            processing_stats.downloading += 1
        elif status_change == 'finish_download':
            processing_stats.downloading -= 1
            processing_stats.decrypting += 1
        elif status_change == 'finish_decrypt':
            processing_stats.decrypting -= 1
            processing_stats.uploading += 1
        elif status_change == 'complete':
            processing_stats.uploading -= 1
            processing_stats.completed += 1
        elif status_change == 'fail_download':
            processing_stats.downloading -= 1
            processing_stats.failed += 1
        elif status_change == 'fail_decrypt':
            processing_stats.decrypting -= 1
            processing_stats.failed += 1
        elif status_change == 'fail_upload':
            processing_stats.uploading -= 1
            processing_stats.failed += 1

def print_processing_progress():
    """Print current processing progress summary"""
    with stats_lock:
        stats = processing_stats
        print(f"\n=== Processing Progress ===")
        print(f"Total IDs: {stats.total}")
        print(f"Completed: {stats.completed}")
        print(f"Failed: {stats.failed}")
        print(f"Downloading: {stats.downloading}")
        print(f"Decrypting: {stats.decrypting}")
        print(f"Uploading: {stats.uploading}")
        print(f"Remaining: {stats.get_remaining()}")
        print("===========================\n")

# ---------------- DMM DOWNLOAD FUNCTIONS ----------------

def get_movie_count(cid: str, headers: Dict, max_retries: int, retry_delay: int, 
                   proxies: Optional[Dict] = None) -> int:
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

def get_online_cid(cid: str, headers: Dict, max_retries: int, retry_delay: int, 
                  proxies: Optional[Dict] = None) -> Optional[str]:
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

def download_video_parts(task: IntegratedTask, temp_dir: str) -> bool:
    """Download all parts for a video ID to temporary directory"""
    cid = task.cid
    config = task.config
    headers_main = task.headers_main
    headers_download = task.headers_download
    proxies = task.proxies
    
    paths = config['Paths']
    settings = config['Settings']
    max_retries = settings.getint('max_retries', fallback=3)
    retry_delay = settings.getint('retry_delay_seconds', fallback=5)

    page_url = f'https://www.dmm.co.jp/monthly/premium/-/detail/=/cid={cid}/'
    ThreadSafeLogger.info(f"Processing ID {cid}: Fetching page details from {page_url}")
    cid_context_page = f"Main page for {cid}"
    response_page = make_request("GET", page_url, headers_main, max_retries, retry_delay, 
                                cid_context_page, proxies=proxies)

    if not response_page:
        ThreadSafeLogger.error(f"Failed to access main page for ID {cid}. Skipping.")
        task.error_message = "Failed to access main page"
        return False

    try:
        soup = BeautifulSoup(response_page.text, 'lxml')
        
        # Extract download links directly from HTML
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
                task.parts_count = parts_count
                
                if parts_count == 0:
                    ThreadSafeLogger.error(f"No download links for selected bitrate {selected_bitrate} for ID {cid}")
                    task.error_message = "No download links found"
                    return False
                
                downloaded_files_list = []
                for i, link in enumerate(part_links, 1):
                    part_suffix = f" Part {i}" if parts_count > 1 else ""
                    cid_part_label = f"{cid}{part_suffix}"
                    
                    downloaded_file_path = download_dcv_file(
                        link, temp_dir, cid_part_label,
                        headers_main, headers_download, max_retries, retry_delay, proxies
                    )
                    
                    if downloaded_file_path:
                        downloaded_files_list.append(downloaded_file_path)
                    else:
                        ThreadSafeLogger.error(f"Download failed for {cid_part_label}. Aborting downloads for this ID.")
                        # Clean up partial downloads
                        for file_to_remove in downloaded_files_list:
                            safe_remove(file_to_remove, "partial download cleanup")
                        task.error_message = f"Download failed for {cid_part_label}"
                        return False
                
                ThreadSafeLogger.info(f"Successfully downloaded all {parts_count} part(s) for ID {cid}.")
                task.downloaded_files = downloaded_files_list
                return True
            else:
                ThreadSafeLogger.error(f"Could not select best download link for ID {cid}")
                task.error_message = "Could not select best download link"
                return False
        else:
            ThreadSafeLogger.error(f"No download links found in HTML for ID {cid}")
            task.error_message = "No download links found in HTML"
            return False
            
    except Exception as e:
        ThreadSafeLogger.error(f"Error parsing page for ID {cid}: {e}")
        task.error_message = f"Error parsing page: {e}"
        return False

# ---------------- DECRYPTION FUNCTIONS ----------------

def decrypt_dcv_file(dcv_file_path: str, output_dir: str, decrypt_tool_path: str, 
                    max_retries: int, decrypt_retry_delay: int) -> Optional[str]:
    """Decrypt a single .dcv file with verification and retry logic."""
    if not Path(dcv_file_path).exists():
        ThreadSafeLogger.error(f"DCV file not found: {dcv_file_path}")
        return None

    # Prepare output path
    base_name = Path(dcv_file_path).stem
    output_mkv_path = Path(output_dir) / f"{base_name}.mkv"

    # Remove existing output file if present
    if output_mkv_path.exists():
        safe_remove(str(output_mkv_path), "pre-decryption cleanup")

    # Pre-decryption conformance check
    ThreadSafeLogger.info(f"Performing conformance check on {Path(dcv_file_path).name}...")
    if has_conformance_errors(dcv_file_path):
        ThreadSafeLogger.error(f"Conformance check failed for {Path(dcv_file_path).name}. File is likely corrupt.")
        return None

    # Decryption command
    cmd = [decrypt_tool_path, "decrypt", "-i", dcv_file_path, "-o", str(output_mkv_path), "-t", "dmm"]
    ThreadSafeLogger.info(f"Decrypting {Path(dcv_file_path).name}...")
    ThreadSafeLogger.debug(f"Running command: {' '.join(cmd)}")

    # Decryption retry loop
    for attempt in range(max_retries):
        should_retry = False
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=False, 
                                  encoding='utf-8', errors='replace', timeout=3600)  # 1 hour timeout
            
            ThreadSafeLogger.debug(f"Decryption attempt {attempt + 1}/{max_retries} stdout for {Path(dcv_file_path).name}:\n{result.stdout}")
            if result.stderr:
                ThreadSafeLogger.debug(f"Decryption attempt {attempt + 1}/{max_retries} stderr for {Path(dcv_file_path).name}:\n{result.stderr}")

            if result.returncode != 0:
                stderr_lower = result.stderr.lower() if result.stderr else ""
                error_msg = f"Decryption command failed for {Path(dcv_file_path).name} (Attempt {attempt + 1}/{max_retries}, Return Code: {result.returncode})."

                if DECRYPT_CONN_ERROR_MSG in stderr_lower:
                    error_msg += f" jav-it.exe reported '{DECRYPT_CONN_ERROR_MSG}'."
                    if attempt < max_retries - 1:
                        ThreadSafeLogger.warning(error_msg + f" Will retry after {decrypt_retry_delay} seconds.")
                        should_retry = True
                    else:
                        ThreadSafeLogger.error(error_msg + " Max retries reached.")
                        return None
                else:
                    ThreadSafeLogger.error(error_msg)
                    ThreadSafeLogger.error(f"Stderr:\n{result.stderr}")
                    return None

                if not should_retry:
                    break
            else:
                stdout_lower = result.stdout.lower() if result.stdout else ""
                stderr_lower = result.stderr.lower() if result.stderr else ""
                if "[ ok ] decryption complete!" in stderr_lower or \
                   "[ ok ] decryption complete!" in stdout_lower:
                    ThreadSafeLogger.info(f"Decryption command successful for {Path(dcv_file_path).name} on attempt {attempt + 1}.")
                    break
                else:
                    ThreadSafeLogger.warning(f"Decryption command finished with code 0 for {Path(dcv_file_path).name} (Attempt {attempt + 1}), but success message not found.")
                    ThreadSafeLogger.warning(f"Stderr:\n{result.stderr}")
                    return None

        except subprocess.TimeoutExpired:
            ThreadSafeLogger.error(f"Decryption timeout for {Path(dcv_file_path).name} on attempt {attempt + 1}")
            return None
        except FileNotFoundError:
            ThreadSafeLogger.critical(f"Decryption executable '{decrypt_tool_path}' not found.")
            return None
        except Exception as e:
            ThreadSafeLogger.error(f"Error running decryption command for {Path(dcv_file_path).name} on attempt {attempt + 1}: {e}")
            return None

        if should_retry:
            time.sleep(decrypt_retry_delay)

    # Verify decryption output
    if not output_mkv_path.exists():
        ThreadSafeLogger.error(f"Decryption reported success but output file not found: {output_mkv_path}")
        return None

    # Size verification with variable tolerance
    size_in = safe_getsize(dcv_file_path)
    size_out = safe_getsize(str(output_mkv_path))

    if size_in is None or size_out is None:
        ThreadSafeLogger.error(f"Could not get file sizes to verify {Path(dcv_file_path).name}. Deleting output.")
        safe_remove(str(output_mkv_path), "size verification failed")
        return None

    if size_in > 0:
        MB_500 = 500 * 1024 * 1024
        GB_1 = 1024 * 1024 * 1024
        GB_1_5 = 1.5 * 1024 * 1024 * 1024
        
        if size_in < MB_500:
            required_ratio = 0.95
        elif size_in < GB_1:
            required_ratio = 0.96
        elif size_in < GB_1_5:
            required_ratio = 0.97
        else:
            required_ratio = 0.99

        size_ratio = size_out / size_in
        ThreadSafeLogger.debug(f"Verifying size for {Path(dcv_file_path).name}. Input: {size_in}, Output: {size_out}, Ratio: {size_ratio:.4f}, Required: >={required_ratio}")
        
        if size_ratio < required_ratio:
            ThreadSafeLogger.error(f"Size mismatch for {Path(dcv_file_path).name}: Ratio {size_ratio:.4f} is less than required {required_ratio}.")
            safe_remove(str(output_mkv_path), "size mismatch")
            return None
        else:
            ThreadSafeLogger.info(f"Size check passed for {output_mkv_path.name} (Ratio: {size_ratio:.4f}, Required: {required_ratio})")
    
    # Duration verification using MediaInfo
    ThreadSafeLogger.debug(f"Comparing duration for {Path(dcv_file_path).name} and its MKV output.")
    dcv_duration = get_media_duration(dcv_file_path)
    mkv_duration = get_media_duration(str(output_mkv_path))

    if dcv_duration is None or mkv_duration is None:
        ThreadSafeLogger.error(f"Could not get MediaInfo duration for '{Path(dcv_file_path).name}' or its output. Verification failed.")
        safe_remove(str(output_mkv_path), "duration check failed")
        return None
    
    duration_diff = abs(dcv_duration - mkv_duration)
    if duration_diff > 30.0:
        ThreadSafeLogger.error(f"Duration mismatch for {Path(dcv_file_path).name}: DCV={dcv_duration:.2f}s, MKV={mkv_duration:.2f}s (Difference: {duration_diff:.2f}s > 6s)")
        safe_remove(str(output_mkv_path), "duration mismatch")
        return None
    else:
        ThreadSafeLogger.info(f"Duration check passed for {output_mkv_path.name} (Difference: {duration_diff:.2f}s)")
        
    ThreadSafeLogger.info(f"✅ Decryption and verification successful for {Path(dcv_file_path).name}")
    return str(output_mkv_path)

def decrypt_video_parts(task: IntegratedTask, temp_dir: str) -> bool:
    """Decrypt all downloaded DCV files to MKV format"""
    if not task.downloaded_files:
        ThreadSafeLogger.error(f"No DCV files to decrypt for ID {task.cid}")
        task.error_message = "No DCV files to decrypt"
        return False

    config = task.config
    decrypt_tool_path = task.decrypt_tool_path
    
    try:
        settings = config['Settings']
        max_retries = settings.getint('max_retries', fallback=3)
        decrypt_retry_delay = settings.getint('decrypt_retry_delay_seconds', fallback=100)
    except Exception as e:
        ThreadSafeLogger.error(f"Configuration error reading decryption settings: {e}. Using defaults.")
        max_retries = 3
        decrypt_retry_delay = 10

    decrypted_files = []
    all_parts_decrypted = True

    for dcv_file_path in task.downloaded_files:
        ThreadSafeLogger.info(f"Decrypting {Path(dcv_file_path).name} for ID {task.cid}...")
        
        decrypted_mkv_path = decrypt_dcv_file(
            dcv_file_path, temp_dir, decrypt_tool_path, max_retries, decrypt_retry_delay
        )
        
        if decrypted_mkv_path:
            decrypted_files.append(decrypted_mkv_path)
            ThreadSafeLogger.info(f"Successfully decrypted {Path(dcv_file_path).name}")
        else:
            ThreadSafeLogger.error(f"Failed to decrypt {Path(dcv_file_path).name}")
            all_parts_decrypted = False
            break

    if all_parts_decrypted and len(decrypted_files) == len(task.downloaded_files):
        ThreadSafeLogger.info(f"Successfully decrypted all {len(decrypted_files)} part(s) for ID {task.cid}")
        task.decrypted_files = decrypted_files
        return True
    else:
        ThreadSafeLogger.error(f"Decryption failed for ID {task.cid}. Expected {len(task.downloaded_files)}, got {len(decrypted_files)}")
        task.error_message = f"Decryption failed: expected {len(task.downloaded_files)}, got {len(decrypted_files)}"
        # Clean up any successfully decrypted files
        for mkv_file in decrypted_files:
            safe_remove(mkv_file, "partial decryption cleanup")
        return False

# ---------------- RCLONE UPLOAD FUNCTIONS ----------------

def upload_mkv_file(local_mkv_path: str, rclone_config: Dict) -> bool:
    """Upload MKV file to rclone remote"""
    if not Path(local_mkv_path).exists():
        ThreadSafeLogger.error(f"MKV file does not exist: {local_mkv_path}")
        return False

    mkv_filename = Path(local_mkv_path).name
    remote_mkv_path = f"{rclone_config['remote_name']}:{rclone_config['remote_mkv_path']}"
    rclone_executable = rclone_config['rclone_executable']
    
    ThreadSafeLogger.info(f"Uploading decrypted file: {mkv_filename}")
    
    try:
        # Ensure remote directory exists
        mkdir_cmd = [rclone_executable, 'mkdir', remote_mkv_path]
        subprocess.run(mkdir_cmd, capture_output=True, timeout=30)
        
        # Upload file
        cmd = [rclone_executable, 'copy', local_mkv_path, remote_mkv_path, '-v']
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)  # 30 minutes timeout
        
        if result.returncode == 0:
            ThreadSafeLogger.info(f"✅ Upload successful: {mkv_filename}")
            return True
        else:
            ThreadSafeLogger.error(f"❌ Upload failed: {mkv_filename}")
            ThreadSafeLogger.error(f"Error message: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        ThreadSafeLogger.error(f"❌ Upload timeout: {mkv_filename}")
        return False
    except Exception as e:
        ThreadSafeLogger.error(f"❌ Error occurred during upload {mkv_filename}: {e}")
        return False

def upload_video_parts(task: IntegratedTask) -> bool:
    """Upload all decrypted MKV files to rclone remote"""
    if not task.decrypted_files:
        ThreadSafeLogger.error(f"No MKV files to upload for ID {task.cid}")
        task.error_message = "No MKV files to upload"
        return False

    uploaded_files = []
    all_parts_uploaded = True

    for mkv_file_path in task.decrypted_files:
        ThreadSafeLogger.info(f"Uploading {Path(mkv_file_path).name} for ID {task.cid}...")
        
        if upload_mkv_file(mkv_file_path, task.rclone_config):
            uploaded_files.append(mkv_file_path)
            ThreadSafeLogger.info(f"Successfully uploaded {Path(mkv_file_path).name}")
        else:
            ThreadSafeLogger.error(f"Failed to upload {Path(mkv_file_path).name}")
            all_parts_uploaded = False
            break

    if all_parts_uploaded and len(uploaded_files) == len(task.decrypted_files):
        ThreadSafeLogger.info(f"Successfully uploaded all {len(uploaded_files)} part(s) for ID {task.cid}")
        task.uploaded_files = uploaded_files
        return True
    else:
        ThreadSafeLogger.error(f"Upload failed for ID {task.cid}. Expected {len(task.decrypted_files)}, got {len(uploaded_files)}")
        task.error_message = f"Upload failed: expected {len(task.decrypted_files)}, got {len(uploaded_files)}"
        return False

# ---------------- INTEGRATED PROCESSING FUNCTIONS ----------------

def check_rclone_available(rclone_executable: str) -> bool:
    """Check if rclone is available"""
    try:
        result = subprocess.run([rclone_executable, 'version'], 
                              capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            version_info = result.stdout.split('\n')[0] if result.stdout else "Unknown"
            ThreadSafeLogger.info(f"✅ Rclone available: {version_info}")
            return True
        else:
            ThreadSafeLogger.error("❌ Rclone command execution failed")
            return False
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError):
        ThreadSafeLogger.error("❌ Rclone not found or unavailable")
        ThreadSafeLogger.error("   Please ensure rclone is installed and added to system PATH")
        return False

def process_integrated_task(task: IntegratedTask) -> bool:
    """Process a complete integrated task: download -> decrypt -> upload (or save locally)"""
    cid = task.cid
    task.start_time = datetime.now()
    
    # Determine if we're uploading or saving locally
    rclone_enabled = task.rclone_config is not None
    phase_count = 3 if rclone_enabled else 2
    
    ThreadSafeLogger.info(f"=== Starting Integrated Processing for ID: {cid} ===")
    
    try:
        # Use output_dir for decryption if in local mode, otherwise use temp_dir
        if rclone_enabled:
            temp_context = TemporaryDirectory(prefix=f"integrated_{cid}_")
            temp_dir = temp_context.__enter__()
            decrypt_output_dir = temp_dir
        else:
            temp_context = None
            temp_dir = None
            decrypt_output_dir = task.output_dir
            Path(decrypt_output_dir).mkdir(parents=True, exist_ok=True)
        
        try:
            # Use a temp dir for downloads even in local mode
            if temp_dir is None:
                download_temp = TemporaryDirectory(prefix=f"download_{cid}_")
                download_dir = download_temp.__enter__()
            else:
                download_temp = None
                download_dir = temp_dir
            
            try:
                ThreadSafeLogger.debug(f"Using download directory: {download_dir}")
                ThreadSafeLogger.debug(f"Using decrypt output directory: {decrypt_output_dir}")
                
                # Check disk space
                if not check_disk_space(download_dir, 10.0):
                    ThreadSafeLogger.error(f"Insufficient disk space in temporary directory for ID {cid}")
                    task.error_message = "Insufficient disk space"
                    update_processing_stats('fail_download')
                    return False
                
                # Phase 1: Download
                ThreadSafeLogger.info(f"Phase 1/{phase_count}: Downloading DCV files for ID {cid}")
                task.status = TaskStatus.DOWNLOADING
                update_processing_stats('start_download')
                
                if not download_video_parts(task, download_dir):
                    ThreadSafeLogger.error(f"Download phase failed for ID {cid}")
                    update_processing_stats('fail_download')
                    task.status = TaskStatus.FAILED
                    return False
                
                # Handle special case where video has 0 parts
                if task.parts_count == 0:
                    ThreadSafeLogger.info(f"ID {cid} has 0 parts. Marking as completed.")
                    update_processing_stats('complete')
                    task.status = TaskStatus.COMPLETED
                    return True
                
                # Phase 2: Decrypt
                ThreadSafeLogger.info(f"Phase 2/{phase_count}: Decrypting DCV files for ID {cid}")
                task.status = TaskStatus.DECRYPTING
                update_processing_stats('finish_download')
                
                if not decrypt_video_parts(task, decrypt_output_dir):
                    ThreadSafeLogger.error(f"Decryption phase failed for ID {cid}")
                    update_processing_stats('fail_decrypt')
                    task.status = TaskStatus.FAILED
                    return False
                
                # Phase 3: Upload (only if rclone is configured)
                if rclone_enabled:
                    ThreadSafeLogger.info(f"Phase 3/{phase_count}: Uploading MKV files for ID {cid}")
                    task.status = TaskStatus.UPLOADING
                    update_processing_stats('finish_decrypt')
                    
                    if not upload_video_parts(task):
                        ThreadSafeLogger.error(f"Upload phase failed for ID {cid}")
                        update_processing_stats('fail_upload')
                        task.status = TaskStatus.FAILED
                        return False
                else:
                    ThreadSafeLogger.info(f"Skipping upload - files saved to: {decrypt_output_dir}")
                    update_processing_stats('finish_decrypt')
                
                # Success
                ThreadSafeLogger.info(f"✅ All phases completed successfully for ID {cid}")
                update_processing_stats('complete')
                task.status = TaskStatus.COMPLETED
                task.end_time = datetime.now()
                
                # Log processing time
                duration = task.end_time - task.start_time
                ThreadSafeLogger.info(f"Processing time for ID {cid}: {duration}")
                
                return True
            
            finally:
                if download_temp is not None:
                    download_temp.__exit__(None, None, None)
        
        finally:
            if temp_context is not None:
                temp_context.__exit__(None, None, None)
    
    except Exception as e:
        ThreadSafeLogger.error(f"Unexpected error processing ID {cid}: {e}")
        task.error_message = f"Unexpected error: {e}"
        task.status = TaskStatus.FAILED
        # Determine which phase failed for stats update
        if task.status == TaskStatus.DOWNLOADING:
            update_processing_stats('fail_download')
        elif task.status == TaskStatus.DECRYPTING:
            update_processing_stats('fail_decrypt')
        elif task.status == TaskStatus.UPLOADING:
            update_processing_stats('fail_upload')
        return False
    
    finally:
        task.end_time = datetime.now() if task.end_time is None else task.end_time
        ThreadSafeLogger.info(f"=== Finished Processing ID: {cid} ===")

# ---------------- MAIN EXECUTION ----------------

def main():
    """Main function to initialize and start integrated processing"""
    start_time = datetime.now()
    print(f"DMM Integrated Processor started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Check dependencies
    if not check_dependencies():
        print("❌ Dependency check failed. Please install required components.")
        return 1
    
    # Load configuration
    config = load_configuration(CONFIG_FILE)
    paths = config['Paths']
    network = config['Network']
    settings = config['Settings']
    
    # Check if Rclone is configured (optional)
    rclone_enabled = config.has_section('Rclone')
    rclone_config = dict(config['Rclone']) if rclone_enabled else None
    
    # Setup logging
    setup_logging(paths['log_file_all'], paths['log_file_errors'])
    ThreadSafeLogger.info("Logging initialized for integrated processor.")
    ThreadSafeLogger.info(f"Using configuration from: {CONFIG_FILE}")
    
    if rclone_enabled:
        ThreadSafeLogger.info("Rclone is configured - files will be uploaded after decryption")
    else:
        ThreadSafeLogger.info("Rclone not configured - files will be saved locally after decryption")
    
    # Setup directories
    try:
        setup_directories(config)
    except Exception as e:
        print(f"❌ Failed to create required directories: {e}")
        return 1
    
    # Locate decryption tool
    decrypt_executable = paths['decrypt_executable']
    script_dir = Path(__file__).parent.absolute()
    
    local_decrypt_path = script_dir / decrypt_executable
    if local_decrypt_path.exists():
        decrypt_tool_path = str(local_decrypt_path)
        ThreadSafeLogger.info(f"Using decryption tool from script directory: {decrypt_tool_path}")
    else:
        decrypt_tool_path = shutil.which(decrypt_executable)
        if decrypt_tool_path:
            ThreadSafeLogger.info(f"Using decryption tool found in PATH: {decrypt_tool_path}")
        elif Path(decrypt_executable).is_absolute() and Path(decrypt_executable).is_file():
            decrypt_tool_path = decrypt_executable
            ThreadSafeLogger.info(f"Using decryption tool specified in config: {decrypt_tool_path}")
        else:
            ThreadSafeLogger.critical(f"Decryption executable '{decrypt_executable}' not found.")
            return 1
    
    # Check rclone availability (only if configured)
    if rclone_enabled:
        if not check_rclone_available(rclone_config['rclone_executable']):
            print("❌ Rclone not available. Please install rclone and ensure it's in PATH.")
            return 1
    else:
        ThreadSafeLogger.info("Skipping rclone check - not configured")
    
    # Get output directory for local mode
    output_dir = None
    if not rclone_enabled:
        output_dir = expand_user_path(paths.get('decrypt_output_dir', './decrypted'))
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        ThreadSafeLogger.info(f"Local output directory: {output_dir}")
    
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
        print("⚠️  No IDs found to process.")
        return 0
    
    ThreadSafeLogger.info(f"Found {len(ids_to_process)} IDs to process.")
    print(f"Found {len(ids_to_process)} IDs to process.")
    
    # Initialize processing statistics
    processing_stats.total = len(ids_to_process)
    print_processing_progress()
    
    # Get thread count from configuration
    max_workers = min(settings.getint('decrypt_threads', fallback=2), 
                     settings.getint('download_threads', fallback=2))
    max_workers = max(1, max_workers)  # Ensure at least 1 thread
    
    ThreadSafeLogger.info(f"Starting integrated processing with {max_workers} concurrent threads.")
    print(f"Starting processing with {max_workers} concurrent threads.")
    
    # Create integrated tasks
    integrated_tasks = []
    for cid in ids_to_process:
        task = IntegratedTask(cid, config, headers_main, headers_download, 
                             decrypt_tool_path, rclone_config, proxies, output_dir)
        integrated_tasks.append(task)
    
    # Execute integrated processing with thread pool
    successful_tasks = []
    failed_tasks = []
    
    try:
        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="IntegratedProcessor") as executor:
            # Submit all processing tasks
            future_to_task = {executor.submit(process_integrated_task, task): task for task in integrated_tasks}
            
            # Process completed tasks
            for future in as_completed(future_to_task):
                task = future_to_task[future]
                try:
                    success = future.result()
                    if success:
                        successful_tasks.append(task)
                        ThreadSafeLogger.info(f"✅ Processing completed for ID: {task.cid}")
                    else:
                        failed_tasks.append(task)
                        ThreadSafeLogger.error(f"❌ Processing failed for ID: {task.cid} - {task.error_message}")
                        
                        # Record failed ID
                        write_failed_id(task.cid, paths['failed_ids_file'])
                        
                except Exception as exc:
                    failed_tasks.append(task)
                    task.error_message = f"Exception: {exc}"
                    ThreadSafeLogger.error(f"❌ Processing generated exception for ID {task.cid}: {exc}")
                    write_failed_id(task.cid, paths['failed_ids_file'])
                
                # Remove processed ID from the list
                remove_id_from_file(task.cid, paths['ids_file'])
                
                # Print periodic progress updates
                if (len(successful_tasks) + len(failed_tasks)) % 3 == 0:
                    print_processing_progress()
    
    except KeyboardInterrupt:
        ThreadSafeLogger.warning("Integrated processing interrupted by user.")
        print("\n⚠️  Processing was interrupted!")
    except Exception as e:
        ThreadSafeLogger.error(f"Unexpected error in main processing loop: {e}")
        print(f"❌ Unexpected error occurred: {e}")
    
    # Final summary
    end_time = datetime.now()
    duration = end_time - start_time
    
    print("\n" + "="*70)
    print("INTEGRATED PROCESSING SUMMARY")
    print("="*70)
    print(f"Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"End Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Duration: {duration}")
    print(f"Total IDs: {len(ids_to_process)}")
    print(f"Successful: {len(successful_tasks)}")
    print(f"Failed: {len(failed_tasks)}")
    print("="*70)
    
    if failed_tasks:
        print("\nFailed Processing:")
        for task in failed_tasks:
            print(f"  - {task.cid}: {task.error_message}")
    
    if successful_tasks:
        print(f"\nSuccessful Processing Summary:")
        total_parts_processed = sum(task.parts_count for task in successful_tasks)
        print(f"  - Total video parts processed: {total_parts_processed}")
        
        # Calculate average processing time
        completed_durations = [(task.end_time - task.start_time) for task in successful_tasks 
                              if task.start_time and task.end_time]
        if completed_durations:
            avg_duration = sum(completed_durations, timedelta(0)) / len(completed_durations)
            print(f"  - Average processing time per ID: {avg_duration}")
    
    ThreadSafeLogger.info(f"Integrated processing finished. Success: {len(successful_tasks)}, Failed: {len(failed_tasks)}")
    
    return 0 if len(failed_tasks) == 0 else 1

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)
