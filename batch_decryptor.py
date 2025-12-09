"""
DMM Batch Decryptor - Multi-threaded decryptor for DMM .dcv files
Decrypts and verifies .dcv files to .mkv format with parallel processing support.
"""

import time
import subprocess
import shutil
from pathlib import Path
from datetime import datetime
from typing import List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import glob

# Local imports
from shared_utils import (
    check_dependencies, load_configuration, setup_logging, 
    expand_user_path, ThreadSafeLogger, write_failed_id,
    safe_getsize, safe_remove, check_disk_space,
    get_media_duration, has_conformance_errors,
    setup_directories, CONFIG_FILE, DECRYPT_CONN_ERROR_MSG
)

# Progress tracking
decrypt_progress_lock = threading.Lock()
decrypt_stats = {
    'completed': 0,
    'failed': 0,
    'in_progress': 0,
    'total': 0
}

class DecryptTask:
    """Represents a single decryption task"""
    def __init__(self, dcv_file_path: str, config, decrypt_tool_path: str):
        self.dcv_file_path = dcv_file_path
        self.dcv_file_name = Path(dcv_file_path).name
        self.cid = self.extract_cid_from_filename()
        self.config = config
        self.decrypt_tool_path = decrypt_tool_path
        self.output_mkv_path = None
        
    def extract_cid_from_filename(self) -> str:
        """Extract CID from DCV filename"""
        # Assuming filename format like: cid123_Part1.dcv or cid123.dcv
        base_name = Path(self.dcv_file_path).stem
        # Remove part suffix if present
        if '_Part' in base_name:
            return base_name.split('_Part')[0]
        return base_name
        
    def __str__(self):
        return f"DecryptTask({self.dcv_file_name})"

def update_decrypt_stats(status: str):
    """Update global decryption statistics (thread-safe)"""
    with decrypt_progress_lock:
        if status == 'start':
            decrypt_stats['in_progress'] += 1
        elif status == 'complete':
            decrypt_stats['completed'] += 1
            decrypt_stats['in_progress'] -= 1
        elif status == 'failed':
            decrypt_stats['failed'] += 1
            decrypt_stats['in_progress'] -= 1

def print_decrypt_progress():
    """Print current decryption progress"""
    with decrypt_progress_lock:
        total = decrypt_stats['total']
        completed = decrypt_stats['completed']
        failed = decrypt_stats['failed']
        in_progress = decrypt_stats['in_progress']
        
        print(f"\n=== Decryption Progress ===")
        print(f"Total Files: {total}")
        print(f"Completed: {completed}")
        print(f"Failed: {failed}")
        print(f"In Progress: {in_progress}")
        print(f"Remaining: {total - completed - failed}")
        print("===========================\n")

# ---------------- CORE DECRYPTION FUNCTIONS ----------------

def decrypt_single_file(task: DecryptTask) -> bool:
    """Decrypt a single .dcv file with verification and retry logic."""
    dcv_file_path = task.dcv_file_path
    config = task.config
    decrypt_tool_path = task.decrypt_tool_path
    
    if not Path(dcv_file_path).exists():
        ThreadSafeLogger.error(f"DCV file not found: {dcv_file_path}")
        return False

    try:
        paths = config['Paths']
        settings = config['Settings']
        decrypt_output_dir = expand_user_path(paths['decrypt_output_dir'])
        max_retries = settings.getint('max_retries', fallback=3)
        decrypt_retry_delay = settings.getint('decrypt_retry_delay_seconds', fallback=100)
    except Exception as e:
        ThreadSafeLogger.error(f"Configuration error reading decryption settings: {e}")
        max_retries = 3
        decrypt_retry_delay = 10

    # Prepare output path
    base_name = Path(dcv_file_path).stem
    output_mkv_path = Path(decrypt_output_dir) / f"{base_name}.mkv"
    task.output_mkv_path = str(output_mkv_path)

    # Remove existing output file if present
    if output_mkv_path.exists():
        mkv_size = safe_getsize(str(output_mkv_path))
        if mkv_size is not None and mkv_size > 1024:
            ThreadSafeLogger.warning(f"Decrypted file already exists: {output_mkv_path}. Re-decrypting to ensure validity.")
        safe_remove(str(output_mkv_path), "pre-decryption cleanup")

    # Pre-decryption conformance check
    ThreadSafeLogger.info(f"Performing conformance check on {Path(dcv_file_path).name}...")
    if has_conformance_errors(dcv_file_path):
        ThreadSafeLogger.error(f"Conformance check failed for {Path(dcv_file_path).name}. File is likely corrupt.")
        return False

    # Decryption command
    cmd = [decrypt_tool_path, "decrypt", "-i", dcv_file_path, "-o", str(output_mkv_path), "-t", "dmm"]
    ThreadSafeLogger.info(f"Decrypting {Path(dcv_file_path).name}...")
    ThreadSafeLogger.debug(f"Running command: {' '.join(cmd)}")

    # Decryption retry loop
    for attempt in range(max_retries):
        ThreadSafeLogger.info(f"Waiting 60 seconds before decryption attempt {attempt + 1} for {Path(dcv_file_path).name}")
        time.sleep(60)
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
                        ThreadSafeLogger.error(f"Stderr:\n{result.stderr}")
                        return False
                else:
                    ThreadSafeLogger.error(error_msg)
                    ThreadSafeLogger.error(f"Stderr:\n{result.stderr}")
                    return False

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
                    return False

        except subprocess.TimeoutExpired:
            ThreadSafeLogger.error(f"Decryption timeout for {Path(dcv_file_path).name} on attempt {attempt + 1}")
            return False
        except FileNotFoundError:
            ThreadSafeLogger.critical(f"Decryption executable '{decrypt_tool_path}' not found.")
            return False
        except Exception as e:
            ThreadSafeLogger.error(f"Error running decryption command for {Path(dcv_file_path).name} on attempt {attempt + 1}: {e}")
            return False

        if should_retry:
            time.sleep(decrypt_retry_delay)

    # Verify decryption output
    if not output_mkv_path.exists():
        ThreadSafeLogger.error(f"Decryption reported success but output file not found: {output_mkv_path}")
        return False

    # Size verification with variable tolerance
    size_in = safe_getsize(dcv_file_path)
    size_out = safe_getsize(str(output_mkv_path))

    if size_in is None or size_out is None:
        ThreadSafeLogger.error(f"Could not get file sizes to verify {Path(dcv_file_path).name}. Deleting output.")
        safe_remove(str(output_mkv_path), "size verification failed")
        return False

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
            return False
        else:
            ThreadSafeLogger.info(f"Size check passed for {output_mkv_path.name} (Ratio: {size_ratio:.4f}, Required: {required_ratio})")
    
    # Duration verification using MediaInfo
    ThreadSafeLogger.debug(f"Comparing duration for {Path(dcv_file_path).name} and its MKV output.")
    dcv_duration = get_media_duration(dcv_file_path)
    mkv_duration = get_media_duration(str(output_mkv_path))

    if dcv_duration is None or mkv_duration is None:
        ThreadSafeLogger.error(f"Could not get MediaInfo duration for '{Path(dcv_file_path).name}' or its output. Verification failed.")
        safe_remove(str(output_mkv_path), "duration check failed")
        return False
    
    duration_diff = abs(dcv_duration - mkv_duration)
    if duration_diff > 30.0:
        ThreadSafeLogger.error(f"Duration mismatch for {Path(dcv_file_path).name}: DCV={dcv_duration:.2f}s, MKV={mkv_duration:.2f}s (Difference: {duration_diff:.2f}s > 6s)")
        safe_remove(str(output_mkv_path), "duration mismatch")
        return False
    else:
        ThreadSafeLogger.info(f"Duration check passed for {output_mkv_path.name} (Difference: {duration_diff:.2f}s)")
        
    ThreadSafeLogger.info(f"✅ Decryption and verification successful for {Path(dcv_file_path).name}")
    return True

def process_single_decryption(task: DecryptTask) -> bool:
    """Process a single decryption task (thread worker function)."""
    dcv_file_path = task.dcv_file_path
    file_name = Path(dcv_file_path).name
    
    ThreadSafeLogger.info(f"=== Starting Decryption for: {file_name} ===")
    update_decrypt_stats('start')
    
    success = False
    
    try:
        # Check disk space before starting decryption
        config = task.config
        paths = config['Paths']
        decrypt_output_dir = expand_user_path(paths['decrypt_output_dir'])
        
        if not check_disk_space(decrypt_output_dir, 10.0):  # Need less space for decryption output
            ThreadSafeLogger.error(f"Insufficient disk space in decryption output directory for {file_name}")
            raise Exception("Insufficient disk space")
        
        # Perform the decryption
        success = decrypt_single_file(task)
        
        if success:
            # Delete source .dcv file after successful decryption
            ThreadSafeLogger.info(f"Decryption successful, deleting source file: {file_name}")
            safe_remove(dcv_file_path, "successful decryption cleanup")
            
    except Exception as e:
        ThreadSafeLogger.error(f"Unexpected error processing {file_name}: {e}")
    
    # Handle results
    if success:
        update_decrypt_stats('complete')
        ThreadSafeLogger.info(f"=== Decryption Successful for: {file_name} ===")
    else:
        update_decrypt_stats('failed')
        # Write failed file info
        failed_ids_file = task.config['Paths']['failed_ids_file']
        write_failed_id(f"DECRYPT_{task.cid}_{file_name}", failed_ids_file)
        ThreadSafeLogger.error(f"=== Decryption Failed for: {file_name} ===")
    
    return success

def find_dcv_files(search_directories: List[str]) -> List[str]:
    """Find all .dcv files in the specified directories."""
    dcv_files = []
    
    for directory in search_directories:
        dir_path = Path(directory)
        if not dir_path.exists():
            ThreadSafeLogger.warning(f"Directory does not exist: {directory}")
            continue
            
        ThreadSafeLogger.info(f"Searching for .dcv files in: {directory}")
        
        # Search for .dcv files recursively
        pattern = str(dir_path / "**" / "*.dcv")
        found_files = glob.glob(pattern, recursive=True)
        
        if found_files:
            ThreadSafeLogger.info(f"Found {len(found_files)} .dcv files in {directory}")
            dcv_files.extend(found_files)
        else:
            ThreadSafeLogger.warning(f"No .dcv files found in {directory}")
    
    # Remove duplicates and sort
    dcv_files = sorted(list(set(dcv_files)))
    ThreadSafeLogger.info(f"Total unique .dcv files found: {len(dcv_files)}")
    
    return dcv_files

# ---------------- MAIN EXECUTION ----------------

def main():
    """Main function to initialize and start multi-threaded decryption."""
    start_time = datetime.now()
    print(f"DMM Batch Decryptor started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Check dependencies
    if not check_dependencies():
        print("❌ Dependency check failed. Please install required components.")
        return 1
    
    # Load configuration
    config = load_configuration(CONFIG_FILE)
    paths = config['Paths']
    settings = config['Settings']
    
    # Setup logging
    setup_logging(paths['log_file_all'], paths['log_file_errors'])
    ThreadSafeLogger.info("Logging initialized for batch decryptor.")
    ThreadSafeLogger.info(f"Using configuration from: {CONFIG_FILE}")
    
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
    
    # Find .dcv files to decrypt
    search_directories = [
        expand_user_path(paths['download_dir']),
        expand_user_path(paths.get('dcv_files_dir', './dcv_files'))
    ]
    
    dcv_files = find_dcv_files(search_directories)
    if not dcv_files:
        ThreadSafeLogger.warning("No .dcv files found for decryption.")
        print("⚠️  No .dcv files found for decryption.")
        return 0
    
    ThreadSafeLogger.info(f"Found {len(dcv_files)} .dcv files to decrypt.")
    
    # Initialize decryption statistics
    decrypt_stats['total'] = len(dcv_files)
    print_decrypt_progress()
    
    # Get thread count from configuration
    decrypt_threads = settings.getint('decrypt_threads', fallback=2)
    ThreadSafeLogger.info(f"Starting decryption with {decrypt_threads} concurrent threads.")
    
    # Create decryption tasks
    decrypt_tasks = []
    for dcv_file in dcv_files:
        task = DecryptTask(dcv_file, config, decrypt_tool_path)
        decrypt_tasks.append(task)
    
    # Execute decryptions with thread pool
    successful_decryptions = []
    failed_decryptions = []
    
    try:
        with ThreadPoolExecutor(max_workers=decrypt_threads, thread_name_prefix="Decryptor") as executor:
            # Submit all decryption tasks
            future_to_task = {executor.submit(process_single_decryption, task): task for task in decrypt_tasks}
            
            # Process completed decryptions
            for future in as_completed(future_to_task):
                task = future_to_task[future]
                try:
                    success = future.result()
                    if success:
                        successful_decryptions.append(task.dcv_file_name)
                        ThreadSafeLogger.info(f"✅ Decryption completed for: {task.dcv_file_name}")
                    else:
                        failed_decryptions.append(task.dcv_file_name)
                        ThreadSafeLogger.error(f"❌ Decryption failed for: {task.dcv_file_name}")
                except Exception as exc:
                    failed_decryptions.append(task.dcv_file_name)
                    ThreadSafeLogger.error(f"❌ Decryption generated exception for {task.dcv_file_name}: {exc}")
                
                # Print periodic progress updates
                if (len(successful_decryptions) + len(failed_decryptions)) % 3 == 0:
                    print_decrypt_progress()
    
    except KeyboardInterrupt:
        ThreadSafeLogger.warning("Decryption process interrupted by user.")
        print("\n⚠️  Decryption process was interrupted!")
    except Exception as e:
        ThreadSafeLogger.error(f"Unexpected error in main decryption loop: {e}")
        print(f"❌ Unexpected error occurred: {e}")
    
    # Final summary
    end_time = datetime.now()
    duration = end_time - start_time
    
    print("\n" + "="*60)
    print("DECRYPTION SUMMARY")
    print("="*60)
    print(f"Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"End Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Duration: {duration}")
    print(f"Total Files: {len(dcv_files)}")
    print(f"Successful: {len(successful_decryptions)}")
    print(f"Failed: {len(failed_decryptions)}")
    print("="*60)
    
    if failed_decryptions:
        print("\nFailed Decryptions:")
        for file_name in failed_decryptions:
            print(f"  - {file_name}")
    
    ThreadSafeLogger.info(f"Batch decryption finished. Success: {len(successful_decryptions)}, Failed: {len(failed_decryptions)}")
    
    return 0 if len(failed_decryptions) == 0 else 1

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)
