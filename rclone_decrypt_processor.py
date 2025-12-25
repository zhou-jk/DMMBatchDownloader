#!/usr/bin/env python3
"""
Rclone DCV Decrypt Processor - Download DCV files from remote, decrypt locally and upload back
Downloads .dcv files from rclone remote /root/jav-it directory, decrypts them to .mkv locally, then uploads back to remote
"""

import os
import sys
import time
import json
import shutil
import subprocess
import logging
import configparser
import platform
import argparse
from pathlib import Path
from datetime import datetime
from typing import List, Optional, Tuple
from tempfile import TemporaryDirectory

# Local imports (reuse existing utility functions)
from shared_utils import (
    check_dependencies, setup_logging, expand_user_path,
    has_conformance_errors, get_media_duration,
    safe_getsize, safe_remove, check_disk_space,
    create_default_config, CONFIG_FILE, DECRYPT_CONN_ERROR_MSG
)

class RcloneDecryptProcessor:
    """Rclone DCV Decryption Processor Class"""
    
    def __init__(self, config: configparser.ConfigParser, remote_name: Optional[str] = None):
        self.config = config
        
        # Check if Rclone section exists
        if not config.has_section('Rclone'):
            raise ValueError("[Rclone] section not found in config. Rclone features are not configured.")
        
        # Get rclone configuration from config file
        rclone_config = config['Rclone']
        
        self.remote_name = remote_name or rclone_config.get('remote_name', 'remote')
        self.remote_dcv_path = rclone_config.get('remote_dcv_path', '')
        self.remote_mkv_path = rclone_config.get('remote_mkv_path', '')
        self.rclone_executable = rclone_config.get('rclone_executable', 'rclone')
        
        self.decrypt_tool_path = self._locate_decrypt_tool()
        self.temp_dir = None
        
    def _locate_decrypt_tool(self) -> str:
        """Locate decryption tool path"""
        decrypt_executable = self.config['Paths']['decrypt_executable']
        script_dir = Path(__file__).parent.absolute()
        
        # First check script directory
        local_decrypt_path = script_dir / decrypt_executable
        if local_decrypt_path.exists():
            logging.info(f"Using decryption tool from script directory: {local_decrypt_path}")
            return str(local_decrypt_path)
        
        # Check system PATH
        decrypt_tool_path = shutil.which(decrypt_executable)
        if decrypt_tool_path:
            logging.info(f"Using decryption tool found in PATH: {decrypt_tool_path}")
            return decrypt_tool_path
        
        # Check if it's an absolute path
        if Path(decrypt_executable).is_absolute() and Path(decrypt_executable).is_file():
            logging.info(f"Using decryption tool specified in config: {decrypt_executable}")
            return decrypt_executable
        
        raise FileNotFoundError(f"Decryption tool '{decrypt_executable}' not found")
    
    def check_rclone_available(self) -> bool:
        """Check if rclone is available"""
        try:
            result = subprocess.run([self.rclone_executable, 'version'], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                version_info = result.stdout.split('\n')[0] if result.stdout else "Unknown"
                logging.info(f"✅ Rclone available: {version_info}")
                return True
            else:
                logging.error("❌ Rclone command execution failed")
                return False
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError):
            logging.error("❌ Rclone not found or unavailable")
            logging.error("   Please ensure rclone is installed and added to system PATH")
            return False
    
    def list_remote_dcv_files(self) -> List[str]:
        """List remote DCV files"""
        logging.info(f"Scanning remote path: {self.remote_name}:{self.remote_dcv_path}")
        
        try:
            cmd = [
                self.rclone_executable, 'lsf', '--include', '*.dcv',
                f"{self.remote_name}:{self.remote_dcv_path}"
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            
            if result.returncode == 0:
                files = [f.strip() for f in result.stdout.split('\n') if f.strip().endswith('.dcv')]
                logging.info(f"Found {len(files)} DCV files")
                return files
            else:
                logging.error(f"Failed to list remote files: {result.stderr}")
                return []
                
        except subprocess.TimeoutExpired:
            logging.error("Timeout listing remote files")
            return []
        except Exception as e:
            logging.error(f"Error occurred while listing remote files: {e}")
            return []
    
    def download_dcv_file(self, filename: str, local_dir: str) -> Optional[str]:
        """Download single DCV file from remote"""
        remote_file_path = f"{self.remote_name}:{self.remote_dcv_path}/{filename}"
        local_file_path = Path(local_dir) / filename
        
        logging.info(f"Downloading: {filename}")
        
        try:
            cmd = [self.rclone_executable, 'copy', remote_file_path, str(local_dir), '-v']
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)  # 10 minutes timeout
            
            if result.returncode == 0 and local_file_path.exists():
                file_size = safe_getsize(str(local_file_path))
                if file_size and file_size > 0:
                    logging.info(f"✅ Download successful: {filename} ({file_size} bytes)")
                    return str(local_file_path)
                else:
                    logging.error(f"❌ Downloaded file is empty: {filename}")
                    safe_remove(str(local_file_path), "empty file cleanup")
                    return None
            else:
                logging.error(f"❌ Download failed: {filename}")
                logging.error(f"Error message: {result.stderr}")
                return None
                
        except subprocess.TimeoutExpired:
            logging.error(f"❌ Download timeout: {filename}")
            return None
        except Exception as e:
            logging.error(f"❌ Error occurred during download {filename}: {e}")
            return None
    
    def upload_mkv_file(self, local_mkv_path: str, original_filename: str) -> bool:
        """Upload MKV file to remote"""
        # Generate MKV filename (replace extension)
        mkv_filename = Path(original_filename).stem + ".mkv"
        remote_mkv_path = f"{self.remote_name}:{self.remote_mkv_path}"
        
        logging.info(f"Uploading decrypted file: {mkv_filename}")
        
        try:
            # Ensure remote directory exists
            mkdir_cmd = [self.rclone_executable, 'mkdir', remote_mkv_path]
            subprocess.run(mkdir_cmd, capture_output=True, timeout=30)
            
            # Upload file
            cmd = [self.rclone_executable, 'copy', local_mkv_path, remote_mkv_path, '-v']
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)  # 30 minutes timeout
            
            if result.returncode == 0:
                logging.info(f"✅ Upload successful: {mkv_filename}")
                return True
            else:
                logging.error(f"❌ Upload failed: {mkv_filename}")
                logging.error(f"Error message: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            logging.error(f"❌ Upload timeout: {mkv_filename}")
            return False
        except Exception as e:
            logging.error(f"❌ Error occurred during upload {mkv_filename}: {e}")
            return False
    
    def decrypt_dcv_file(self, dcv_file_path: str, output_dir: str) -> Optional[str]:
        """Decrypt single DCV file"""
        if not Path(dcv_file_path).exists():
            logging.error(f"DCV file does not exist: {dcv_file_path}")
            return None
        
        # Get configuration parameters
        try:
            settings = self.config['Settings']
            max_retries = settings.getint('max_retries', fallback=3)
            decrypt_retry_delay = settings.getint('decrypt_retry_delay_seconds', fallback=100)
        except Exception as e:
            logging.error(f"Error reading decryption configuration: {e}, using default values")
            max_retries = 3
            decrypt_retry_delay = 10
        
        # Prepare output path
        base_name = Path(dcv_file_path).stem
        output_mkv_path = Path(output_dir) / f"{base_name}.mkv"
        
        # Clean up existing output file
        if output_mkv_path.exists():
            safe_remove(str(output_mkv_path), "pre-decryption cleanup")
        
        # Pre-decryption conformance check
        logging.info(f"Performing conformance check on {Path(dcv_file_path).name}...")
        if has_conformance_errors(dcv_file_path):
            logging.error(f"Conformance check failed: {Path(dcv_file_path).name}, file may be corrupted")
            return None
        
        # Decryption command
        cmd = [self.decrypt_tool_path, "decrypt", "-i", dcv_file_path, "-o", str(output_mkv_path), "-t", "dmm"]
        logging.info(f"Decrypting: {Path(dcv_file_path).name}")
        logging.debug(f"Running command: {' '.join(cmd)}")
        
        # Decryption retry loop
        for attempt in range(max_retries):
            should_retry = False
            try:
                result = subprocess.run(cmd, capture_output=True, text=True, check=False, 
                                      encoding='utf-8', errors='replace', timeout=3600)  # 1 hour timeout
                
                logging.debug(f"Decryption attempt {attempt + 1}/{max_retries} stdout:\n{result.stdout}")
                if result.stderr:
                    logging.debug(f"Decryption attempt {attempt + 1}/{max_retries} stderr:\n{result.stderr}")
                
                if result.returncode != 0:
                    stderr_lower = result.stderr.lower() if result.stderr else ""
                    error_msg = f"Decryption command failed (attempt {attempt + 1}/{max_retries}, return code: {result.returncode})"
                    
                    if DECRYPT_CONN_ERROR_MSG in stderr_lower:
                        error_msg += f" jav-it reported '{DECRYPT_CONN_ERROR_MSG}'"
                        if attempt < max_retries - 1:
                            logging.warning(error_msg + f" Will retry after {decrypt_retry_delay} seconds")
                            should_retry = True
                        else:
                            logging.error(error_msg + " Max retries reached")
                            return None
                    else:
                        logging.error(error_msg)
                        logging.error(f"Error message:\n{result.stderr}")
                        return None
                    
                    if not should_retry:
                        break
                else:
                    stdout_lower = result.stdout.lower() if result.stdout else ""
                    stderr_lower = result.stderr.lower() if result.stderr else ""
                    if "[ ok ] decryption complete!" in stderr_lower or \
                       "[ ok ] decryption complete!" in stdout_lower:
                        logging.info(f"Decryption successful, attempt {attempt + 1}")
                        break
                    else:
                        logging.warning(f"Decryption command returned 0 but success message not found (attempt {attempt + 1})")
                        logging.warning(f"Error message:\n{result.stderr}")
                        return None
            
            except subprocess.TimeoutExpired:
                logging.error(f"Decryption timeout, attempt {attempt + 1}")
                return None
            except FileNotFoundError:
                logging.critical(f"Decryption tool not found: {self.decrypt_tool_path}")
                return None
            except Exception as e:
                logging.error(f"Error occurred during decryption, attempt {attempt + 1}: {e}")
                return None
            
            if should_retry:
                time.sleep(decrypt_retry_delay)
        
        # Verify decryption output
        if not output_mkv_path.exists():
            logging.error(f"Decryption reported success but output file does not exist: {output_mkv_path}")
            return None
        
        # File size verification
        if not self._verify_file_size(dcv_file_path, str(output_mkv_path)):
            return None
        
        # Duration verification
        if not self._verify_duration(dcv_file_path, str(output_mkv_path)):
            return None
        
        logging.info(f"✅ Decryption and verification successful: {Path(dcv_file_path).name}")
        return str(output_mkv_path)
    
    def _verify_file_size(self, dcv_path: str, mkv_path: str) -> bool:
        """Verify file size"""
        size_in = safe_getsize(dcv_path)
        size_out = safe_getsize(mkv_path)
        
        if size_in is None or size_out is None:
            logging.error(f"Cannot get file sizes for verification")
            safe_remove(mkv_path, "size verification failed")
            return False
        
        if size_in > 0:
            # Set different tolerance based on file size
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
            logging.debug(f"Size verification: input={size_in}, output={size_out}, ratio={size_ratio:.4f}, required>={required_ratio}")
            
            if size_ratio < required_ratio:
                logging.error(f"Size mismatch: ratio {size_ratio:.4f} less than required {required_ratio}")
                safe_remove(mkv_path, "size mismatch")
                return False
            else:
                logging.info(f"Size check passed (ratio: {size_ratio:.4f}, required: {required_ratio})")
        
        return True
    
    def _verify_duration(self, dcv_path: str, mkv_path: str) -> bool:
        """Verify duration"""
        logging.debug(f"Comparing duration")
        dcv_duration = get_media_duration(dcv_path)
        mkv_duration = get_media_duration(mkv_path)
        
        if dcv_duration is None or mkv_duration is None:
            logging.error(f"Cannot get MediaInfo duration, verification failed")
            safe_remove(mkv_path, "duration check failed")
            return False
        
        duration_diff = abs(dcv_duration - mkv_duration)
        if duration_diff > 6.0:
            logging.error(f"Duration mismatch: DCV={dcv_duration:.2f}s, MKV={mkv_duration:.2f}s (difference: {duration_diff:.2f}s > 6s)")
            safe_remove(mkv_path, "duration mismatch")
            return False
        else:
            logging.info(f"Duration check passed (difference: {duration_diff:.2f}s)")
        
        return True
    
    def process_single_file(self, filename: str) -> bool:
        """Process complete workflow for a single file"""
        logging.info(f"=== Starting to process file: {filename} ===")
        
        with TemporaryDirectory(prefix="rclone_decrypt_") as temp_dir:
            temp_path = Path(temp_dir)
            
            try:
                # Check disk space
                if not check_disk_space(temp_dir, 10.0):
                    logging.error(f"Insufficient disk space in temporary directory: {temp_dir}")
                    return False
                
                # 1. Download DCV file
                logging.info("Step 1: Download DCV file")
                local_dcv_path = self.download_dcv_file(filename, temp_dir)
                if not local_dcv_path:
                    logging.error(f"Download failed: {filename}")
                    return False
                
                # 2. Decrypt file
                logging.info("Step 2: Decrypt DCV file")
                local_mkv_path = self.decrypt_dcv_file(local_dcv_path, temp_dir)
                if not local_mkv_path:
                    logging.error(f"Decryption failed: {filename}")
                    return False
                
                # 3. Upload MKV file
                logging.info("Step 3: Upload MKV file")
                if not self.upload_mkv_file(local_mkv_path, filename):
                    logging.error(f"Upload failed: {filename}")
                    return False
                
                logging.info(f"✅ File processing successful: {filename}")
                return True
                
            except Exception as e:
                logging.error(f"Error occurred while processing file {filename}: {e}")
                return False
    
    def process_files(self, file_list: Optional[List[str]] = None) -> Tuple[int, int]:
        """Process file list"""
        if file_list is None:
            file_list = self.list_remote_dcv_files()
        
        if not file_list:
            logging.warning("No DCV files found to process")
            return 0, 0
        
        logging.info(f"Starting to process {len(file_list)} files")
        successful = 0
        failed = 0
        
        for i, filename in enumerate(file_list, 1):
            logging.info(f"--- Processing file {i}/{len(file_list)}: {filename} ---")
            
            try:
                if self.process_single_file(filename):
                    successful += 1
                    logging.info(f"✅ File {i}/{len(file_list)} processed successfully: {filename}")
                else:
                    failed += 1
                    logging.error(f"❌ File {i}/{len(file_list)} processing failed: {filename}")
            
            except KeyboardInterrupt:
                logging.warning("User interrupted processing")
                break
            except Exception as e:
                logging.error(f"Unexpected error occurred while processing file {filename}: {e}")
                failed += 1
            
            # Pause briefly between files
            if i < len(file_list):
                time.sleep(2)
        
        return successful, failed


# Configuration creation is now handled by shared_utils.create_default_config()


def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Rclone DCV Decryption Processor")
    parser.add_argument('--remote', '-r', default=None,
                        help='Rclone remote name (default: from config file)')
    parser.add_argument('--files', '-f', nargs='+',
                        help='Specify list of filenames to process')
    parser.add_argument('--list', '-l', action='store_true',
                        help='Only list remote DCV files')
    parser.add_argument('--create-config', action='store_true',
                        help='Create default configuration file')
    
    args = parser.parse_args()
    
    # Create configuration file
    if args.create_config:
        create_default_config(CONFIG_FILE)
        return 0
    
    start_time = datetime.now()
    print(f"Rclone DCV Decryption Processor started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Check dependencies
    if not check_dependencies():
        print("❌ Dependency check failed, please install required components")
        return 1
    
    # Load configuration
    if not Path(CONFIG_FILE).exists():
        print(f"Configuration file '{CONFIG_FILE}' does not exist")
        print("Use --create-config to create default configuration file")
        return 1
    
    config = configparser.ConfigParser(interpolation=None)
    try:
        config.read(CONFIG_FILE, encoding='utf-8')
        print(f"Configuration loaded from {CONFIG_FILE}")
    except Exception as e:
        print(f"Failed to load configuration file: {e}", file=sys.stderr)
        return 1
    
    # Setup logging
    try:
        paths = config['Paths']
        setup_logging(paths['log_file_all'], paths['log_file_errors'])
        logging.info("Rclone decryption processor logging initialized")
        logging.info(f"Using configuration file: {CONFIG_FILE}")
        logging.info(f"Running platform: {platform.system()} {platform.release()}")
    except Exception as e:
        print(f"Failed to setup logging: {e}", file=sys.stderr)
        return 1
    
    # Create processor
    try:
        processor = RcloneDecryptProcessor(config, args.remote)
    except FileNotFoundError as e:
        logging.critical(str(e))
        return 1
    except ValueError as e:
        logging.error(str(e))
        print(f"❌ {e}")
        print("   Add [Rclone] section to config.ini to use rclone features.")
        return 1
    
    # Check rclone availability
    if not processor.check_rclone_available():
        return 1
    
    # List files only
    if args.list:
        dcv_files = processor.list_remote_dcv_files()
        if dcv_files:
            print(f"\nFound DCV files ({len(dcv_files)}):")
            for i, filename in enumerate(dcv_files, 1):
                print(f"  {i}. {filename}")
        else:
            print("No DCV files found")
        return 0
    
    # Process files
    try:
        successful, failed = processor.process_files(args.files)
    except KeyboardInterrupt:
        logging.warning("Processing interrupted by user")
        print("\n⚠️  Processing interrupted!")
        return 1
    except Exception as e:
        logging.error(f"Unexpected error occurred during processing: {e}")
        print(f"❌ Unexpected error occurred: {e}")
        return 1
    
    # Final summary
    end_time = datetime.now()
    duration = end_time - start_time
    
    print("\n" + "="*60)
    print("PROCESSING SUMMARY")
    print("="*60)
    print(f"Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"End Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Duration: {duration}")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print("="*60)
    
    logging.info(f"Processing completed. Successful: {successful}, Failed: {failed}")
    
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)
