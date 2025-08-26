"""
Shared utilities for DMM batch downloader and decryptor.
Contains common functions for configuration, logging, and file operations.
"""

import json
import shutil
import subprocess
import logging
import configparser
import sys
import platform
from pathlib import Path
from datetime import datetime
from typing import List, Tuple, Optional, Dict
import threading

# Third-party libraries
import requests
from requests.exceptions import ConnectionError, HTTPError, Timeout, RequestException

# Constants
CONFIG_FILE = "config.ini"
DECRYPT_CONN_ERROR_MSG = "no internet connection"

# Thread-safe logging lock
log_lock = threading.Lock()

class ThreadSafeLogger:
    """Thread-safe logger wrapper"""
    
    @staticmethod
    def log(level: int, message: str):
        with log_lock:
            logging.log(level, message)
    
    @staticmethod
    def info(message: str):
        ThreadSafeLogger.log(logging.INFO, message)
    
    @staticmethod
    def warning(message: str):
        ThreadSafeLogger.log(logging.WARNING, message)
    
    @staticmethod
    def error(message: str):
        ThreadSafeLogger.log(logging.ERROR, message)
    
    @staticmethod
    def debug(message: str):
        ThreadSafeLogger.log(logging.DEBUG, message)
    
    @staticmethod
    def critical(message: str):
        ThreadSafeLogger.log(logging.CRITICAL, message)

# ---------------- DEPENDENCY CHECKING ----------------

def check_dependencies() -> bool:
    """Check for required dependencies and provide download links if missing."""
    errors = []
    script_dir = Path(__file__).parent.absolute()
    
    print("Checking dependencies...")
    
    jav_it_name = "jav-it.exe" if platform.system() == "Windows" else "jav-it"
    jav_it_path = script_dir / jav_it_name
    
    if not jav_it_path.exists():
        errors.append(f"❌ {jav_it_name} not found in script directory")
        errors.append(f"   Download from: https://www.patreon.com/c/AsahiMintia/posts")
        errors.append(f"   Place it in: {script_dir}")
    else:
        print(f"✅ {jav_it_name} found")
    
    decrypt_mod_path = script_dir / "DECRYPT.MOD"
    
    if not decrypt_mod_path.exists():
        errors.append(f"❌ DECRYPT.MOD not found in script directory")
        errors.append(f"   Download from: https://www.patreon.com/posts/decryption-1-0-103706011")
        errors.append(f"   Place it in: {script_dir}")
    else:
        print("✅ DECRYPT.MOD found")
    
    # Check for MediaInfo CLI
    try:
        result = subprocess.run(['mediainfo', '--version'], 
                              capture_output=True, text=True, timeout=5)
        if result.returncode == 0:
            version_line = result.stdout.split('\n')[0] if result.stdout else "Unknown version"
            print(f"✅ MediaInfo CLI found: {version_line}")
        else:
            raise subprocess.CalledProcessError(result.returncode, 'mediainfo')
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError):
        errors.append("❌ MediaInfo CLI not found or not working")
        errors.append("   Download CLI version from: https://mediaarea.net/en/MediaInfo/Download")
        errors.append("   Make sure it's added to your system PATH")
    
    # Check for FFmpeg
    try:
        result = subprocess.run(['ffmpeg', '-version'], 
                              capture_output=True, text=True, timeout=5)
        if result.returncode == 0:
            version_line = result.stdout.split('\n')[0] if result.stdout else "Unknown version"
            print(f"✅ FFmpeg found: {version_line}")
        else:
            raise subprocess.CalledProcessError(result.returncode, 'ffmpeg')
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError):
        errors.append("❌ FFmpeg not found or not working")
        errors.append("   Download from: https://ffmpeg.org/download.html")
        errors.append("   Make sure it's added to your system PATH")
    
    if errors:
        print("\n" + "="*60)
        print("❌ DEPENDENCY CHECK FAILED")
        print("="*60)
        for error in errors:
            print(error)
        print("\nPlease install the missing dependencies and try again.")
        print("="*60)
        return False
    
    print("✅ All dependencies found!")
    return True

# ---------------- CONFIGURATION MANAGEMENT ----------------

def expand_user_path(path_str: str) -> str:
    """Expand user path (~) to absolute path in a cross-platform way."""
    if path_str.startswith('~'):
        return str(Path(path_str).expanduser())
    return path_str

def create_default_config(file_path: str):
    """Create a default config.ini file."""
    default_config = configparser.ConfigParser(interpolation=None)
    home_dir = Path.home()
    
    if platform.system() == "Windows":
        decrypt_executable = "jav-it.exe"
        default_decrypt_dir = home_dir / "Videos" / "DecryptedVideos"
        default_failed_dir = home_dir / "Videos" / "FailedVideos"
    else:
        decrypt_executable = "jav-it"
        default_decrypt_dir = home_dir / "DecryptedVideos"
        default_failed_dir = home_dir / "FailedVideos"
    
    default_config['Paths'] = {
        'ids_file': 'ids.txt',
        'download_dir': './downloaded',
        'decrypt_output_dir': str(default_decrypt_dir),
        'failed_dir': str(default_failed_dir),
        'decrypt_executable': decrypt_executable,
        'log_file_all': 'process_all.log',
        'log_file_errors': 'process_errors.log',
        'failed_ids_file': 'failed_ids.txt',
        'dcv_files_dir': './dcv_files'
    }
    default_config['Network'] = {
        'user_agent': 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)',
        'cookie': 'age_check_done=1; ; INT_SESID=YOUR-SESSION-ID; licenseUID=YOUR-LICENSE-ID',
        'http_proxy': '',
        'https_proxy': ''
    }
    default_config['Settings'] = {
        'max_retries': '3',
        'retry_delay_seconds': '5',
        'decrypt_retry_delay_seconds': '100',
        'pause_between_ids': '2',
        'download_threads': '3',
        'decrypt_threads': '2'
    }
    try:
        with open(file_path, 'w', encoding='utf-8') as configfile:
            default_config.write(configfile)
        print(f"Default configuration file created at '{file_path}'.")
        print("Please review and edit it with your actual settings (especially the cookie).")
    except OSError as e:
        print(f"Error writing default configuration file {file_path}: {e}", file=sys.stderr)
        sys.exit(1)

def validate_configuration(config: configparser.ConfigParser):
    """Perform detailed validation of configuration parameters."""
    errors = []

    try:
        ids_file = config.get('Paths', 'ids_file')
        if not Path(ids_file).exists():
            errors.append(f"[Paths] 'ids_file': File not found at '{ids_file}'. Please create it.")

        if not config.get('Paths', 'decrypt_executable'):
             errors.append("[Paths] 'decrypt_executable': Path cannot be empty.")

        required_dirs = ['download_dir', 'decrypt_output_dir', 'failed_dir']
        for dir_key in required_dirs:
            if not config.get('Paths', dir_key):
                 errors.append(f"[Paths] '{dir_key}': Path cannot be empty.")

    except configparser.NoOptionError as e:
        errors.append(f"Missing required option in [Paths]: {e.option}")
    except Exception as e:
         errors.append(f"Unexpected error validating [Paths]: {e}")

    try:
        cookie = config.get('Network', 'cookie', fallback='').strip()
        if not cookie:
            errors.append("[Network] 'cookie': Value cannot be empty. Please provide your DMM cookie.")
        elif 'example_key=example_value' in cookie:
            errors.append("[Network] 'cookie': Value seems to be the default example. Please replace it with your actual DMM cookie.")

        if not config.get('Network', 'user_agent', fallback=''):
             errors.append("[Network] 'user_agent': Value cannot be empty.")

    except configparser.NoOptionError as e:
        errors.append(f"Missing required option in [Network]: {e.option}")
    except Exception as e:
         errors.append(f"Unexpected error validating [Network]: {e}")

    try:
        positive_ints = ['max_retries', 'retry_delay_seconds', 'decrypt_retry_delay_seconds', 
                        'pause_between_ids', 'download_threads', 'decrypt_threads']
        for key in positive_ints:
            try:
                value = config.getint('Settings', key, fallback=1 if 'threads' in key else 3)
                if value < 1 if 'threads' in key else value < 0:
                    min_val = 1 if 'threads' in key else 0
                    errors.append(f"[Settings] '{key}': Value '{value}' must be >= {min_val}.")
            except ValueError:
                errors.append(f"[Settings] '{key}': Invalid integer value '{config.get('Settings', key)}'.")
            except configparser.NoOptionError:
                if 'threads' not in key:  # Thread settings have defaults
                    errors.append(f"[Settings] Missing required option: '{key}'.")

    except Exception as e:
         errors.append(f"Unexpected error validating [Settings]: {e}")

    if errors:
        error_message = f"Configuration validation failed ({CONFIG_FILE}):\n" + "\n".join(f"- {e}" for e in errors)
        raise ValueError(error_message)
    else:
        ThreadSafeLogger.info("Configuration parameters validated successfully.")

def load_configuration(file_path: str) -> configparser.ConfigParser:
    """Load and validate configuration from the INI file."""
    if not Path(file_path).exists():
        print(f"Configuration file '{file_path}' not found.", file=sys.stderr)
        create_default_config(file_path)
        sys.exit(1)

    config = configparser.ConfigParser(interpolation=None)
    try:
        config.read(file_path, encoding='utf-8')
        required_sections = ['Paths', 'Network', 'Settings']
        for section in required_sections:
            if section not in config:
                raise ValueError(f"Missing required section [{section}] in {file_path}")
        validate_configuration(config)
        print(f"Configuration loaded and validated successfully from {file_path}")
        return config
    except (configparser.Error, ValueError) as e:
        if logging.getLogger().hasHandlers():
             ThreadSafeLogger.critical(f"Configuration error: {e}")
        else:
             print(f"Configuration error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        if logging.getLogger().hasHandlers():
             ThreadSafeLogger.critical(f"Unexpected error loading configuration: {e}")
        else:
             print(f"Unexpected error loading configuration: {e}", file=sys.stderr)
        sys.exit(1)

# ---------------- LOGGING SETUP ----------------

def setup_logging(log_file_all: str, log_file_errors: str):
    """Configure the logging module with thread-safe handlers."""
    log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - [%(threadName)s] - %(filename)s:%(lineno)d - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s')

    root_logger = logging.getLogger()
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    root_logger.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)

    # Create directories for log files if needed
    for log_file in [log_file_all, log_file_errors]:
        log_path = Path(log_file)
        log_dir = log_path.parent
        if log_dir != Path() and not log_dir.exists():
            try:
                log_dir.mkdir(parents=True, exist_ok=True)
            except OSError as e:
                print(f"Warning: Could not create directory for log file {log_file}: {e}", file=sys.stderr)

    # All events log file
    try:
        file_handler_all = logging.FileHandler(log_file_all, mode='a', encoding='utf-8')
        file_handler_all.setLevel(logging.DEBUG)
        file_handler_all.setFormatter(log_formatter)
        root_logger.addHandler(file_handler_all)
    except OSError as e:
        print(f"Error setting up log file {log_file_all}: {e}", file=sys.stderr)

    # Error events log file
    try:
        file_handler_errors = logging.FileHandler(log_file_errors, mode='a', encoding='utf-8')
        file_handler_errors.setLevel(logging.ERROR)
        file_handler_errors.setFormatter(log_formatter)
        root_logger.addHandler(file_handler_errors)
    except OSError as e:
        print(f"Error setting up error log file {log_file_errors}: {e}", file=sys.stderr)

# ---------------- UTILITY FUNCTIONS ----------------

def has_conformance_errors(file_path: str) -> bool:
    """Use MediaInfo to check for conformance errors in a media file."""
    file_path_obj = Path(file_path)
    if not file_path_obj.exists():
        ThreadSafeLogger.error(f"MediaInfo conformance check failed: File not found at {file_path}")
        return True

    try:
        cmd = ["mediainfo", "--Output=JSON", file_path]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True, encoding='utf-8', errors='replace')
        media_info = json.loads(result.stdout)

        general_track = media_info.get('media', {}).get('track', [{}])[0]
        if general_track.get('@type') == 'General':
            error_count = int(general_track.get('Error_Count', '0'))
            if error_count > 0:
                ThreadSafeLogger.warning(f"MediaInfo found {error_count} conformance error(s) in {file_path_obj.name}.")
                if 'Error' in general_track:
                    ThreadSafeLogger.warning(f"  - Details: {general_track['Error']}")
                return True
            
            if general_track.get('IsTruncated') == 'Yes':
                ThreadSafeLogger.warning(f"MediaInfo reports the file is truncated: {file_path_obj.name}.")
                return True

        return False
    except FileNotFoundError:
        ThreadSafeLogger.error("`mediainfo` command not found. Cannot perform conformance check.")
        return False
    except (subprocess.CalledProcessError, json.JSONDecodeError, KeyError, IndexError, ValueError) as e:
        ThreadSafeLogger.error(f"Could not perform MediaInfo conformance check on {file_path}: {e}")
        return True
    except Exception as e:
        ThreadSafeLogger.error(f"Unexpected error in has_conformance_errors for {file_path}: {e}")
        return True

def get_media_duration(file_path: str) -> Optional[float]:
    """Use MediaInfo to get the duration of a media file in seconds."""
    if not Path(file_path).exists():
        ThreadSafeLogger.error(f"MediaInfo check failed: File not found at {file_path}")
        return None

    try:
        cmd = ["mediainfo", "--Output=JSON", file_path]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True, encoding='utf-8', errors='replace')
        media_info = json.loads(result.stdout)

        tracks = media_info.get('media', {}).get('track', [])
        if not tracks:
            ThreadSafeLogger.warning(f"No tracks found in MediaInfo output for {file_path}")
            return None

        duration_str = tracks[0].get('Duration')
        if duration_str:
            return float(duration_str)
        else:
            ThreadSafeLogger.warning(f"Could not find 'Duration' in MediaInfo output for {file_path}")
            return None
    except FileNotFoundError:
        ThreadSafeLogger.error("`mediainfo` command not found. Please ensure it is installed and in your system's PATH.")
        return None
    except subprocess.CalledProcessError as e:
        ThreadSafeLogger.error(f"MediaInfo returned an error for {file_path}: {e.stderr or e.stdout}")
        return None
    except (json.JSONDecodeError, KeyError, IndexError, TypeError, ValueError) as e:
        ThreadSafeLogger.error(f"Error parsing MediaInfo JSON output for {file_path}: {e}")
        return None
    except Exception as e:
        ThreadSafeLogger.error(f"An unexpected error occurred in get_media_duration for {file_path}: {e}")
        return None

def write_failed_id(cid: str, file_path: str):
    """Append a failed ID to the specified failure file (thread-safe)."""
    file_path_obj = Path(file_path)
    fail_dir = file_path_obj.parent
    if fail_dir != Path() and not fail_dir.exists():
        try:
            fail_dir.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            ThreadSafeLogger.error(f"Could not create directory for failed IDs file {file_path}: {e}")
            return
    
    # Use a lock for thread-safe file writing
    with threading.Lock():
        try:
            with open(file_path, "a", encoding="utf8") as f:
                f.write(f"{cid}\n")
            ThreadSafeLogger.info(f"Recorded failed ID {cid} to {file_path}")
        except OSError as e:
            ThreadSafeLogger.error(f"Could not write failed ID {cid} to {file_path}: {e}")

def remove_id_from_file(cid: str, file_path: str):
    """Remove a specific ID (line) from a file (thread-safe)."""
    if not Path(file_path).exists():
        ThreadSafeLogger.warning(f"Cannot remove ID {cid}, file not found: {file_path}")
        return
    
    # Use a lock for thread-safe file operations
    with threading.Lock():
        try:
            with open(file_path, "r", encoding="utf8") as f:
                lines = f.readlines()
            updated_lines = [line for line in lines if line.strip() != cid]

            if len(updated_lines) < len(lines):
                with open(file_path, "w", encoding="utf8") as f:
                    f.writelines(updated_lines)
                ThreadSafeLogger.info(f"Removed ID {cid} from {file_path}")
            else:
                ThreadSafeLogger.warning(f"ID {cid} not found in {file_path} for removal.")
        except OSError as e:
            ThreadSafeLogger.error(f"Error processing file {file_path} for removing ID {cid}: {e}")

def move_files(file_list: List[str], destination_folder: str, cid: str):
    """Move specified files to the destination folder, creating it if necessary."""
    if not file_list:
        return
    try:
        destination_path = Path(destination_folder)
        cid_destination_folder = destination_path / cid
        cid_destination_folder.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        ThreadSafeLogger.error(f"Could not create destination directory {cid_destination_folder} for ID {cid}: {e}")
        return

    ThreadSafeLogger.info(f"Moving {len(file_list)} file(s) for failed ID {cid} to {cid_destination_folder}")
    for file_path in file_list:
        try:
            file_path_obj = Path(file_path)
            if file_path_obj.exists():
                dest_path = cid_destination_folder / file_path_obj.name
                if dest_path.exists():
                    ThreadSafeLogger.warning(f"Destination file already exists, skipping move: {dest_path}")
                    continue
                shutil.move(str(file_path_obj), str(dest_path))
                ThreadSafeLogger.debug(f"Moved {file_path} to {dest_path}")
            else:
                ThreadSafeLogger.warning(f"File not found for moving: {file_path}")
        except (OSError, shutil.Error) as e:
            ThreadSafeLogger.error(f"Error moving file {file_path} to {cid_destination_folder}: {e}")

def safe_getsize(file_path: str) -> Optional[int]:
    """Safely get file size, returning None on error."""
    try:
        return Path(file_path).stat().st_size
    except OSError as e:
        ThreadSafeLogger.error(f"Error getting size of file {file_path}: {e}")
        return None

def safe_remove(file_path: str, purpose: str = "cleanup"):
    """Safely remove a file, logging errors."""
    try:
        file_path_obj = Path(file_path)
        if file_path_obj.exists():
            file_path_obj.unlink()
            ThreadSafeLogger.debug(f"Removed file ({purpose}): {file_path}")
    except OSError as e:
        ThreadSafeLogger.error(f"Error removing file ({purpose}) {file_path}: {e}")

def check_disk_space(path: str, required_gb: float = 30.0) -> bool:
    """Check if there's enough free disk space at the given path."""
    try:
        path_obj = Path(path)
        if not path_obj.exists():
            path_obj.mkdir(parents=True, exist_ok=True)
        
        # Use shutil.disk_usage for cross-platform compatibility
        _, _, free_bytes = shutil.disk_usage(path)
        free_gb = free_bytes / (1024**3)
        ThreadSafeLogger.debug(f"Free space at {path}: {free_gb:.2f} GB")
        
        if free_gb < required_gb:
            ThreadSafeLogger.error(f"Insufficient disk space at {path}: {free_gb:.2f} GB available, {required_gb} GB required")
            return False
        else:
            ThreadSafeLogger.info(f"Disk space check passed for {path}: {free_gb:.2f} GB available")
            return True
            
    except Exception as e:
        ThreadSafeLogger.error(f"Error checking disk space for {path}: {e}")
        return False

# ---------------- NETWORK OPERATIONS with Retries ----------------

def make_request(method: str, url: str, headers: Dict,
                 max_retries: int, retry_delay: int, cid_context: str,
                 allow_redirects: bool = True, data: Optional[Dict] = None,
                 stream: bool = False, timeout: int = 30, 
                 proxies: Optional[Dict] = None) -> Optional[requests.Response]:
    """Make an HTTP request with retry logic."""
    for attempt in range(max_retries):
        try:
            response = requests.request(
                method, url, headers=headers, data=data, timeout=timeout,
                allow_redirects=allow_redirects, stream=stream, proxies=proxies
            )
            response.raise_for_status()
            return response
        except Timeout as e:
            ThreadSafeLogger.warning(f"Attempt {attempt + 1}/{max_retries}: Timeout ({cid_context}): {url} - {e}")
        except ConnectionError as e:
            ThreadSafeLogger.warning(f"Attempt {attempt + 1}/{max_retries}: Connection error ({cid_context}): {url} - {e}")
        except HTTPError as e:
            ThreadSafeLogger.error(f"Attempt {attempt + 1}/{max_retries}: HTTP error ({cid_context}): {url} - {e.response.status_code} {e.response.reason}")
            if 400 <= e.response.status_code < 500 and e.response.status_code != 429:
                 return None
        except RequestException as e:
            ThreadSafeLogger.warning(f"Attempt {attempt + 1}/{max_retries}: Network error ({cid_context}): {url} - {e}")
        except Exception as e:
            ThreadSafeLogger.error(f"Unexpected error during request ({cid_context}) on attempt {attempt + 1}: {url} - {e}")

        if attempt < max_retries - 1:
            ThreadSafeLogger.info(f"Waiting {retry_delay} seconds before retrying {cid_context} request...")
            import time
            time.sleep(retry_delay)
        else:
            ThreadSafeLogger.error(f"Failed request for {cid_context} after {max_retries} attempts: {url}")
            return None
    return None

def read_ids_from_file(file_path: str) -> List[str]:
    """Read IDs from a text file, ignoring comments and empty lines."""
    try:
        with open(file_path, "r", encoding="utf8") as f:
            ids = [
                line.strip() for line in f
                if line.strip() and not line.strip().startswith('#')
            ]
        return ids
    except OSError as e:
        ThreadSafeLogger.error(f"Error reading IDs file '{file_path}': {e}")
        return []

def setup_directories(config: configparser.ConfigParser):
    """Create all required directories from configuration."""
    paths = config['Paths']
    required_dirs = [
        expand_user_path(paths['download_dir']), 
        expand_user_path(paths['decrypt_output_dir']),
        expand_user_path(paths['failed_dir']),
        expand_user_path(paths.get('dcv_files_dir', './dcv_files'))
    ]
    
    # Create parent directories for log files
    parent_dirs = {Path(p).parent for p in [paths['log_file_all'], paths['log_file_errors'], paths['failed_ids_file']] if Path(p).parent != Path()}
    
    for folder in required_dirs + [str(p) for p in parent_dirs]:
        try:
            Path(folder).mkdir(parents=True, exist_ok=True)
            ThreadSafeLogger.debug(f"Ensured directory exists: {folder}")
        except OSError as e:
            ThreadSafeLogger.critical(f"Could not create required directory {folder}: {e}")
            raise
