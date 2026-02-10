import json
import re
import shutil
import subprocess
import time
import logging
import configparser
import sys
import platform
from pathlib import Path
from datetime import datetime
from typing import List, Tuple, Optional, Dict

# Third-party libraries
import requests
from bs4 import BeautifulSoup, Tag
from requests.exceptions import ConnectionError, HTTPError, Timeout, RequestException
from tqdm import tqdm

# Constants
CONFIG_FILE = "config.ini"
DECRYPT_CONN_ERROR_MSG = "no internet connection"  # Error message for decryption retry logic

# ---------------- DEPENDENCY CHECKING ----------------

def check_dependencies() -> bool:
    """Checks for required dependencies and provides download links if missing."""
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
    """Expands user path (~) to absolute path in a cross-platform way."""
    if path_str.startswith('~'):
        return str(Path(path_str).expanduser())
    return path_str

def create_default_config(file_path: str):
    """Creates a default config.ini file."""
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
        'failed_ids_file': 'failed_ids.txt'
    }
    default_config['Network'] = {
        'user_agent': 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)',
        'cookie': 'age_check_done=1; example_key=example_value', # Simpler cookie value
        'proxy': ''  # Proxy for both HTTP and HTTPS
    }
    default_config['Settings'] = {
        'max_retries': '3',
        'retry_delay_seconds': '5',
        'decrypt_retry_delay_seconds': '100', # Set to 100 as requested
        'pause_between_ids': '2'
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
    """Performs detailed validation of configuration parameters."""
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
        positive_ints = ['max_retries', 'retry_delay_seconds', 'decrypt_retry_delay_seconds', 'pause_between_ids']
        for key in positive_ints:
            try:
                value = config.getint('Settings', key)
                if value < 0:
                    errors.append(f"[Settings] '{key}': Value '{value}' must be a non-negative integer.")
            except ValueError:
                errors.append(f"[Settings] '{key}': Invalid integer value '{config.get('Settings', key)}'.")
            except configparser.NoOptionError:
                errors.append(f"[Settings] Missing required option: '{key}'.")

    except Exception as e:
         errors.append(f"Unexpected error validating [Settings]: {e}")


    if errors:
        error_message = f"Configuration validation failed ({CONFIG_FILE}):\n" + "\n".join(f"- {e}" for e in errors)
        raise ValueError(error_message)
    else:
        logging.info("Configuration parameters validated successfully.")


def load_configuration(file_path: str) -> configparser.ConfigParser:
    """Loads and validates configuration from the INI file."""
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
             logging.critical(f"Configuration error: {e}")
        else:
             print(f"Configuration error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        if logging.getLogger().hasHandlers():
             logging.critical(f"Unexpected error loading configuration: {e}", exc_info=True)
        else:
             print(f"Unexpected error loading configuration: {e}", file=sys.stderr)
        sys.exit(1)


# ---------------- LOGGING SETUP ----------------

def setup_logging(log_file_all: str, log_file_errors: str):
    """Configures the logging module."""
    log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    root_logger = logging.getLogger()
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    root_logger.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)

    log_path_all = Path(log_file_all)
    log_dir_all = log_path_all.parent
    if log_dir_all != Path() and not log_dir_all.exists():
        try:
            log_dir_all.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            print(f"Warning: Could not create directory for log file {log_file_all}: {e}. Log file may not be created.", file=sys.stderr)
    try:
        file_handler_all = logging.FileHandler(log_file_all, mode='a', encoding='utf-8')
        file_handler_all.setLevel(logging.DEBUG)
        file_handler_all.setFormatter(log_formatter)
        root_logger.addHandler(file_handler_all)
    except OSError as e:
        print(f"Error setting up log file {log_file_all}: {e}. Logging to this file disabled.", file=sys.stderr)

    log_path_errors = Path(log_file_errors)
    log_dir_errors = log_path_errors.parent
    if log_dir_errors != Path() and not log_dir_errors.exists():
         try:
             log_dir_errors.mkdir(parents=True, exist_ok=True)
         except OSError as e:
             print(f"Warning: Could not create directory for error log file {log_file_errors}: {e}. Error log file may not be created.", file=sys.stderr)
    try:
        file_handler_errors = logging.FileHandler(log_file_errors, mode='a', encoding='utf-8')
        file_handler_errors.setLevel(logging.ERROR)
        file_handler_errors.setFormatter(log_formatter)
        root_logger.addHandler(file_handler_errors)
    except OSError as e:
        print(f"Error setting up error log file {log_file_errors}: {e}. Error logging to file disabled.", file=sys.stderr)


# ---------------- UTILITY FUNCTIONS ----------------

def has_conformance_errors(file_path: str) -> bool:
    """Uses MediaInfo to check for conformance errors in a media file."""
    file_path_obj = Path(file_path)
    if not file_path_obj.exists():
        logging.error(f"MediaInfo conformance check failed: File not found at {file_path}")
        return True

    try:
        cmd = ["mediainfo", "--Output=JSON", file_path]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True, encoding='utf-8', errors='replace')
        media_info = json.loads(result.stdout)

        general_track = media_info.get('media', {}).get('track', [{}])[0]
        if general_track.get('@type') == 'General':
            error_count = int(general_track.get('Error_Count', '0'))
            if error_count > 0:
                logging.warning(f"MediaInfo found {error_count} conformance error(s) in {file_path_obj.name}.")
                if 'Error' in general_track:
                    logging.warning(f"  - Details: {general_track['Error']}")
                return True
            
            if general_track.get('IsTruncated') == 'Yes':
                logging.warning(f"MediaInfo reports the file is truncated: {file_path_obj.name}.")
                return True

        return False
    except FileNotFoundError:
        logging.error("`mediainfo` command not found. Cannot perform conformance check.")
        return False
    except (subprocess.CalledProcessError, json.JSONDecodeError, KeyError, IndexError, ValueError) as e:
        logging.error(f"Could not perform MediaInfo conformance check on {file_path}: {e}")
        return True
    except Exception as e:
        logging.exception(f"Unexpected error in has_conformance_errors for {file_path}")
        return True

def get_media_duration(file_path: str) -> Optional[float]:
    """Uses MediaInfo to get the duration of a media file in seconds."""
    if not Path(file_path).exists():
        logging.error(f"MediaInfo check failed: File not found at {file_path}")
        return None

    try:
        cmd = ["mediainfo", "--Output=JSON", file_path]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True, encoding='utf-8', errors='replace')
        media_info = json.loads(result.stdout)

        tracks = media_info.get('media', {}).get('track', [])
        if not tracks:
            logging.warning(f"No tracks found in MediaInfo output for {file_path}")
            return None

        duration_str = tracks[0].get('Duration')
        if duration_str:
            return float(duration_str)
        else:
            logging.warning(f"Could not find 'Duration' in MediaInfo output for {file_path}")
            return None
    except FileNotFoundError:
        logging.error("`mediainfo` command not found. Please ensure it is installed and in your system's PATH.")
        return None
    except subprocess.CalledProcessError as e:
        logging.error(f"MediaInfo returned an error for {file_path}: {e.stderr or e.stdout}")
        return None
    except (json.JSONDecodeError, KeyError, IndexError, TypeError, ValueError) as e:
        logging.error(f"Error parsing MediaInfo JSON output for {file_path}: {e}")
        return None
    except Exception as e:
        logging.exception(f"An unexpected error occurred in get_media_duration for {file_path}")
        return None


def write_failed_id(cid: str, file_path: str):
    """Appends a failed ID to the specified failure file."""
    file_path_obj = Path(file_path)
    fail_dir = file_path_obj.parent
    if fail_dir != Path() and not fail_dir.exists():
        try:
            fail_dir.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            logging.error(f"Could not create directory for failed IDs file {file_path}: {e}. Failed ID {cid} not recorded.")
            return
    try:
        with open(file_path, "a", encoding="utf8") as f:
            f.write(f"{cid}\n")
        logging.info(f"Recorded failed ID {cid} to {file_path}")
    except OSError as e:
        logging.error(f"Could not write failed ID {cid} to {file_path}: {e}")

def remove_id_from_file(cid: str, file_path: str):
    """Removes a specific ID (line) from a file."""
    if not Path(file_path).exists():
        logging.warning(f"Cannot remove ID {cid}, file not found: {file_path}")
        return
    try:
        with open(file_path, "r", encoding="utf8") as f:
            lines = f.readlines()
        updated_lines = [line for line in lines if line.strip() != cid]

        if len(updated_lines) < len(lines):
            with open(file_path, "w", encoding="utf8") as f:
                f.writelines(updated_lines)
            logging.info(f"Removed ID {cid} from {file_path}")
        else:
            logging.warning(f"ID {cid} not found in {file_path} for removal.")
    except OSError as e:
        logging.error(f"Error processing file {file_path} for removing ID {cid}: {e}")

def move_files(file_list: List[str], destination_folder: str, cid: str):
    """Moves specified files to the destination folder, creating it if necessary."""
    if not file_list:
        return
    try:
        destination_path = Path(destination_folder)
        cid_destination_folder = destination_path / cid
        cid_destination_folder.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        logging.error(f"Could not create destination directory {cid_destination_folder} for ID {cid}: {e}. Files not moved.")
        return

    logging.info(f"Moving {len(file_list)} file(s) for failed ID {cid} to {cid_destination_folder}")
    for file_path in file_list:
        try:
            file_path_obj = Path(file_path)
            if file_path_obj.exists():
                dest_path = cid_destination_folder / file_path_obj.name
                if dest_path.exists():
                    logging.warning(f"Destination file already exists, skipping move: {dest_path}")
                    continue
                shutil.move(str(file_path_obj), str(dest_path))
                logging.debug(f"Moved {file_path} to {dest_path}")
            else:
                logging.warning(f"File not found for moving: {file_path}")
        except (OSError, shutil.Error) as e:
            logging.error(f"Error moving file {file_path} to {cid_destination_folder}: {e}")

def safe_getsize(file_path: str) -> Optional[int]:
    """Safely get file size, returning None on error."""
    try:
        return Path(file_path).stat().st_size
    except OSError as e:
        logging.error(f"Error getting size of file {file_path}: {e}")
        return None

def safe_remove(file_path: str, purpose: str = "cleanup"):
    """Safely remove a file, logging errors."""
    try:
        file_path_obj = Path(file_path)
        if file_path_obj.exists():
            file_path_obj.unlink()
            logging.debug(f"Removed file ({purpose}): {file_path}")
    except OSError as e:
        logging.error(f"Error removing file ({purpose}) {file_path}: {e}")

def check_disk_space(path: str, required_gb: float = 30.0) -> bool:
    """Check if there's enough free disk space at the given path."""
    try:
        path_obj = Path(path)
        if not path_obj.exists():
            path_obj.mkdir(parents=True, exist_ok=True)
        
        # Use shutil.disk_usage for cross-platform compatibility
        _, _, free_bytes = shutil.disk_usage(path)
        free_gb = free_bytes / (1024**3)
        logging.debug(f"Free space at {path}: {free_gb:.2f} GB")
        
        if free_gb < required_gb:
            logging.error(f"Insufficient disk space at {path}: {free_gb:.2f} GB available, {required_gb} GB required")
            return False
        else:
            logging.info(f"Disk space check passed for {path}: {free_gb:.2f} GB available")
            return True
            
    except Exception as e:
        logging.error(f"Error checking disk space for {path}: {e}")
        return False

# ---------------- NETWORK OPERATIONS with Retries ----------------

def make_request(method: str, url: str, headers: Dict,
                 max_retries: int, retry_delay: int, cid_context: str,
                 allow_redirects: bool = True, data: Optional[Dict] = None,
                 stream: bool = False, timeout: int = 30, proxies: Optional[Dict] = None) -> Optional[requests.Response]:
    """Makes an HTTP request with retry logic."""
    for attempt in range(max_retries):
        try:
            response = requests.request(
                method, url, headers=headers, data=data, timeout=timeout,
                allow_redirects=allow_redirects, stream=stream, proxies=proxies
            )
            response.raise_for_status()
            return response
        except Timeout as e:
            logging.warning(f"Attempt {attempt + 1}/{max_retries}: Timeout ({cid_context}): {url} - {e}")
        except ConnectionError as e:
            logging.warning(f"Attempt {attempt + 1}/{max_retries}: Connection error ({cid_context}): {url} - {e}")
        except HTTPError as e:
            logging.error(f"Attempt {attempt + 1}/{max_retries}: HTTP error ({cid_context}): {url} - {e.response.status_code} {e.response.reason}")
            if 400 <= e.response.status_code < 500 and e.response.status_code != 429:
                 return None
        except RequestException as e:
            logging.warning(f"Attempt {attempt + 1}/{max_retries}: Network error ({cid_context}): {url} - {e}")
        except Exception as e:
            logging.exception(f"Unexpected error during request ({cid_context}) on attempt {attempt + 1}: {url}")

        if attempt < max_retries - 1:
            logging.info(f"Waiting {retry_delay} seconds before retrying {cid_context} request...")
            time.sleep(retry_delay)
        else:
            logging.error(f"Failed request for {cid_context} after {max_retries} attempts: {url}")
            return None
    return None

# ---------------- CORE FUNCTIONALITY ----------------

def get_movie_count(cid: str, headers: Dict,
                    max_retries: int, retry_delay: int, proxies: Optional[Dict] = None) -> int:
    """Requests the JSON playlist and returns the number of parts. Returns -1 on failure."""
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
                logging.debug(f"Found {count} parts via API for {cid}")
                return count
            else:
                logging.warning(f"API response for {cid} seems empty or invalid: {data}")
                return -1
        except json.JSONDecodeError as e:
            logging.error(f"Failed to decode JSON response for movie count {cid}: {e}")
            return -1
        except Exception as e:
            logging.exception(f"Unexpected error processing API response for {cid}")
            return -1
    else:
        return -1

def get_online_cid(cid: str, headers: Dict,
                   max_retries: int, retry_delay: int, proxies: Optional[Dict] = None) -> Optional[str]:
    """Retrieves the online ID ('item_variant') by parsing the video page JavaScript."""
    url = f'https://www.dmm.co.jp/monthly/premium/-/detail/=/cid={cid}/'
    cid_context = f"Online CID page for {cid}"
    response = make_request("GET", url, headers, max_retries, retry_delay, cid_context, proxies=proxies)

    if response:
        try:
            # Extract item_variant from JavaScript using regex
            match = re.search(r'item_variant\s*:\s*"([^"]+)"', response.text)
            if match:
                item_variant = match.group(1)
                logging.debug(f"Found item_variant {item_variant} for {cid}")
                return item_variant
            else:
                logging.warning(f"item_variant not found in JavaScript for {cid}")
                return None
        except Exception as e:
            logging.exception(f"Error parsing online CID page for {cid}")
            return None
    else:
        return None


def download_dcv_file(download_url: str, output_dir: str, cid_part_label: str,
                      headers_main: Dict, headers_download: Dict,
                      max_retries: int, retry_delay: int, config: configparser.ConfigParser, proxies: Optional[Dict] = None) -> Optional[str]:
    """Gets the redirect location and downloads the .dcv file with a progress bar.
    
    Follows multiple redirects manually:
    1. dmm.co.jp/proxy/... -> str.dmm.com/... (with proxy)
    2. str.dmm.com/... -> stcXXX.dmm.com/... (with proxy)
    3. Download from stcXXX.dmm.com (no proxy, uses X-Forwarded-For)
    """
    logging.info(f"Requesting download location for {cid_part_label}...")
    logging.info(f"Download URL: {download_url}")
    
    # Follow redirects manually until we get the final download URL
    current_url = download_url
    max_redirects = 5
    
    for redirect_num in range(1, max_redirects + 1):
        cid_context_loc = f"Redirect #{redirect_num} for {cid_part_label}"
        response_loc = make_request("GET", current_url, headers_main, max_retries, retry_delay,
                                    cid_context_loc, allow_redirects=False, timeout=60, proxies=proxies)
        
        if response_loc and 300 <= response_loc.status_code < 400:
            location = response_loc.headers.get('Location')
            if location:
                logging.info(f"Redirect #{redirect_num} for {cid_part_label}: {location[:120]}...")
                current_url = location
            else:
                logging.error(f"Redirect #{redirect_num} for {cid_part_label}: no 'Location' header found.")
                return None
        elif response_loc and 200 <= response_loc.status_code < 300:
            logging.info(f"Got direct response (no redirect) at step #{redirect_num} for {cid_part_label}")
            break
        elif response_loc:
            logging.error(f"Unexpected status {response_loc.status_code} at redirect #{redirect_num} for {cid_part_label}.")
            return None
        else:
            logging.error(f"Failed to get response at redirect #{redirect_num} for {cid_part_label}.")
            return None
    
    # current_url is now the final download URL (stcXXX.dmm.com)
    final_url = current_url
    logging.info(f"Final download URL for {cid_part_label}: {final_url[:120]}...")

    try:
        filename = Path(final_url.split("?")[0]).name
        if not filename.endswith(".dcv"):
            logging.warning(f"Extracted filename '{filename}' for {cid_part_label} does not end with .dcv. Using generic name.")
            filename = f"{cid_part_label.replace(' ', '_').replace(':', '_')}.dcv"

        output_file_path = Path(output_dir) / filename
        logging.info(f"Starting download: {filename} -> {output_file_path}")

        # Download from final CDN URL (no proxy needed, uses X-Forwarded-For)
        cid_context_dl = f"File download for {cid_part_label}"
        r_dl = make_request("GET", final_url, headers_download, max_retries, retry_delay,
                            cid_context_dl, stream=True, timeout=300, proxies=None)
        if not r_dl:
            return None

        total_size_str = r_dl.headers.get('content-length')
        total_size = int(total_size_str) if total_size_str else None
        chunk_size = 8192

        try:
            with open(output_file_path, 'wb') as f_dl, \
                 tqdm(total=total_size, unit='B', unit_scale=True, unit_divisor=1024,
                      desc=filename, ascii=True, miniters=1) as progress_bar:
                for chunk in r_dl.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f_dl.write(chunk)
                        progress_bar.update(len(chunk))

            final_size = safe_getsize(str(output_file_path))
            if final_size is None:
                logging.error(f"Download completed for {cid_part_label}, but couldn't get final file size.")
                safe_remove(str(output_file_path), "incomplete download")
                return None
            elif total_size is not None and final_size < total_size:
                logging.error(f"Download incomplete for {cid_part_label}: Expected {total_size} bytes, got {final_size} bytes.")
                safe_remove(str(output_file_path), "incomplete download")
                return None
            elif final_size == 0:
                logging.error(f"Download for {cid_part_label} resulted in an empty file: {output_file_path}")
                safe_remove(str(output_file_path), "empty download")
                return None
            else:
                logging.info(f"Download completed successfully for {cid_part_label}: {output_file_path} ({final_size} bytes)")
                return str(output_file_path)

        except OSError as e:
            logging.exception(f"File system error during download for {cid_part_label}: {e}")
            safe_remove(str(output_file_path), "filesystem error")
            return None
        finally:
            r_dl.close()

    except Exception as e:
        logging.exception(f"Unexpected error during file download or handling for {cid_part_label}: {e}")
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
        logging.debug(f"Found {len(all_dcv_links)} <a> dcv links in HTML for {cid}")
        
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
            logging.info(f"Found download links in <a> tags for {cid}: {list(download_links.keys())}")
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
                            logging.debug(f"Constructed download URL for rate {rate}, part {part_num}: {download_url}")
            
            if download_links:
                break  # Found data, no need to check other scripts
        
        if download_links:
            logging.info(f"Extracted download links from JavaScript for {cid}: {list(download_links.keys())}")
            for rate, urls in download_links.items():
                logging.info(f"  Rate {rate}: {len(urls)} part(s)")
        else:
            logging.warning(f"No download links found in HTML for {cid}")
        
    except Exception as e:
        logging.error(f"Error extracting download links for {cid}: {e}")
    
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

def fetch_and_download_parts(cid: str, config: configparser.ConfigParser,
                             headers_main: Dict, headers_download: Dict, proxies: Optional[Dict] = None) -> Tuple[Optional[List[str]], Optional[int]]:
    """Fetches page details, determines parts, and downloads them."""
    paths = config['Paths']
    settings = config['Settings']
    download_dir = expand_user_path(paths['download_dir'])
    max_retries = settings.getint('max_retries', fallback=3)
    retry_delay = settings.getint('retry_delay_seconds', fallback=5)

    page_url = f'https://www.dmm.co.jp/monthly/premium/-/detail/=/cid={cid}/'
    logging.info(f"Processing ID {cid}: Fetching page details from {page_url}")
    cid_context_page = f"Main page for {cid}"
    response_page = make_request("GET", page_url, headers_main, max_retries, retry_delay, cid_context_page, proxies=proxies)

    if not response_page:
        logging.error(f"Failed to access main page for ID {cid}. Skipping.")
        return None, None

    try:
        soup = BeautifulSoup(response_page.text, 'lxml')
        
        # Extract download links directly from HTML
        download_links = extract_download_links(soup, cid)
        
        if download_links:
            # Found download links in HTML - use them directly
            logging.info(f"Found {len(download_links)} bitrate option(s) in HTML for ID {cid}")
            
            # Select best quality link
            best_link, selected_bitrate = select_best_download_link(download_links)
            if best_link and selected_bitrate:
                logging.info(f"Selected bitrate: {selected_bitrate} for ID {cid}")
                
                # Get all parts for selected bitrate
                part_links = download_links.get(selected_bitrate, [])
                parts_count = len(part_links)
                
                if parts_count == 0:
                    logging.error(f"No download links for selected bitrate {selected_bitrate} for ID {cid}")
                    return None, None
                
                downloaded_files_list = []
                for i, link in enumerate(part_links, 1):
                    part_suffix = f" Part {i}" if parts_count > 1 else ""
                    cid_part_label = f"{cid}{part_suffix}"
                    
                    downloaded_file_path = download_dcv_file(
                        link, download_dir, cid_part_label,
                        headers_main, headers_download,
                        max_retries, retry_delay, config, proxies
                    )
                    
                    if downloaded_file_path:
                        downloaded_files_list.append(downloaded_file_path)
                    else:
                        logging.error(f"Download failed for {cid_part_label}")
                        # Clean up partial downloads
                        for f in downloaded_files_list:
                            safe_remove(f, "partial download cleanup")
                        return None, None
                
                logging.info(f"Successfully downloaded all {parts_count} part(s) for ID {cid}.")
                return downloaded_files_list, parts_count
            else:
                logging.error(f"Could not select best download link for ID {cid}")
                return None, None
        else:
            logging.error(f"No download links found in HTML for ID {cid}")
            return None, None
            
    except Exception as e:
        logging.exception(f"Error parsing page for ID {cid}")
        return None, None


# ---------------- DECRYPTION with Retry ----------------

def decrypt_and_verify(cid: str, downloaded_files: List[str], expected_parts: int,
                       decrypt_tool_path: str, decrypt_output_dir: str,
                       config: configparser.ConfigParser) -> bool:
    """
    Decrypts .dcv files, verifies success, and performs advanced checks on size and duration.
    Includes retry logic for specific "No internet connection" errors from the tool.
    """
    if not downloaded_files:
        logging.error(f"No files provided for decryption for ID {cid}.")
        return False

    try:
        max_retries = config.getint('Settings', 'max_retries')
        decrypt_retry_delay = config.getint('Settings', 'decrypt_retry_delay_seconds')
    except (configparser.NoOptionError, ValueError) as e:
        logging.error(f"Configuration error reading decryption retry settings: {e}. Using defaults (Max Retries: 3, Delay: 10s)")
        max_retries = 3
        decrypt_retry_delay = 10 # Default fallback, though config should have 100

    is_multipart = expected_parts > 1
    overall_success = True
    successfully_decrypted_mkv_paths = []

    for dcv_file_path in downloaded_files:
        part_success = False
        base_name = Path(dcv_file_path).stem
        output_mkv_path = Path(decrypt_output_dir) / f"{base_name}.mkv"
        file_basename = Path(dcv_file_path).name

        if output_mkv_path.exists():
             mkv_size = safe_getsize(str(output_mkv_path))
             if mkv_size is not None and mkv_size > 1024: # Basic check for non-empty file
                 logging.warning(f"Decrypted file already exists: {output_mkv_path}. Re-decrypting to ensure validity.")
             safe_remove(str(output_mkv_path), "pre-decryption cleanup of existing file")


        cmd = [decrypt_tool_path, "decrypt", "-i", dcv_file_path, "-o", str(output_mkv_path), "-t", "dmm"]
        logging.info(f"Decrypting {file_basename} for ID {cid}...")
        logging.debug(f"Running command: {' '.join(cmd)}")

        # Decryption retry loop
        for attempt in range(max_retries):
            should_retry = False
            try:
                result = subprocess.run(cmd, capture_output=True, text=True, check=False, encoding='utf-8', errors='replace')
                logging.debug(f"Decryption attempt {attempt + 1}/{max_retries} stdout for {file_basename}:\n{result.stdout}")
                if result.stderr:
                    logging.debug(f"Decryption attempt {attempt + 1}/{max_retries} stderr for {file_basename}:\n{result.stderr}")

                if result.returncode != 0:
                    stderr_lower = result.stderr.lower() if result.stderr else ""
                    error_msg = f"Decryption command failed for {file_basename} (Attempt {attempt + 1}/{max_retries}, Return Code: {result.returncode})."

                    if DECRYPT_CONN_ERROR_MSG in stderr_lower:
                        error_msg += f" jav-it.exe reported '{DECRYPT_CONN_ERROR_MSG}'."
                        if attempt < max_retries - 1:
                            logging.warning(error_msg + f" Will retry after {decrypt_retry_delay} seconds.")
                            should_retry = True
                        else:
                            logging.error(error_msg + " Max retries reached.")
                            logging.error(f"Stderr:\n{result.stderr}")
                            overall_success = False
                    else:
                        logging.error(error_msg)
                        logging.error(f"Stderr:\n{result.stderr}")
                        overall_success = False

                    if not should_retry:
                        break
                else:
                    stdout_lower = result.stdout.lower() if result.stdout else ""
                    stderr_lower = result.stderr.lower() if result.stderr else ""
                    if "[ ok ] decryption complete!" in stderr_lower or \
                       "[ ok ] decryption complete!" in stdout_lower:
                        logging.info(f"Decryption command successful for {file_basename} on attempt {attempt + 1}.")
                        part_success = True
                        break
                    else:
                        logging.warning(f"Decryption command finished with code 0 for {file_basename} (Attempt {attempt + 1}), but success message not found. Treating as failure.")
                        logging.warning(f"Stderr:\n{result.stderr}")
                        overall_success = False
                        break

            except FileNotFoundError:
                logging.critical(f"Decryption executable '{decrypt_tool_path}' not found. Cannot decrypt.")
                return False
            except (OSError, subprocess.SubprocessError) as e:
                logging.exception(f"Error running decryption command for {file_basename} on attempt {attempt + 1}: {e}")
                overall_success = False
                break
            except Exception as e:
                logging.exception(f"Unexpected exception running decryption for {file_basename} on attempt {attempt + 1}: {e}")
                overall_success = False
                break

            if should_retry:
                time.sleep(decrypt_retry_delay)

        # Verification steps (only if decryption was successful)
        if part_success:
            if not output_mkv_path.exists():
                logging.error(f"Decryption reported success but output file not found: {output_mkv_path}")
                overall_success = False
                continue

            # Size verification with variable tolerance
            size_in = safe_getsize(dcv_file_path)
            size_out = safe_getsize(str(output_mkv_path))

            if size_in is None or size_out is None:
                logging.error(f"Could not get file sizes to verify {file_basename}. Deleting output.")
                overall_success = False
                safe_remove(str(output_mkv_path), "size verification failed")
                continue

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
                logging.debug(f"Verifying size for {file_basename}. Input: {size_in}, Output: {size_out}, Ratio: {size_ratio:.4f}, Required: >={required_ratio}")
                
                if size_ratio < required_ratio:
                    logging.error(f"Size mismatch for {file_basename}: Ratio {size_ratio:.4f} is less than the required {required_ratio} for its size.")
                    overall_success = False
                    safe_remove(str(output_mkv_path), "size mismatch")
                    continue
                else:
                    logging.info(f"Size check passed for {output_mkv_path.name} (Ratio: {size_ratio:.4f}, Required: {required_ratio})")
            
            # Duration verification using MediaInfo
            logging.debug(f"Comparing duration for {file_basename} and its MKV output.")
            dcv_duration = get_media_duration(dcv_file_path)
            mkv_duration = get_media_duration(str(output_mkv_path))

            if dcv_duration is None or mkv_duration is None:
                logging.error(f"Could not get MediaInfo duration for '{file_basename}' or its output. Verification failed.")
                overall_success = False
                safe_remove(str(output_mkv_path), "duration check failed")
                continue
            
            duration_diff = abs(dcv_duration - mkv_duration)
            if duration_diff > 30.0:
                logging.error(f"Duration mismatch for {file_basename}: DCV={dcv_duration:.2f}s, MKV={mkv_duration:.2f}s (Difference: {duration_diff:.2f}s > 6s)")
                overall_success = False
                safe_remove(str(output_mkv_path), "duration mismatch")
                continue
            else:
                logging.info(f"Duration check passed for {output_mkv_path.name} (Difference: {duration_diff:.2f}s)")
                
            successfully_decrypted_mkv_paths.append(str(output_mkv_path))
        else:
             logging.error(f"Decryption failed for {file_basename} after all attempts.")
             overall_success = False

    # Final validation for the whole ID
    if not overall_success:
        logging.error(f"One or more parts failed decryption or verification for ID {cid}.")
        if successfully_decrypted_mkv_paths:
             logging.warning(f"Cleaning up {len(successfully_decrypted_mkv_paths)} successfully decrypted file(s) due to overall failure for ID {cid}...")
             for mkv_path in successfully_decrypted_mkv_paths:
                 safe_remove(mkv_path, "overall failure cleanup")
        return False

    if is_multipart and len(successfully_decrypted_mkv_paths) != expected_parts:
        logging.error(f"Decrypted parts count mismatch for ID {cid}: Expected {expected_parts}, Found {len(successfully_decrypted_mkv_paths)} successful MKV files.")
        if successfully_decrypted_mkv_paths:
             logging.warning(f"Cleaning up {len(successfully_decrypted_mkv_paths)} file(s) due to part count mismatch...")
             for mkv_path in successfully_decrypted_mkv_paths:
                 safe_remove(mkv_path, "part count mismatch cleanup")
        return False

    logging.info(f"All {expected_parts} part(s) for ID {cid} decrypted and verified successfully.")
    return True

# ---------------- ORCHESTRATOR FUNCTION FOR EACH ID ----------------

def process_video_id(cid: str, config: configparser.ConfigParser,
                     headers_main: Dict, headers_download: Dict,
                     decrypt_tool_path: str, proxies: Optional[Dict] = None):
    """Processes a single video ID through all phases."""
    logging.info(f"=== Processing ID: {cid} ===")
    failure_occurred = False
    downloaded_files = []
    parts_count = -1

    paths = config['Paths']
    settings = config['Settings']
    max_retries = settings.getint('max_retries')
    retry_delay = settings.getint('retry_delay_seconds')
    failed_dir = expand_user_path(paths['failed_dir'])
    decrypt_output_dir = expand_user_path(paths['decrypt_output_dir'])
    download_dir = expand_user_path(paths['download_dir'])
    failed_ids_file = paths['failed_ids_file']
    ids_file = paths['ids_file']

    # Check disk space before starting download
    logging.info(f"Checking disk space for ID {cid}...")
    if not check_disk_space(download_dir, 30.0):
        logging.critical(f"Insufficient disk space in download directory: {download_dir}. Stopping script.")
        sys.exit(1)
    
    if not check_disk_space(decrypt_output_dir, 30.0):
        logging.critical(f"Insufficient disk space in decryption output directory: {decrypt_output_dir}. Stopping script.")
        sys.exit(1)

    try:
        # Phase 1: Download
        downloaded_files, parts_count = fetch_and_download_parts(cid, config, headers_main, headers_download, proxies)

        is_download_phase_ok = False
        if downloaded_files is None:
            logging.error(f"Critical error fetching details for ID {cid}. Cannot proceed.")
            failure_occurred = True
        elif not isinstance(parts_count, int) or parts_count < 0:
             logging.error(f"Invalid parts_count ({parts_count}) received for ID {cid}. Cannot proceed.")
             failure_occurred = True
        elif parts_count == 0:
            logging.info(f"ID {cid} has 0 parts. Marking as processed.")
            is_download_phase_ok = True
        elif not downloaded_files or len(downloaded_files) != parts_count:
            logging.error(f"Download phase failed or incomplete for ID {cid}. Expected {parts_count}, Got {len(downloaded_files)}.")
            failure_occurred = True
        else:
            logging.info(f"ID {cid}: Download phase complete. Got {len(downloaded_files)} files.")
            is_download_phase_ok = True

        # Phases 2 & 3: Validation and Decryption (only if download OK and parts > 0)
        if is_download_phase_ok and parts_count is not None and parts_count > 0:
            # Pre-decryption conformance check
            if downloaded_files is not None:
                logging.info(f"Performing MediaInfo conformance check on {len(downloaded_files)} downloaded .dcv file(s)...")
            else:
                logging.info("No downloaded files to perform MediaInfo conformance check on.")
            all_dcv_files_valid = True
            for dcv_file in downloaded_files or []:
                if has_conformance_errors(dcv_file):
                    logging.error(f"Conformance check failed for {Path(dcv_file).name}. File is likely corrupt or incomplete.")
                    all_dcv_files_valid = False
                    break

            if not all_dcv_files_valid:
                failure_occurred = True

            if all_dcv_files_valid:
                is_multipart = parts_count > 1
                verification_passed = True

                if is_multipart:
                    if downloaded_files and len(downloaded_files) == parts_count:
                        logging.info(f"Multipart verification successful for ID {cid}: {len(downloaded_files)}/{parts_count} parts downloaded.")
                    else:
                        downloaded_count = len(downloaded_files) if downloaded_files else 0
                        logging.error(f"Multipart verification failed for ID {cid}: Expected {parts_count} parts, got {downloaded_count}.")
                        failure_occurred = True
                        verification_passed = False

                if verification_passed:
                    logging.info(f"Starting decryption phase for ID {cid}")
                    decryption_success = decrypt_and_verify(cid, downloaded_files if downloaded_files is not None else [], parts_count, decrypt_tool_path, decrypt_output_dir, config)

                    if decryption_success:
                        logging.info(f"Decryption and all verifications successful for ID {cid}. Deleting source .dcv files.")
                        for file_path in downloaded_files if downloaded_files is not None else []:
                            safe_remove(file_path, "successful decryption cleanup")
                    else:
                        logging.error(f"Decryption or final verification failed for ID {cid}.")
                        failure_occurred = True

    except Exception as e:
        logging.exception(f"An unexpected error occurred while processing ID {cid}: {e}")
        failure_occurred = True

    # Final cleanup and ID removal
    if failure_occurred:
        write_failed_id(cid, failed_ids_file)
        if downloaded_files:
            move_files(downloaded_files, failed_dir, cid)
    
    remove_id_from_file(cid, ids_file)
    logging.info(f"=== Finished processing ID: {cid} ===")


# ---------------- MAIN EXECUTION ----------------

def main():
    """Main function to initialize, read IDs, and process them."""
    start_time = datetime.now()
    print(f"Script started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    if not check_dependencies():
        sys.exit(1)
    
    config = load_configuration(CONFIG_FILE)
    paths = config['Paths']
    network = config['Network']
    settings = config['Settings']

    setup_logging(paths['log_file_all'], paths['log_file_errors'])
    logging.info("Logging initialized.")
    logging.info(f"Using configuration from: {CONFIG_FILE}")
    logging.info(f"Running on {platform.system()} {platform.release()}")

    decrypt_executable = paths['decrypt_executable']
    script_dir = Path(__file__).parent.absolute()
    
    # Locate decryption tool
    local_decrypt_path = script_dir / decrypt_executable
    if local_decrypt_path.exists():
        decrypt_tool_path = str(local_decrypt_path)
        logging.info(f"Using decryption tool from script directory: {decrypt_tool_path}")
    else:
        decrypt_tool_path = shutil.which(decrypt_executable)
        if decrypt_tool_path:
            logging.info(f"Using decryption tool found in PATH: {decrypt_tool_path}")
        elif Path(decrypt_executable).is_absolute() and Path(decrypt_executable).is_file():
            decrypt_tool_path = decrypt_executable
            logging.info(f"Using decryption tool specified in config: {decrypt_tool_path}")
        else:
            logging.critical(f"Decryption executable '{decrypt_executable}' not found in script directory, PATH, or as a valid absolute file path. Exiting.")
            sys.exit(1)

    # Create required directories
    required_dirs = [
        expand_user_path(paths['download_dir']), 
        expand_user_path(paths['decrypt_output_dir']),
        expand_user_path(paths['failed_dir'])
    ]
    # Create required directories
    required_dirs = [
        expand_user_path(paths['download_dir']), 
        expand_user_path(paths['decrypt_output_dir']),
        expand_user_path(paths['failed_dir'])
    ]
    parent_dirs = {Path(p).parent for p in [paths['log_file_all'], paths['log_file_errors'], paths['failed_ids_file']] if Path(p).parent != Path()}
    for folder in required_dirs + [str(p) for p in parent_dirs]:
        try:
            Path(folder).mkdir(parents=True, exist_ok=True)
            logging.debug(f"Ensured directory exists: {folder}")
        except OSError as e:
            logging.critical(f"Could not create required directory {folder}: {e}. Exiting.")
            sys.exit(1)

    headers_main = {
        'User-Agent': network['user_agent'],
        'cookie': network['cookie']
    }
    # Add X-Forwarded-For header if configured
    x_forwarded_for = network.get('x_forwarded_for', '').strip()
    if x_forwarded_for:
        headers_main['X-Forwarded-For'] = x_forwarded_for
        logging.info(f"Using X-Forwarded-For: {x_forwarded_for}")
    headers_download = {
        'User-Agent': network['user_agent']
    }
    if x_forwarded_for:
        headers_download['X-Forwarded-For'] = x_forwarded_for
    
    # Set up proxy configuration
    proxies = {}
    proxy_url = network.get('proxy', '').strip()
    if proxy_url:
        proxies['http'] = proxy_url
        proxies['https'] = proxy_url
    if proxies:
        logging.info(f"Using proxy configuration: {proxies}")
    else:
        proxies = None

    ids_file_path = paths['ids_file']
    try:
        with open(ids_file_path, "r", encoding="utf8") as f:
            ids_to_process = [
                line.strip() for line in f
                if line.strip() and not line.strip().startswith('#')
            ]
        if not ids_to_process:
            logging.warning(f"'{ids_file_path}' is empty or contains only comments. No IDs to process.")
            sys.exit(0)
        logging.info(f"Found {len(ids_to_process)} IDs to process from {ids_file_path}.")
    except OSError as e:
        logging.critical(f"Error reading IDs file '{ids_file_path}': {e}. Exiting.")
        sys.exit(1)

    original_ids = list(ids_to_process)
    pause_between = settings.getint('pause_between_ids')

    for i, cid in enumerate(original_ids):
        try:
            logging.info(f"--- Starting ID {i+1}/{len(original_ids)}: {cid} ---")
            process_video_id(cid, config, headers_main, headers_download, decrypt_tool_path, proxies)

            if i < len(original_ids) - 1 and pause_between is not None and pause_between > 0:
                logging.debug(f"Pausing for {pause_between} seconds before next ID...")
                time.sleep(pause_between)
        except KeyboardInterrupt:
             logging.warning("Script interrupted by user during processing loop.")
             break
        except Exception as loop_err:
             logging.exception(f"Unexpected error processing ID {cid} in main loop: {loop_err}")
             write_failed_id(cid, paths['failed_ids_file'])
             remove_id_from_file(cid, ids_file_path)
             logging.error(f"Continuing to next ID after error processing {cid}.")

    end_time = datetime.now()
    logging.info(f"Script finished at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"Total execution time: {end_time - start_time}")


if __name__ == "__main__":
    main()
