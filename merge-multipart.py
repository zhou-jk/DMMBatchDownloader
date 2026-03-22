import os
import sys
import json
import re
import shutil
import subprocess
import time
from pathlib import Path
from pymediainfo import MediaInfo
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import configparser

# --- Configuration & Paths ---
BASE_DIR = "/root/DecryptedVideos"
DIRS = {
    "broken": os.path.join(BASE_DIR, "broken"),
    "single": os.path.join(BASE_DIR, "single-videos"),
    "multipart": os.path.join(BASE_DIR, "multipart"),
    "missing_parts": os.path.join(BASE_DIR, "missing-parts"),
    "merged": os.path.join(BASE_DIR, "merged"),
    "mismatch": os.path.join(BASE_DIR, "mismatch-duration"),
    "vp9": os.path.join(BASE_DIR, "vp9"),
    "unverified": os.path.join(BASE_DIR, "unverified")
}

VIDEO_EXTENSIONS = ('.mp4', '.mkv', '.avi', '.mov', '.flv', '.wmv', '.m4v', '.mpeg', '.mpg', '.webm', '.ts', '.mxf')
ERROR_KEYWORDS = ['error', 'compliance', 'truncated', 'invalid', 'corrupt']

# --- Utility Functions ---
def setup_directories():
    for d in DIRS.values():
        os.makedirs(d, exist_ok=True)

def load_network_config(config_path="config.ini"):
    config = configparser.ConfigParser(interpolation=None)
    if not os.path.exists(config_path):
        print(f"Warning: {config_path} not found. DMM verification might fail without cookies.")
        return {}, {}
    config.read(config_path, encoding='utf-8')
    headers = {
        'User-Agent': config.get('Network', 'user_agent', fallback='Mozilla/5.0'),
        'cookie': config.get('Network', 'cookie', fallback='')
    }
    proxies = {}
    proxy_url = config.get('Network', 'proxy', fallback='').strip()
    if proxy_url:
        proxies = {'http': proxy_url, 'https': proxy_url}
    return headers, proxies

def log_to_file(filename, message):
    with open(filename, 'a', encoding='utf-8') as f:
        f.write(f"{message}\n")

# --- Step 1 & 2 & P.P.S: Deep Scan & Categorization ---
def scan_and_categorize(source_folder):
    print("\n--- Phase 1: Deep Scanning and Categorizing Videos ---")
    files_to_process = []
    for root, dirs, files in os.walk(source_folder):
        # Prevent recursing into our output directories if source is /data
        if any(root.startswith(d) for d in DIRS.values()):
            continue
        for f in files:
            if f.lower().endswith(VIDEO_EXTENSIONS):
                files_to_process.append(os.path.join(root, f))

    for file_path in tqdm(files_to_process, desc="Scanning Files"):
        filename = os.path.basename(file_path)
        try:
            media_info = MediaInfo.parse(file_path)
        except Exception as e:
            print(f"\n[ERROR] Could not parse {filename}: {e}")
            shutil.move(file_path, os.path.join(DIRS["broken"], filename))
            continue

        is_broken = False
        is_vp9 = False
        comment_data = None

        for track in media_info.tracks:
            track_data = track.to_data()
            
            # Check for VP9
            if track.track_type == "Video":
                format_info = track_data.get("format", "").upper()
                if "VP9" in format_info:
                    is_vp9 = True

# Check for Errors
            for key, value in track_data.items():
                if not value: continue
                key_clean = str(key).lower()
                
                # Ignore standard MKV container properties that contain the word "error"
                if "errordetectiontype" in key_clean:
                    continue
                    
                if any(keyword in key_clean for keyword in ERROR_KEYWORDS):
                    if value == "Yes" or value is True or (isinstance(value, int) and value > 0) or (isinstance(value, str) and len(value) > 3):
                        is_broken = True

            # Extract Comment
            if track.track_type == "General":
                comment_str = track_data.get("comment")
                if comment_str:
                    try:
                        comment_data = json.loads(comment_str)
                    except json.JSONDecodeError:
                        pass # Ignore invalid JSON comments

        # Route the file
        if is_broken:
            shutil.move(file_path, os.path.join(DIRS["broken"], filename))
        elif comment_data and "file_id" in comment_data:
            file_id = comment_data["file_id"]
            if "part" in comment_data: # Multipart
                if is_vp9:
                    dest_dir = os.path.join(DIRS["vp9"], file_id)
                else:
                    dest_dir = os.path.join(DIRS["multipart"], file_id)
                os.makedirs(dest_dir, exist_ok=True)
                shutil.move(file_path, os.path.join(dest_dir, filename))
            else: # Single
                shutil.move(file_path, os.path.join(DIRS["single"], filename))
        else:
            print(f"\n[WARNING] {filename} has no valid JSON comment. Skipping.")

# --- Step 3: DMM Verification ---
def extract_dmm_parts_count(soup, cid):
    """Reused logic from downloader to extract JS/HTML volume count."""
    scripts = soup.find_all('script')
    for script in scripts:
        script_text = script.string if script.string else ''
        if not script_text: continue
        
        block_pattern = r'(\d+)\s*:\s*\{([^}]+)\}'
        blocks = re.findall(block_pattern, script_text, re.DOTALL)
        
        best_volume = 1
        for block_id, block_content in blocks:
            pid_match = re.search(r'product(?:\\u005f|_)id\s*:\s*[\'"]([^\'"]+)[\'"]', block_content)
            volume_match = re.search(r'volume\s*:\s*[\'"]?(\d+)[\'"]?', block_content)
            
            if pid_match and volume_match:
                vol = int(volume_match.group(1))
                if vol > best_volume:
                    best_volume = vol
        if best_volume > 1:
            return best_volume
    return 1 # Fallback

def verify_multipart_folders(headers, proxies):
    print("\n--- Phase 2: Verifying DMM Multi-parts ---")
    folders = [f for f in os.listdir(DIRS["multipart"]) if os.path.isdir(os.path.join(DIRS["multipart"], f))]
    
    for file_id in tqdm(folders, desc="Checking DMM"):
        folder_path = os.path.join(DIRS["multipart"], file_id)
        local_files = [f for f in os.listdir(folder_path) if f.lower().endswith(VIDEO_EXTENSIONS)]
        local_count = len(local_files)

        search_url = f"https://www.dmm.co.jp/monthly/premium/-/list/search/=/?searchstr={file_id}"
        try:
            res = requests.get(search_url, headers=headers, proxies=proxies, timeout=15)
            # If redirected directly to detail page
            if "/detail/=/cid=" in res.url:
                cid_match = re.search(r'cid=([^/]+)', res.url)
                if not cid_match: continue
                cid = cid_match.group(1)
                soup = BeautifulSoup(res.text, 'lxml')
            else:
                soup = BeautifulSoup(res.text, 'lxml')
                links = soup.select('.package-content__package a.package-content__link')
                if len(links) > 1:
                    log_to_file("multiple-results.txt", f"{file_id} matched {len(links)} results. Skipped.")
                    shutil.move(folder_path, os.path.join(DIRS["unverified"], file_id))
                    continue
                elif len(links) == 0:
                    log_to_file("multiple-results.txt", f"{file_id} matched 0 results. Skipped.")
                    shutil.move(folder_path, os.path.join(DIRS["unverified"], file_id))
                    continue
                
                href = links[0].get('href', '')
                cid_match = re.search(r'cid=([^/]+)', href)
                if not cid_match: continue
                cid = cid_match.group(1)
                
                # Fetch Detail Page
                detail_url = f"https://www.dmm.co.jp/monthly/premium/-/detail/=/cid={cid}/"
                res_detail = requests.get(detail_url, headers=headers, proxies=proxies, timeout=15)
                soup = BeautifulSoup(res_detail.text, 'lxml')

            expected_parts = extract_dmm_parts_count(soup, cid)

            if local_count != expected_parts:
                print(f"\n[MISSING PARTS] {file_id}: Expected {expected_parts}, Found {local_count}. Moving folder.")
                shutil.move(folder_path, os.path.join(DIRS["missing_parts"], file_id))

        except Exception as e:
            print(f"\n[NETWORK ERROR] Could not verify {file_id}: {e}")
            time.sleep(2)

# --- Step 4 & 5: Merging & Duration Verification ---
def get_part_number(filename):
    """Safely extract part number from MediaInfo comment to ensure 100% correct sorting."""
    try:
        media_info = MediaInfo.parse(filename)
        for track in media_info.tracks:
            if track.track_type == "General" and track.comment:
                data = json.loads(track.comment)
                if "part" in data:
                    return int(data["part"])
    except:
        pass
    # Fallback: Extract from filename (e.g., mde00001_Part2.mkv)
    match = re.search(r'(?:part|pt)0*(\d+)', filename, re.IGNORECASE)
    return int(match.group(1)) if match else 999

def merge_and_verify():
    print("\n--- Phase 3: Merging & Post-Verification ---")
    folders = [f for f in os.listdir(DIRS["multipart"]) if os.path.isdir(os.path.join(DIRS["multipart"], f))]
    
    for file_id in tqdm(folders, desc="Merging"):
        folder_path = os.path.join(DIRS["multipart"], file_id)
        files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.lower().endswith(VIDEO_EXTENSIONS)]
        if not files:
            continue

        # Sort files based on actual part number
        files.sort(key=get_part_number)

        concat_list_path = os.path.join(folder_path, "concat.txt")
        metadata_path = os.path.join(folder_path, "metadata.txt")
        output_file = os.path.join(DIRS["merged"], f"{file_id}.mkv")

        # Create FFMETADATA and Concat files
        with open(concat_list_path, 'w', encoding='utf-8') as cl:
            for f in files:
                cl.write(f"file '{f}'\n")

        metadata_content = ";FFMETADATA1\n"
        metadata_content += f"comment={{\"file_id\":\"{file_id}\"}}\n\n"

        total_parts_duration = 0.0
        current_time_ms = 0

        for idx, f in enumerate(files):
            try:
                mi = MediaInfo.parse(f)
                dur_ms = mi.tracks[0].duration # Duration in milliseconds
                if not dur_ms: dur_ms = 0
            except:
                dur_ms = 0

            start_time = current_time_ms
            end_time = current_time_ms + dur_ms
            
            metadata_content += "[CHAPTER]\nTIMEBASE=1/1000\n"
            metadata_content += f"START={int(start_time)}\nEND={int(end_time)}\n"
            metadata_content += f"title=Part {idx + 1}\n\n"
            
            current_time_ms = end_time
            total_parts_duration += (dur_ms / 1000.0)

        with open(metadata_path, 'w', encoding='utf-8') as md:
            md.write(metadata_content)

        # Execute FFmpeg
        cmd = [
            "ffmpeg", "-y", "-f", "concat", "-safe", "0", 
            "-i", concat_list_path, "-i", metadata_path, 
            "-map_metadata", "1", "-map", "0", "-c", "copy", output_file
        ]
        
        try:
            subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
            
            # Post-Merge Verification
            mi_out = MediaInfo.parse(output_file)
            out_duration = float(mi_out.tracks[0].duration) / 1000.0 if mi_out.tracks[0].duration else 0.0
            
            # 1% Tolerance Check
            if abs(out_duration - total_parts_duration) > (total_parts_duration * 0.01):
                print(f"\n[DURATION MISMATCH] {file_id}: Parts sum = {total_parts_duration}s, Merged = {out_duration}s")
                log_to_file("duration-mismatch.txt", f"{file_id}: Parts={total_parts_duration}s, Merged={out_duration}s")
                
                os.remove(output_file) # Delete bad merge
                shutil.move(folder_path, os.path.join(DIRS["mismatch"], file_id))
            else:
                # Success! Delete original parts
                shutil.rmtree(folder_path)

        except subprocess.CalledProcessError as e:
            print(f"\n[FFMPEG ERROR] Failed to merge {file_id}: {e}")
        except Exception as e:
            print(f"\n[SYSTEM ERROR] Error during post-verification for {file_id}: {e}")


if __name__ == "__main__":
    target_folder = input("Please insert the path of the folder containing the downloaded videos: ").strip()
    target_folder = target_folder.replace('"', '').replace("'", "")
    
    setup_directories()
    headers, proxies = load_network_config()
    
    scan_and_categorize(target_folder)
    verify_multipart_folders(headers, proxies)
    merge_and_verify()
    
    print("\n🎉 Pipeline Complete! Check /data/ folders for results.")