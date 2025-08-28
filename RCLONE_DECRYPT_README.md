# Rclone DCV Decryption Processor

This is a brand new script for downloading DCV files from rclone remote, decrypting them locally, and uploading them back.

## Features

- 🔽 Download DCV files from rclone remote
- 🔓 Decrypt DCV files to MKV format locally
- ✅ Complete file verification (size, duration, conformance checks)
- 🔼 Upload decrypted MKV files back to remote
- 🧹 Automatic temporary file cleanup
- 📝 Detailed logging
- 🔄 Retry mechanism support

## System Requirements

### Required Dependencies
1. **Python 3.7+**
2. **jav-it decryption tool** - Download from [Patreon](https://www.patreon.com/c/AsahiMintia/posts)
3. **DECRYPT.MOD** - Download from [Patreon](https://www.patreon.com/posts/decryption-1-0-103706011)
4. **MediaInfo CLI** - Download from [official website](https://mediaarea.net/en/MediaInfo/Download)
5. **FFmpeg** - Download from [official website](https://ffmpeg.org/download.html)
6. **rclone** - Download from [official website](https://rclone.org/downloads/)

### Python Dependencies
```bash
pip install requests beautifulsoup4 tqdm
```

## Installation & Configuration

### 1. File Structure
```
Project Directory/
├── rclone_decrypt_processor.py  # New processor script
├── shared_utils.py              # Shared utility functions
├── config.ini                   # Configuration file
├── jav-it.exe (Windows) or jav-it (Linux/Mac)
└── DECRYPT.MOD
```

### 2. Create Configuration File
Create default configuration on first run:
```bash
python rclone_decrypt_processor.py --create-config
```

### 3. Configure rclone Remote
Make sure you have configured an rclone remote, e.g., named `remote`:
```bash
rclone config
```

### 4. Configuration File Example (config.ini)
```ini
[Paths]
ids_file = ids.txt
download_dir = ./downloaded
decrypt_output_dir = C:\Users\YourName\Videos\DecryptedVideos
failed_dir = C:\Users\YourName\Videos\FailedVideos
decrypt_executable = jav-it.exe
log_file_all = process_all.log
log_file_errors = process_errors.log
failed_ids_file = failed_ids.txt
dcv_files_dir = ./dcv_files

[Network]
user_agent = Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)
cookie = age_check_done=1; ; INT_SESID=YOUR-SESSION-ID; licenseUID=YOUR-LICENSE-ID
http_proxy = 
https_proxy = 

[Settings]
max_retries = 3
retry_delay_seconds = 5
decrypt_retry_delay_seconds = 100
pause_between_ids = 2
download_threads = 3
decrypt_threads = 2

[Rclone]
rclone_executable = rclone
remote_name = remote
remote_dcv_path = /root/jav-it
remote_mkv_path = /root/jav-it/decrypted
```

**Key Configuration Sections:**

- **[Rclone]**: Configure rclone settings
  - `rclone_executable`: Path to rclone executable (default: `rclone`)
  - `remote_name`: Name of your configured rclone remote
  - `remote_dcv_path`: Remote path where DCV files are stored
  - `remote_mkv_path`: Remote path where decrypted MKV files will be uploaded

- **[Paths]**: File and directory paths (shared with other scripts)
- **[Network]**: Network settings (shared with other scripts) 
- **[Settings]**: Processing settings including retry configuration

## Usage

### Basic Usage

#### 1. List Remote DCV Files
```bash
python rclone_decrypt_processor.py --list
```

#### 2. Process All DCV Files
```bash
python rclone_decrypt_processor.py
```

#### 3. Process Specific Files
```bash
python rclone_decrypt_processor.py --files file1.dcv file2.dcv
```

#### 4. Use Custom Remote Name
```bash
python rclone_decrypt_processor.py --remote myremote
```

### Command Line Options

| Option | Short | Description |
|--------|-------|-------------|
| `--remote REMOTE` | `-r` | Specify rclone remote name (default: remote) |
| `--files FILE [FILE ...]` | `-f` | Specify list of files to process |
| `--list` | `-l` | Only list remote DCV files, don't process |
| `--create-config` |  | Create default configuration file |
| `--help` | `-h` | Show help message |

## Workflow

1. **Scan Remote Files**: Scan all `.dcv` files in `/root/jav-it/` directory
2. **Download to Temporary Directory**: Download DCV files to local temporary directory
3. **Conformance Check**: Use MediaInfo to check file integrity
4. **Decryption Process**: Use jav-it tool to decrypt to MKV format
5. **File Verification**: 
   - Size comparison verification
   - Duration comparison verification
6. **Upload Results**: Upload MKV files to `/root/jav-it/decrypted/` directory
7. **Cleanup Temporary Files**: Automatically delete local temporary files

## Remote Directory Structure

```
/root/jav-it/
├── file1.dcv           # Source DCV files
├── file2.dcv
├── ...
└── decrypted/          # Decrypted MKV files storage
    ├── file1.mkv
    ├── file2.mkv
    └── ...
```

## Log Files

- `rclone_decrypt_all.log` - Detailed logs of all operations
- `rclone_decrypt_errors.log` - Error logs only
- `rclone_decrypt_failed.txt` - List of failed files

## Troubleshooting

### Common Issues

1. **"Decryption tool not found"**
   - Ensure `jav-it.exe` (Windows) or `jav-it` (Linux/Mac) is in the script directory
   - Or ensure the decryption tool is in system PATH

2. **"Rclone not found"**
   - Install rclone and ensure it's in system PATH
   - Verify: `rclone version`

3. **"MediaInfo CLI unavailable"**
   - Install MediaInfo CLI version
   - Ensure it's in system PATH
   - Verify: `mediainfo --version`

4. **Download/Upload Failed**
   - Check rclone remote configuration
   - Check network connection
   - Check remote path permissions

### Performance Optimization Tips

1. **Disk Space**: Ensure sufficient local temporary storage space
2. **Network Bandwidth**: Large file transfers require stable network connection
3. **Concurrent Processing**: Current version processes sequentially to avoid processing multiple large files simultaneously

## Security Notes

- Temporary files are automatically cleaned up, but periodically check system temp directory
- Log files may contain file path information, be mindful of privacy protection
- Ensure rclone remote configuration security

## Relationship with Original Scripts

This script is completely independent from the original downloader and decryptor:
- Reuses utility functions from `shared_utils.py`
- Does not modify any original code
- Can be used in parallel with original scripts

## License

Please follow the original project's license terms.