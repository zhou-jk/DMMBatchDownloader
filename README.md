# DMM Batch Downloader & Processor Suite

A comprehensive Python toolkit for automatically downloading, decrypting, and processing videos from DMM. This suite includes multiple scripts for different workflows, from basic downloading to complete automated processing with cloud storage integration.

## Available Scripts

### 1. **downloader.py** - Basic Download & Decrypt
Single-threaded script that downloads and decrypts videos sequentially.

### 2. **batch_downloader.py** - Multi-threaded Downloader
Downloads `.dcv` files from DMM using multiple concurrent threads.

### 3. **batch_decryptor.py** - Multi-threaded Decryptor  
Decrypts `.dcv` files to `.mkv` format using multiple concurrent threads.

### 4. **rclone_decrypt_processor.py** - Remote Processing
Downloads `.dcv` files from rclone remote, decrypts locally, and uploads back.

### 5. **integrated_processor.py** - Complete Automated Pipeline ⭐
**RECOMMENDED**: All-in-one solution that combines downloading, decrypting, and uploading in a single multi-threaded workflow.

### 6. **run.py** - Interactive Script Launcher
User-friendly launcher that helps you choose the right script for your needs with descriptions and guidance.

## Features

- **Batch Processing**: Process multiple video IDs from a text file
- **Multi-threaded Processing**: Concurrent download, decryption, and upload operations
- **Multi-part Support**: Automatically handles videos split into multiple parts
- **Quality Selection**: Downloads videos in the highest available quality (including 4K)
- **Robust Error Handling**: Comprehensive retry logic and error recovery
- **File Verification**: Validates downloads using MediaInfo (size, duration, conformance checks)
- **Progress Tracking**: Real-time progress monitoring and statistics
- **Comprehensive Logging**: Detailed logs for debugging and monitoring
- **Disk Space Monitoring**: Prevents operations when insufficient space is available
- **Failed ID Tracking**: Automatically tracks and moves failed downloads
- **HTTP Proxy Support**: Works through proxy servers
- **Cloud Storage Integration**: Automatic upload to rclone remotes
- **Temporary File Management**: Efficient cleanup of intermediate files

## Prerequisites

### Required Software

1. **Python 3.7+**
2. **jav-it.exe** (Decryption tool)
   - Download from: [Developer's Patreon](https://github.com/AsahiMintia/scrapy/blob/master/Patreon.md)
   - Place in the script directory
3. **DECRYPT.MOD** (Decryption module)
   - Download from: [Developer's Patreon (search for module)](https://github.com/AsahiMintia/scrapy/blob/master/Patreon.md)
   - Place in the script directory
4. **MediaInfo CLI**
   - Download from: [MediaArea.net](https://mediaarea.net/en/MediaInfo/Download)
   - Must be added to your system PATH
5. **FFmpeg FULL Build**
   - Download from: [FFmpeg.org](https://ffmpeg.org/download.html)
   - Must be added to your system PATH

### Python Dependencies

Install required packages using pip:

```bash
pip install -r requirements.txt
```

Or manually:

```bash
pip install beautifulsoup4 lxml requests tqdm
```

## Installation

1. **Clone or Download** this repository
2. **Install Python dependencies** (see above)
3. **Download required tools** (jav-it.exe, DECRYPT.MOD, MediaInfo CLI, FFmpeg)
4. **Place tools in script directory** or ensure they're in your system PATH
5. **Run the script once** to generate a default `config.ini` file
6. **Configure your settings** (see Configuration section)

## Configuration

### First Run

Run the script once to generate a default configuration file:

```bash
python downloader.py
```

This will create `config.ini` with default settings that you need to customize.

### config.ini Settings

#### [Paths] Section
```ini
ids_file = ids.txt                    # File containing video IDs to download
download_dir = ./downloaded           # Directory for .dcv files
decrypt_output_dir = C:\DecryptedVideos  # Directory for final .mkv files
failed_dir = C:\FailedVideos         # Directory for failed downloads
decrypt_executable = jav-it.exe      # Decryption tool filename
log_file_all = process_all.log       # Main log file
log_file_errors = process_errors.log # Error log file
failed_ids_file = failed_ids.txt     # Failed IDs tracking file
```

#### [Network] Section
```ini
user_agent = Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html) #Don't touch this, it's required to bypass VPN requirement.
cookie = age_check_done=1; INT_SESID=YOUR-SESSION-ID; licenseUID=YOUR-LICENSE-ID  # **IMPORTANT: Replace with your actual DMM cookie values**
http_proxy =                         # HTTP proxy server (optional) - e.g., http://proxy.company.com:8080
https_proxy =                        # HTTPS proxy server (optional) - e.g., https://proxy.company.com:8080
```

#### [Settings] Section
```ini
max_retries = 3                      # Maximum retry attempts for network operations
retry_delay_seconds = 5              # Delay between retries
pause_between_ids = 2                # Pause between processing different IDs
decrypt_retry_delay_seconds = 100     # Delay before retrying decryption on connection errors
```

### Getting Your DMM Cookie

1. Log into DMM in your browser while having Developer Tools (F12) open (using a Japanese VPN)
2. Go to Network tab on the console
3. Find a request to dmm.co.jp
4. Search for the cookie and extract the value: INT_SESID
5. Paste it into the `config.ini` file under `[Network] INT_SESID=`
6. Navigate to a premium video page
7. Find a request to dmm.co.jp
8. Search for the cookie and extract the value: licenseUID
9. Paste it into the `config.ini` file under `[Network] licenseUID=`

## Usage

### Preparing Video IDs

Create an `ids.txt` file with one video ID per line:

```
1sdde00301
1sdde00302
1sdde00303
1sdde00304
```

### Running the Scripts

#### Interactive Script Launcher (Easiest)

Use the interactive launcher to choose the best script for your needs:

```bash
python run.py
```

This will show you all available scripts with descriptions and help you select the right one.

#### Direct Script Execution

##### Integrated Processor (Recommended)

For the complete automated workflow:

```bash
python integrated_processor.py
```

This will perform the complete pipeline:
1. **Check dependencies** and validate configuration
2. **Process each ID** with multi-threading
3. **Download** encrypted `.dcv` files to temporary directory
4. **Decrypt** to `.mkv` format with verification
5. **Upload** to rclone remote storage
6. **Clean up** temporary files automatically
7. **Track failed IDs** and provide detailed progress

##### Individual Scripts

For specific operations:

```bash
# Download only
python batch_downloader.py

# Decrypt only (processes existing .dcv files)
python batch_decryptor.py

# Basic single-threaded workflow
python downloader.py

# Remote processing via rclone
python rclone_decrypt_processor.py
```

### Output Structure

#### Integrated Processor Workflow

```
project/
├── ids.txt              # Input IDs to process
├── process_all.log      # Complete operation log
├── process_errors.log   # Error-only log
├── failed_ids.txt       # List of failed IDs
└── config.ini          # Configuration file

# Temporary files (auto-cleaned):
/tmp/integrated_<cid>_*/  # Temporary processing directories

# Remote storage (configured via rclone):
remote:/tmp/DecryptedVideos/  # Final .mkv files uploaded here
```

#### Individual Scripts Workflow

```
project/
├── downloaded/           # Temporary .dcv files (batch_downloader)
├── dcv_files/           # .dcv files for batch_decryptor input
├── DecryptedVideos/     # Final .mkv files (configured location)
├── FailedVideos/        # Failed downloads organized by ID
│   └── 1sdde00301/     # Failed files for specific ID
├── process_all.log      # Complete operation log
├── process_errors.log   # Error-only log
└── failed_ids.txt       # List of failed IDs
```

## Features in Detail

### Quality Selection
- Automatically detects and downloads the highest available quality
- Supports 4K, HD, and standard definitions
- Quality is determined by DMM's available options for each video

### Multi-part Handling
- Automatically detects videos split into multiple parts
- Downloads all parts and verifies completeness
- Ensures all parts are successfully decrypted before proceeding

### Error Recovery
- Network timeouts and connection errors are automatically retried
- Decryption failures due to connectivity issues are retried
- Corrupted downloads are detected and re-attempted
- Failed IDs are tracked and can be manually retried later

### File Verification
- **Size validation**: Compares input/output file sizes with intelligent thresholds
- **Duration verification**: Ensures decrypted video duration matches original
- **MediaInfo conformance**: Checks for file corruption and encoding errors
- **Automatic cleanup**: Removes invalid files and retries when possible

### Logging
- **Comprehensive logging**: All operations are logged with timestamps
- **Separate error log**: Critical issues are logged separately
- **Progress tracking**: Real-time feedback during downloads
- **Debug information**: Detailed troubleshooting information available

## Troubleshooting

### Common Issues

**"Dependencies not found"**
- Ensure jav-it.exe and DECRYPT.MOD are in the script directory
- Verify MediaInfo CLI is installed and in PATH
- Check that all Python dependencies are installed

**"Configuration error"**
- Verify your DMM cookie is current and complete
- Check that all directory paths exist and are writable
- Ensure numeric settings are valid integers

**"Network errors"**
- Update your DMM cookie (cookies expire)
- Check your internet connection
- Verify DMM website accessibility
- Try increasing retry delays in configuration

**"Proxy connection issues"**
- Verify proxy server address and port are correct
- Check if proxy requires authentication (username/password)
- Test proxy connectivity outside the script first
- Try different proxy servers if available

**"Decryption failures"**
- Ensure jav-it.exe has proper permissions
- Check that DECRYPT.MOD is the correct version
- Verify internet connectivity (decryption may require online verification)
- Be sure to have jav-it enviroment variables for DMM decryption
- Check available disk space

**"Rclone upload failures"** (integrated_processor.py only)
- Verify rclone is installed and accessible from PATH
- Test rclone configuration: `rclone config show`
- Check remote storage authentication and permissions
- Verify remote path exists and is writable
- Test connectivity: `rclone lsf remote:path`

**"Size/Duration mismatches"**
- Usually indicates incomplete downloads or decryption failures
- Check network stability
- Verify sufficient disk space
- Review logs for specific error details

**"Integrated processor performance issues"**
- Reduce concurrent threads in config if system is overwhelmed
- Monitor system resources (CPU, RAM, network bandwidth)
- Check temporary directory space (integrated processor uses /tmp)
- Consider using individual scripts for troubleshooting specific phases

### Advanced Troubleshooting

1. **Enable debug logging** by checking `process_all.log`
2. **Check system resources** (disk space, memory, CPU)
3. **Verify file permissions** on all directories
4. **Test individual components** (MediaInfo, FFmpeg, jav-it.exe)
5. **Update dependencies** if issues persist

## Script Comparison

| Feature | downloader.py | batch_downloader.py | batch_decryptor.py | integrated_processor.py |
|---------|---------------|--------------------|--------------------|------------------------|
| Multi-threading | ❌ | ✅ | ✅ | ✅ |
| Download | ✅ | ✅ | ❌ | ✅ |
| Decrypt | ✅ | ❌ | ✅ | ✅ |
| Upload to Remote | ❌ | ❌ | ❌ | ✅ |
| Temporary Files | Manual cleanup | Manual cleanup | Manual cleanup | Auto cleanup |
| Memory Usage | Low | Medium | Medium | Medium |
| Processing Speed | Slow | Fast | Fast | Fastest |
| Disk Usage | High | High | High | Low |
| Complete Workflow | ✅ | ❌ | ❌ | ✅ |

### Integrated Processor Advantages

1. **Memory Efficient**: Uses temporary directories that are automatically cleaned
2. **Fastest Overall**: Parallel processing of download, decrypt, and upload phases
3. **Space Saving**: No intermediate file storage on local disk
4. **Cloud Ready**: Direct upload to remote storage (Google Drive, OneDrive, etc.)
5. **Fault Tolerant**: Individual task failures don't affect other tasks
6. **Progress Monitoring**: Real-time statistics across all processing phases

## Changelog

### Current Version Features
- **NEW**: Integrated processor with complete automated pipeline
- **NEW**: Multi-threaded download, decryption, and upload workflow
- **NEW**: Rclone integration for cloud storage uploads
- **NEW**: Temporary file management with automatic cleanup
- Support for DMM Premium subscription
- Multi-part video support with automatic detection
- Advanced file verification using MediaInfo
- Intelligent retry logic for network and decryption failures
- Comprehensive error handling and logging
- Cross-platform compatibility (Windows/Linux/macOS)
- Disk space monitoring and prevention
- Failed ID tracking and organization
- Progress bars for download monitoring
- HTTP/HTTPS proxy support

## Support

For issues and questions:
1. Check the troubleshooting section above
2. Review the log files for specific error details
3. Ensure all dependencies are properly installed
4. Verify your configuration settings are correct
