# DMM Batch Downloader

A Python script for automatically downloading and decrypting videos from DMM. This tool handles the complete workflow from downloading encrypted `.dcv` files to producing decrypted `.mkv` files ready for playback.

## Features

- **Batch Processing**: Process multiple video IDs from a text file
- **Multi-part Support**: Automatically handles videos split into multiple parts
- **Quality Selection**: Downloads videos in the highest available quality (including 4K)
- **Robust Error Handling**: Comprehensive retry logic and error recovery
- **File Verification**: Validates downloads using MediaInfo (size, duration, conformance checks)
- **Progress Tracking**: Real-time download progress bars
- **Comprehensive Logging**: Detailed logs for debugging and monitoring
- **Disk Space Monitoring**: Prevents downloads when insufficient space is available
- **Failed ID Tracking**: Automatically tracks and moves failed downloads

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

### Running the Script

```bash
python downloader.py
```

The script will:
1. **Check dependencies** and validate configuration
2. **Process each ID** in `ids.txt` sequentially
3. **Download** encrypted `.dcv` files
4. **Decrypt** to `.mkv` format with verification
5. **Clean up** temporary files on success
6. **Move failed files** to the failed directory
7. **Log all operations** for monitoring

### Output Structure

```
project/
├── downloaded/           # Temporary .dcv files (auto-cleaned on success)
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

**"Decryption failures"**
- Ensure jav-it.exe has proper permissions
- Check that DECRYPT.MOD is the correct version
- Verify internet connectivity (decryption may require online verification)
- Be sure to have jav-it enviroment variables for DMM decryption
- Check available disk space

**"Size/Duration mismatches"**
- Usually indicates incomplete downloads or decryption failures
- Check network stability
- Verify sufficient disk space
- Review logs for specific error details

### Advanced Troubleshooting

1. **Enable debug logging** by checking `process_all.log`
2. **Check system resources** (disk space, memory, CPU)
3. **Verify file permissions** on all directories
4. **Test individual components** (MediaInfo, FFmpeg, jav-it.exe)
5. **Update dependencies** if issues persist

## Changelog

### Current Version Features
- Support for DMM Premium subscription
- Multi-part video support with automatic detection
- Advanced file verification using MediaInfo
- Intelligent retry logic for network and decryption failures
- Comprehensive error handling and logging
- Cross-platform compatibility (Windows/Linux/macOS)
- Disk space monitoring and prevention
- Failed ID tracking and organization
- Progress bars for download monitoring

## Support

For issues and questions:
1. Check the troubleshooting section above
2. Review the log files for specific error details
3. Ensure all dependencies are properly installed
4. Verify your configuration settings are correct
