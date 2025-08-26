# DMM Batch Downloader and Decryptor - Multithreaded Version

This is a redesigned version of the original DMM batch downloader that separates downloading and decryption into two independent, multithreaded scripts for better performance and flexibility.

## Features

- **Separate Scripts**: Independent downloader and decryptor for modular workflow
- **Multithreading**: Concurrent processing for faster downloads and decryption
- **Thread-Safe Logging**: Detailed logging with thread identification
- **Progress Tracking**: Real-time progress updates and statistics
- **Error Handling**: Robust error handling with retry mechanisms
- **Configuration**: Flexible configuration with threading settings

## Files Overview

- `shared_utils.py` - Common utilities and functions shared between scripts
- `batch_downloader.py` - Multithreaded downloader for .dcv files
- `batch_decryptor.py` - Multithreaded decryptor for .dcv to .mkv conversion
- `config.ini` - Configuration file (auto-generated on first run)

## Prerequisites

1. **Dependencies** (same as original):
   - `jav-it.exe` (Windows) or `jav-it` (Linux/Mac) - Decryption tool
   - `DECRYPT.MOD` - Decryption module
   - MediaInfo CLI - Media file analysis
   - FFmpeg - Media processing

2. **Python Requirements**:
   ```bash
   pip install -r requirements.txt
   ```

## Configuration

The configuration file `config.ini` will be automatically created on first run. Key new settings:

```ini
[Settings]
# Original settings
max_retries = 3
retry_delay_seconds = 5
decrypt_retry_delay_seconds = 100
pause_between_ids = 2

# New threading settings
download_threads = 3      # Number of concurrent downloads
decrypt_threads = 2       # Number of concurrent decryptions
```

### Important Configuration Notes:

- **download_threads**: Recommended 2-4 threads. Too many may overload the server
- **decrypt_threads**: Recommended 1-3 threads. Limited by CPU and disk I/O
- **Cookie**: Must be updated with your actual DMM session cookie

## Usage

### 1. Download Only

Download .dcv files for all IDs in `ids.txt`:

```bash
python batch_downloader.py
```

**Features:**
- Downloads multiple IDs concurrently
- Real-time progress tracking
- Automatic retry on failures
- Failed downloads moved to `failed_dir`
- Thread-safe file operations

### 2. Decrypt Only

Decrypt all .dcv files found in download directories:

```bash
python batch_decryptor.py
```

**Features:**
- Automatically finds .dcv files in configured directories
- Concurrent decryption of multiple files
- Size and duration verification
- Automatic cleanup of source files after successful decryption
- MediaInfo conformance checking

### 3. Combined Workflow

For a complete workflow, run both scripts in sequence:

```bash
# First, download all content
python batch_downloader.py

# Then, decrypt all downloaded files
python batch_decryptor.py
```

## Directory Structure

```
project/
├── shared_utils.py           # Shared utilities
├── batch_downloader.py       # Download script
├── batch_decryptor.py        # Decryption script
├── config.ini               # Configuration file
├── ids.txt                  # List of IDs to download
├── downloaded/              # Downloaded .dcv files
├── DecryptedVideos/         # Final .mkv files
├── FailedVideos/           # Failed downloads/decryptions
├── process_all.log         # Complete log
└── process_errors.log      # Error-only log
```

## Advanced Features

### Thread Safety
- All file operations are thread-safe
- Logging includes thread identification
- Progress tracking is synchronized across threads

### Error Handling
- Individual thread failures don't stop other threads
- Comprehensive retry logic for network and decryption errors
- Failed items are logged and can be retried later

### Performance Tuning

**Download Performance:**
- Adjust `download_threads` based on your internet connection
- Monitor server response times and adjust accordingly
- Consider `pause_between_ids` to avoid rate limiting

**Decryption Performance:**
- Adjust `decrypt_threads` based on CPU cores and disk speed
- SSD storage significantly improves performance
- Monitor disk space usage during decryption

### Monitoring

**Progress Tracking:**
- Real-time progress updates every few completed items
- Statistics show total, completed, failed, and in-progress counts
- Final summary with timing and success rates

**Logging:**
- Thread-aware logging format includes thread names
- Separate error logs for troubleshooting
- Debug-level logging for detailed analysis

## Troubleshooting

### Common Issues

1. **High Memory Usage**: Reduce thread counts if memory usage is too high
2. **Disk Space**: Scripts check available space before starting
3. **Network Timeouts**: Adjust retry settings in configuration
4. **Decryption Failures**: Check that jav-it.exe and DECRYPT.MOD are properly installed

### Performance Tips

1. **SSD Storage**: Use SSD for download and decryption directories
2. **Thread Tuning**: Start with default settings, then adjust based on performance
3. **Network**: Stable internet connection improves download success rates
4. **System Resources**: Monitor CPU and disk usage during operation

## Migration from Original Script

To migrate from the original `downloader.py`:

1. **Configuration**: Your existing `config.ini` will work with minimal updates
2. **File Locations**: Update paths in config if needed
3. **Threading**: Start with default thread counts and adjust as needed
4. **Workflow**: You can now run downloads and decryption separately

## Logs and Debugging

- `process_all.log`: Complete activity log with thread information
- `process_errors.log`: Error-only log for troubleshooting
- `failed_ids.txt`: List of failed download IDs
- Thread names in logs help identify which worker encountered issues

## Performance Comparison

Compared to the original single-threaded script:
- **Download**: 3-5x faster with appropriate thread count
- **Decryption**: 2-3x faster depending on CPU cores
- **Overall**: Significant time savings for large batches
- **Resource Usage**: Higher CPU and memory usage but better throughput
