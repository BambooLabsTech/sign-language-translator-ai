import pandas as pd
import subprocess
import os
import sys
import csv
import logging
import random
import requests
import traceback
from pathlib import Path
from datetime import datetime
import yt_dlp # Use the Python API

# --- Configuration ---
CSV_FILE_PATH = Path("/home/pandu/Documents/explore/sign-language-translator-ai/combined_asl.csv")
OUTPUT_VIDEO_DIR = Path("/home/pandu/Documents/explore/sign-language-translator-ai/videos")
LOG_FILE_PATH = Path("/home/pandu/Documents/explore/sign-language-translator-ai/download_log.csv")
SCRIPT_DIR = Path(__file__).parent # Or set explicitly if needed

# Processing mode: 'test' (random 1000 rows) or 'full' (all rows)
PROCESSING_MODE = 'test'
NUM_TEST_ROWS = 200

# Add HEADERS for requests fallback
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

# FFmpeg configuration (using accurate cutting)
FFMPEG_PATH = "ffmpeg" # Assumes ffmpeg is in PATH
FFPROBE_PATH = "ffprobe" # Assumes ffprobe is in PATH
FFMPEG_CUT_ARGS = [
    "-c:v", "libx264", # Video codec
    "-preset", "fast", # Speed/compression trade-off
    "-crf", "23",      # Quality (lower means better/larger, 23 is often good)
    "-c:a", "aac",     # Audio codec
    "-b:a", "128k",    # Audio bitrate
]

# yt-dlp options for consistent MP4 output
YDL_OPTS_TEMPLATE = {
    'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/mp4/best', # Prioritize mp4
    'outtmpl': None, # Will be set dynamically
    'quiet': True,   # Suppress console output from yt-dlp itself
    'no_warnings': True,
    'noprogress': True,
    'noplaylist': True, # Don't download playlists if URL points to one
    # 'verbose': True, # Uncomment for detailed yt-dlp debugging
}
# --- End Configuration ---

# --- Logging Setup ---
def setup_logging():
    """Configures logging to console and file."""
    log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG) # Capture all levels

    # Console handler (INFO level)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(log_formatter)
    root_logger.addHandler(console_handler)

    # File handler (DEBUG level)
    try:
        file_handler = logging.FileHandler(SCRIPT_DIR / "processing_debug.log", mode='a')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(log_formatter)
        root_logger.addHandler(file_handler)
    except Exception as e:
        print(f"Warning: Could not set up file logging: {e}")

    logging.info("Logging initialized.")

# --- Helper Functions ---
def normalize_url(url):
    """Adds https:// if scheme is missing."""
    url = str(url).strip()
    if not url:
        return None
    if url.startswith("www."):
        return f"https://{url}"
    if not url.startswith(("http://", "https://")):
        # Basic check, might need refinement for other protocols if they exist
        if "youtube.com" in url or "youtu.be" in url:
             return f"https://{url}"
        else:
            # Cannot determine protocol for non-youtube links without scheme
            logging.warning(f"URL '{url}' lacks scheme (http/https) and is not recognized as YouTube. Skipping protocol addition.")
            return url # Return as is, yt-dlp might handle it or fail
    return url

def log_status(log_filepath, row_data, status, error_message=""):
    """Appends a status entry to the CSV log file."""
    file_exists = log_filepath.exists()
    try:
        with open(log_filepath, 'a', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['id', 'url', 'original_filename', 'output_path', 'status', 'error_message', 'timestamp']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            if not file_exists or os.path.getsize(log_filepath) == 0:
                writer.writeheader() # Write header only if file is new/empty

            log_entry = {
                'id': row_data.get('id', 'N/A'),
                'url': row_data.get('url', 'N/A'),
                'original_filename': row_data.get('filename', 'N/A'),
                'output_path': row_data.get('expected_output_path', 'N/A'),
                'status': status,
                'error_message': str(error_message)[:500], # Limit error message length
                'timestamp': datetime.now().isoformat()
            }
            writer.writerow(log_entry)
    except IOError as e:
        logging.error(f"Failed to write to log file {log_filepath}: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred during logging: {e}")

def download_video(url, output_template):
    """
    Downloads video using yt-dlp Python API.
    If yt-dlp fails for a direct .mp4 URL (non-YouTube),
    it attempts a fallback using the requests library.

    Args:
        url (str): The URL of the video to download.
        output_template (Path): The base path template for the download
                                (yt-dlp adds extension, requests will use .mp4).

    Returns:
        Path: The Path object to the successfully downloaded file, or None if download failed.
    """
    logging.debug(f"Attempting download for {url} using template {output_template}")

    # --- Attempt 1: yt-dlp ---
    ydl_opts = YDL_OPTS_TEMPLATE.copy()
    # yt-dlp expects a string path template, potentially without suffix
    ydl_opts['outtmpl'] = str(output_template)

    downloaded_file_path = None # Variable to store the final path

    try:
        logging.info(f"[yt-dlp] Attempting download: {url}")
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])

        # yt-dlp succeeded, now find the actual downloaded file
        # (It might have added an extension like .mp4, .mkv, etc.)
        base_path = Path(output_template) # Work with Path object
        possible_files = list(base_path.parent.glob(f"{base_path.name}.*"))

        if not possible_files:
             logging.error(f"[yt-dlp] SUCCESS reported but couldn't find downloaded file for template: {output_template}")
             # Fall through to fallback check, maybe it created nothing
        elif len(possible_files) == 1:
             downloaded_file_path = possible_files[0]
             logging.info(f"[yt-dlp] SUCCESS. Found downloaded file: {downloaded_file_path}")
        else:
             # Multiple files match (e.g., video.mp4, video.description) - prioritize mp4
             mp4_files = [f for f in possible_files if f.suffix.lower() == '.mp4']
             if len(mp4_files) == 1:
                 downloaded_file_path = mp4_files[0]
                 logging.info(f"[yt-dlp] SUCCESS. Multiple files found, using MP4: {downloaded_file_path}")
             else:
                 # Still ambiguous or no MP4, just pick the first one found
                 downloaded_file_path = possible_files[0]
                 logging.warning(f"[yt-dlp] SUCCESS. Multiple files found matching template {output_template}, using first match: {downloaded_file_path}")

        # If we found a file via yt-dlp, return it
        if downloaded_file_path and downloaded_file_path.exists() and downloaded_file_path.stat().st_size > 0:
            return downloaded_file_path
        else:
            # If file wasn't found or is empty despite ydl success report, log it and fall through
            logging.warning(f"[yt-dlp] Reported success but file '{downloaded_file_path}' is invalid/missing. Will check fallback.")
            downloaded_file_path = None # Reset path

    except yt_dlp.utils.DownloadError as e:
        logging.warning(f"[yt-dlp] FAILED for {url}. Error: {e}")
        # Proceed to fallback check below
    except Exception as e:
        logging.error(f"[yt-dlp] UNEXPECTED FAILURE for {url}. Error: {e}")
        logging.debug(traceback.format_exc())
        # Proceed to fallback check below

    # --- Attempt 2: Requests Fallback (if applicable) ---
    is_direct_mp4 = url.endswith('.mp4')
    is_youtube = "youtube.com" in url or "youtu.be" in url

    if is_direct_mp4 and not is_youtube:
        logging.info(f"[Requests Fallback] yt-dlp failed/skipped, attempting requests for: {url}")

        # Determine the exact output path for requests, assuming .mp4
        requests_output_path = output_template.with_suffix('.mp4')
        logging.debug(f"[Requests Fallback] Target path: {requests_output_path}")

        try:
            response = requests.get(url, headers=HEADERS, stream=True, timeout=200) # Adjust timeout as needed
            response.raise_for_status()  # Check for HTTP errors (4xx or 5xx)

            # Ensure parent directory exists before writing
            requests_output_path.parent.mkdir(parents=True, exist_ok=True)

            with open(requests_output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192): # 8KB chunks
                    f.write(chunk)

            # Verify download success
            if requests_output_path.exists() and requests_output_path.stat().st_size > 0:
                logging.info(f"[Requests Fallback] SUCCESS. Downloaded to: {requests_output_path}")
                return requests_output_path # Success! Return the path.
            else:
                 # File doesn't exist or is empty after download attempt
                 raise IOError(f"File not found or empty after requests download: {requests_output_path}")

        except requests.exceptions.RequestException as req_err:
            logging.error(f"[Requests Fallback] FAILED for {url}. Error: {req_err}")
        except IOError as io_err:
            logging.error(f"[Requests Fallback] FAILED (I/O Error) for {url}. Error: {io_err}")
        except Exception as gen_err:
            logging.error(f"[Requests Fallback] UNEXPECTED FAILURE for {url}. Error: {gen_err}")
            logging.debug(traceback.format_exc())

        # If requests fallback failed, try to clean up any partial file
        if requests_output_path and requests_output_path.exists():
            try:
                logging.debug(f"[Requests Fallback] Cleaning up partial download: {requests_output_path}")
                os.remove(requests_output_path)
            except OSError as e:
                logging.warning(f"[Requests Fallback] Could not remove partial file {requests_output_path}: {e}")

        # If we reached here, requests fallback failed. Return None.
        return None

    else:
        # yt-dlp failed, and it was not a candidate for requests fallback.
        if not downloaded_file_path: # Only log if yt-dlp actually failed (not just missing file after success)
             logging.error(f"Download FAILED for {url}. yt-dlp failed and not eligible for requests fallback.")
        return None # Indicate overall failure

def cut_video(input_path, output_path, start_sec, end_sec):
    """Cuts video using ffmpeg with accurate re-encoding."""
    logging.debug(f"Cutting {input_path} from {start_sec:.3f}s to {end_sec:.3f}s -> {output_path}")

    # Base command
    cmd = [
        FFMPEG_PATH,
        "-y", # Overwrite output without asking
        "-i", str(input_path), # Input file *first* for potential speedup on seek before decode
        "-ss", str(start_sec), # Start time *after* input for accuracy
        "-to", str(end_sec),   # End time
        "-copyts", # Copy timestamps to avoid starting near 0 if -ss is large
        "-avoid_negative_ts", "make_zero", # Adjust negative timestamps if they occur after seeking
    ]
    # Add encoding arguments
    cmd.extend(FFMPEG_CUT_ARGS)
    # Add output file
    cmd.append(str(output_path))

    logging.debug(f"Executing ffmpeg command: {' '.join(cmd)}")

    try:
        process = subprocess.run(cmd, check=True, capture_output=True, text=True)
        logging.debug(f"ffmpeg stdout:\n{process.stdout}")
        logging.debug(f"ffmpeg stderr:\n{process.stderr}")
        if not output_path.exists() or output_path.stat().st_size == 0:
             logging.error(f"ffmpeg command ran but output file is missing or empty: {output_path}")
             logging.error(f"ffmpeg stderr was:\n{process.stderr}")
             return False
        logging.info(f"Successfully cut video to {output_path}")
        return True
    except FileNotFoundError:
        logging.error(f"ffmpeg command not found at '{FFMPEG_PATH}'. Ensure ffmpeg is installed and in PATH.")
        return False
    except subprocess.CalledProcessError as e:
        logging.error(f"ffmpeg cutting failed for {input_path} -> {output_path}")
        logging.error(f"Command: {' '.join(e.cmd)}")
        logging.error(f"Return Code: {e.returncode}")
        logging.error(f"Stderr:\n{e.stderr}") # Stderr is usually most informative
        return False
    except Exception as e:
        logging.error(f"Unexpected error during ffmpeg cutting: {e}")
        import traceback
        logging.debug(traceback.format_exc())
        return False


# --- Main Execution ---
if __name__ == "__main__":
    setup_logging()

    # Create output directory if it doesn't exist
    try:
        OUTPUT_VIDEO_DIR.mkdir(parents=True, exist_ok=True)
        logging.info(f"Ensured output directory exists: {OUTPUT_VIDEO_DIR}")
    except OSError as e:
        logging.critical(f"Failed to create output directory {OUTPUT_VIDEO_DIR}: {e}. Exiting.")
        sys.exit(1)

    # Load CSV
    try:
        df = pd.read_csv(CSV_FILE_PATH)
        logging.info(f"Loaded CSV: {CSV_FILE_PATH} with {len(df)} rows.")
    except FileNotFoundError:
        logging.critical(f"Input CSV file not found: {CSV_FILE_PATH}. Exiting.")
        sys.exit(1)
    except Exception as e:
        logging.critical(f"Failed to load CSV {CSV_FILE_PATH}: {e}. Exiting.")
        sys.exit(1)

    # Filter for test mode if enabled
    if PROCESSING_MODE == 'test':
        if len(df) > NUM_TEST_ROWS:
            df = df.sample(n=NUM_TEST_ROWS, random_state=42) # Use random_state for reproducibility if needed
            logging.info(f"Running in 'test' mode. Sampled {len(df)} random rows.")
        else:
            logging.info(f"Running in 'test' mode, but dataset has <= {NUM_TEST_ROWS} rows. Processing all {len(df)} rows.")
    elif PROCESSING_MODE == 'full':
        logging.info("Running in 'full' mode. Processing all rows.")
    else:
        logging.critical(f"Invalid PROCESSING_MODE: '{PROCESSING_MODE}'. Choose 'test' or 'full'. Exiting.")
        sys.exit(1)


    # --- Process Videos ---
    processed_count = 0
    success_count = 0
    skipped_count = 0
    failed_count = 0

    logging.info("Starting video processing loop...")

    for index, row in df.iterrows():
        processed_count += 1
        row_data = row.to_dict()
        video_id = row_data.get('id')
        url = row_data.get('url')
        frame_start = row_data.get('frame_start')
        frame_end = row_data.get('frame_end')
        fps = row_data.get('fps')
        dataset_type = row_data.get('dataset_type') # WLASL or MSASL

        logging.info(f"--- Processing row {index} (ID: {video_id}) ---")

        # --- 1. Basic Validation and Path Setup ---
        try:
            video_id = int(video_id) # Ensure ID is integer for filename
            expected_output_path = OUTPUT_VIDEO_DIR / f"{video_id}.mp4"
            row_data['expected_output_path'] = str(expected_output_path) # Add for logging
            frame_start = int(frame_start)
            # frame_end can be -1, handle conversion carefully
            if frame_end != -1:
                frame_end = int(frame_end)
            fps = float(fps)
            if fps <= 0:
                raise ValueError("FPS must be positive")
        except (ValueError, TypeError) as e:
            logging.warning(f"Skipping row {index} (ID: {video_id}): Invalid numeric data (id, frame_start, frame_end, or fps). Error: {e}")
            log_status(LOG_FILE_PATH, row_data, "INVALID_DATA", f"Invalid numeric data: {e}")
            failed_count += 1
            continue

        if not url or pd.isna(url):
             logging.warning(f"Skipping row {index} (ID: {video_id}): Missing URL.")
             log_status(LOG_FILE_PATH, row_data, "INVALID_DATA", "Missing URL")
             failed_count += 1
             continue

        if not dataset_type or dataset_type not in ['WLASL', 'MSASL']:
            logging.warning(f"Skipping row {index} (ID: {video_id}): Missing or invalid dataset_type ('{dataset_type}'). Assuming MSASL frame indexing (0-based).")
            # Allow processing but log potential issue, default to MSASL logic
            # Alternatively, uncomment below to skip:
            # log_status(LOG_FILE_PATH, row_data, "INVALID_DATA", f"Invalid dataset_type: {dataset_type}")
            # failed_count += 1
            # continue

        # --- 2. Check if Already Processed ---
        if expected_output_path.exists() and expected_output_path.stat().st_size > 0:
            logging.info(f"Skipping row {index} (ID: {video_id}): Output file already exists: {expected_output_path}")
            log_status(LOG_FILE_PATH, row_data, "SKIPPED_EXISTING")
            skipped_count += 1
            continue

        # --- 3. Normalize URL ---
        normalized_url = normalize_url(url)
        if not normalized_url:
             logging.warning(f"Skipping row {index} (ID: {video_id}): Invalid or empty URL after normalization.")
             log_status(LOG_FILE_PATH, row_data, "INVALID_DATA", "Invalid/Empty URL")
             failed_count += 1
             continue
        row_data['normalized_url'] = normalized_url # Update for potential logging

        # --- 4. Determine if Cutting is Needed ---
        needs_cutting = (frame_end != -1)

        temp_download_path = None # Path for full download if cutting
        final_output_target = expected_output_path # Where the final file should end up

        # Define where yt-dlp should initially download
        if needs_cutting:
             # Download to a temporary file first, use expected name + _temp suffix
             temp_download_path_template = OUTPUT_VIDEO_DIR / f"{video_id}_temp"
             download_output_template = temp_download_path_template
             logging.debug(f"Cutting needed for ID {video_id}. Temp download template: {temp_download_path_template}")
        else:
             # Download directly to the final destination
             download_output_template = final_output_target.with_suffix('') # yt-dlp adds suffix
             logging.debug(f"No cutting needed for ID {video_id}. Direct download template: {download_output_template}")


        # --- 5. Download ---
        downloaded_full_path = download_video(normalized_url, download_output_template)

        if not downloaded_full_path:
            # Log the failure - the reason (yt-dlp/requests) is already logged by download_video
            logging.error(f"Download failed for row {index} (ID: {video_id}), URL: {normalized_url}. See debug log for details.")
            # Use a generic error message for the CSV log
            log_status(LOG_FILE_PATH, row_data, "FAILED_DOWNLOAD", f"Download failed for {normalized_url}")
            failed_count += 1
            continue # Move to next row

        # --- If download succeeded (either method) ---
        logging.info(f"Download successful for ID {video_id}. File at: {downloaded_full_path}. Proceeding to check for cutting.")

        # --- 6. Cut Video (if needed) ---
        cut_success = True # Assume success if no cutting needed
        if needs_cutting:
            try:
                # Calculate start/end times in seconds
                # Adjust start frame for WLASL (1-based index means frame 1 is at time 0)
                adjusted_frame_start = frame_start
                if dataset_type == 'WLASL':
                     if frame_start >= 1:
                         adjusted_frame_start = frame_start - 1
                     else:
                         logging.warning(f"WLASL row {index} (ID: {video_id}) has frame_start < 1 ({frame_start}). Using 0.")
                         adjusted_frame_start = 0

                start_time_sec = adjusted_frame_start / fps
                end_time_sec = frame_end / fps

                # Basic time validation
                if start_time_sec < 0 or end_time_sec < start_time_sec:
                    raise ValueError(f"Invalid calculated times: start={start_time_sec:.3f}s, end={end_time_sec:.3f}s")

                # Perform the cut
                cut_success = cut_video(downloaded_full_path, final_output_target, start_time_sec, end_time_sec)

            except ValueError as e:
                 logging.error(f"Skipping cut for row {index} (ID: {video_id}): Invalid time calculation or data. Error: {e}")
                 log_status(LOG_FILE_PATH, row_data, "FAILED_CUT", f"Invalid time calculation: {e}")
                 cut_success = False
                 failed_count += 1
            except Exception as e:
                 logging.error(f"Unexpected error during pre-cut/cut setup for row {index} (ID: {video_id}): {e}")
                 log_status(LOG_FILE_PATH, row_data, "FAILED_CUT", f"Unexpected error: {e}")
                 cut_success = False
                 failed_count += 1

            # --- 7. Clean up Temporary File ---
            if downloaded_full_path.exists():
                 try:
                     logging.debug(f"Removing temporary file: {downloaded_full_path}")
                     os.remove(downloaded_full_path)
                 except OSError as e:
                     logging.warning(f"Could not remove temporary file {downloaded_full_path}: {e}")
            else:
                 logging.debug(f"Temporary file {downloaded_full_path} already removed or wasn't created properly.")

            if not cut_success:
                 failed_count += 1 # Already logged status inside cut_video or exception block
                 continue # Move to next row

        else:
            # No cutting needed, ensure downloaded file has the correct final name
            # yt-dlp should have placed it correctly if template was right
             if downloaded_full_path != final_output_target:
                 logging.warning(f"Downloaded file path '{downloaded_full_path}' differs from expected '{final_output_target}'. Attempting rename.")
                 try:
                     downloaded_full_path.rename(final_output_target)
                     logging.info(f"Renamed {downloaded_full_path} to {final_output_target}")
                 except OSError as e:
                     logging.error(f"Failed to rename {downloaded_full_path} to {final_output_target}: {e}")
                     log_status(LOG_FILE_PATH, row_data, "FAILED_POSTPROCESS", f"Failed to rename downloaded file: {e}")
                     failed_count += 1
                     # Clean up the incorrectly named file if possible
                     if downloaded_full_path.exists():
                         try: os.remove(downloaded_full_path)
                         except OSError: pass
                     continue # Failed this step

        # --- 8. Final Success Logging ---
        if cut_success: # This is true if no cutting was needed OR if cutting succeeded
            log_status(LOG_FILE_PATH, row_data, "SUCCESS")
            success_count += 1
            logging.info(f"Successfully processed row {index} (ID: {video_id}) -> {final_output_target}")


    # --- End of Processing ---
    logging.info("--- Processing Complete ---")
    logging.info(f"Total rows considered: {processed_count}")
    logging.info(f"Successfully processed: {success_count}")
    logging.info(f"Skipped (already exist): {skipped_count}")
    logging.info(f"Failed (download/cut/invalid): {failed_count}")
    logging.info(f"Check {LOG_FILE_PATH} for detailed status of each video.")
    logging.info(f"Check {SCRIPT_DIR / 'processing_debug.log'} for detailed debug logs.")