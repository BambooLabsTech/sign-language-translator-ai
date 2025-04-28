import subprocess
import os
import glob
import sys
import json # For parsing ffprobe output
import re   # For parsing time strings

# --- Helper Function to Parse Time String to Seconds ---
def time_str_to_seconds(time_str):
    """Converts HH:MM:SS.ms, MM:SS.ms, or SS.ms string to seconds."""
    if isinstance(time_str, (int, float)): # Already seconds
        return float(time_str)
    
    time_str = str(time_str).strip()
    parts = time_str.split(':')
    try:
        if len(parts) == 3: # HH:MM:SS.ms
            h, m, s = parts
            return int(h) * 3600 + int(m) * 60 + float(s)
        elif len(parts) == 2: # MM:SS.ms
            m, s = parts
            return int(m) * 60 + float(s)
        elif len(parts) == 1: # SS.ms
            return float(parts[0])
        else:
            raise ValueError("Invalid time format")
    except ValueError:
        print(f"Warning: Could not parse time string '{time_str}'. Returning 0.")
        return 0.0 # Or raise a more specific error

# --- Helper Function to Get Video Duration ---
def get_video_duration(filepath):
    """Gets the duration of a video file using ffprobe."""
    if not os.path.exists(filepath):
        print(f"Error: File not found for duration check: {filepath}")
        return None

    cmd = [
        "ffprobe",
        "-v", "error",                  # Hide informational messages
        "-show_entries", "format=duration", # Only get the duration
        "-of", "default=noprint_wrappers=1:nokey=1", # Output only the value
        # Alternative using JSON output (more robust parsing)
        # "-print_format", "json",
        # "-show_format",
        filepath
    ]
    try:
        process = subprocess.run(cmd, check=True, capture_output=True, text=True)
        duration_str = process.stdout.strip()
        if not duration_str or duration_str.lower() == 'n/a':
             # Fallback attempt if simple duration fails (e.g., complex formats)
             cmd_json = [
                 "ffprobe", "-v", "error", "-print_format", "json",
                 "-show_format", "-show_streams", filepath
             ]
             process_json = subprocess.run(cmd_json, check=True, capture_output=True, text=True)
             data = json.loads(process_json.stdout)
             if 'format' in data and 'duration' in data['format']:
                 return float(data['format']['duration'])
             else:
                 print(f"Warning: Could not determine duration from ffprobe output for {filepath}.")
                 return None # Indicate failure
        return float(duration_str)
    except FileNotFoundError:
        print("\nError: ffprobe command not found.")
        print("Please ensure ffmpeg (which includes ffprobe) is installed and in your system's PATH.")
        return None # Indicate failure
    except subprocess.CalledProcessError as e:
        print(f"\nError running ffprobe for '{filepath}':")
        print(f"Command: {' '.join(e.cmd)}")
        print(f"Return Code: {e.returncode}")
        print(f"Stderr:\n{e.stderr}")
        return None # Indicate failure
    except ValueError:
        print(f"Warning: Could not convert ffprobe duration output '{duration_str}' to float for {filepath}.")
        return None # Indicate failure
    except Exception as e:
        print(f"\nAn unexpected error occurred during ffprobe execution: {e}")
        return None # Indicate failure


def download_and_cut_segments(youtube_url, segments, output_base_name, cleanup_full_video=True, use_accurate_cutting=False):
    """
    Downloads a full YouTube video and cuts specified time segments using ffmpeg.

    Args:
        youtube_url (str): The URL of the YouTube video.
        segments (list): A list of tuples or lists, where each inner element
                         is a pair of strings/numbers representing the start and end
                         time of a segment (e.g., [("0:00", 5), ("15", "0:30")]).
                         Time format can be HH:MM:SS, MM:SS, or seconds.
        output_base_name (str): The base name for the output files.
        cleanup_full_video (bool): If True, remove the full downloaded video.
        use_accurate_cutting (bool): If True, use slower re-encoding for frame-accurate
                                     cuts. If False (default), use fast '-c copy' which
                                     may result in slightly inaccurate segment lengths
                                     due to keyframe limitations.
    """

    # --- Step 1: Download --- (Keep your existing robust download logic)
    full_video_template = f"{output_base_name}_full.%(ext)s"
    full_video_pattern = f"{output_base_name}_full.*"

    print(f"Attempting to download full video from {youtube_url}...")
    # ... (rest of your yt-dlp download and file finding logic remains the same) ...
    # Make sure 'downloaded_path' is correctly set after this block
    # Example placeholder for the download logic result:
    # downloaded_path = "path/to/your/downloaded/video.ext" # This needs to be set by your download code

    # --- (Copying the download block from your previous working version) ---
    print(f"Output template: {full_video_template}")
    print(f"File search pattern: {full_video_pattern}")
    download_cmd = [sys.executable, "-m", "yt_dlp", "-o", full_video_template, youtube_url]
    downloaded_path = None
    try:
        process = subprocess.run(download_cmd, check=True, capture_output=True, text=True)
        print("Full video download command executed successfully.")
        print(f"Searching for downloaded file matching pattern: '{full_video_pattern}'")
        downloaded_files = glob.glob(full_video_pattern)

        if not downloaded_files:
            # ... (Error handling as before) ...
            print(f"\nError: Could not find the downloaded file matching pattern '{full_video_pattern}' after successful download command.")
            # ...(print stdout/stderr etc.) ...
            return
        elif len(downloaded_files) > 1:
            # ... (Multi-file handling as before) ...
             print(f"Warning: Found multiple files matching pattern '{full_video_pattern}': {downloaded_files}")
             # Try to use stdout parsing to disambiguate
             parsed_path = None
             for line in process.stdout.splitlines():
                if ("[download] Destination:" in line or
                    "[Merger] Merging formats into" in line or
                    "[ExtractAudio] Destination:" in line or
                    "[VideoConvertor] Destination:" in line):
                    try:
                        if '"' in line: candidate = line.split('"')[1]
                        else: candidate = line.split("Destination: ", 1)[-1].strip()
                        if os.path.abspath(candidate) in [os.path.abspath(f) for f in downloaded_files]:
                             parsed_path = candidate
                             print(f"Prioritizing file mentioned in yt-dlp output: '{parsed_path}'")
                             break
                    except IndexError: continue
             if parsed_path and os.path.exists(parsed_path): downloaded_path = parsed_path
             else:
                downloaded_path = downloaded_files[0]
                print(f"Using the first file found by glob: {downloaded_path}")
        else:
            downloaded_path = downloaded_files[0]
            print(f"Found downloaded file using glob: {downloaded_path}")

        if not downloaded_path or not os.path.exists(downloaded_path):
             print(f"\nError: Could not definitively determine or locate the downloaded video file.")
             print(f"Path determined: {downloaded_path}")
             print(f"Files matching pattern '{full_video_pattern}': {glob.glob(full_video_pattern)}")
             return
    # ... (Rest of your exception handling for download) ...
    except FileNotFoundError:
        print("\nError: Required command (python or yt-dlp) not found.")
        return
    except subprocess.CalledProcessError as e:
        print(f"\nError during yt-dlp download:")
        print(f"Stderr:\n{e.stderr}") # Often more useful than stdout on error
        return
    except Exception as e:
        print(f"\nAn unexpected error occurred during download phase: {e}")
        return
    # --- End Download Block ---


    # --- Step 1.5: Get Full Video Duration ---
    print(f"\nGetting duration of '{downloaded_path}'...")
    full_duration = get_video_duration(downloaded_path)

    if full_duration is None:
        print("Error: Could not get video duration. Cannot proceed with segment cutting.")
        # Optional: Clean up downloaded file if desired, even on error
        if cleanup_full_video and os.path.exists(downloaded_path):
            try: os.remove(downloaded_path)
            except OSError as e: print(f"Error removing video file during cleanup: {e}")
        return

    print(f"Full video duration: {full_duration:.2f} seconds")


    # --- Step 2: Cut segments using ffmpeg ---
    print(f"\nCutting segments from '{downloaded_path}'...")
    if not use_accurate_cutting:
        print("Note: Using fast cutting (-c copy). Segment durations might be approximate due to keyframe alignment.")
    else:
        print("Note: Using accurate cutting (re-encoding). This will be slower.")


    output_ext = ".mp4" # Force mp4 for segments
    ffmpeg_errors = False

    for i, (start_time_req, end_time_req) in enumerate(segments):
        segment_output_name = f"{output_base_name}_segment_{i+1}{output_ext}"

        try:
            # Convert requested times to seconds for validation
            start_sec = time_str_to_seconds(start_time_req)
            end_sec = time_str_to_seconds(end_time_req)

            # --- Validation Checks ---
            if start_sec < 0 or end_sec < 0:
                print(f" Skipping segment {i+1}: Invalid negative time requested ({start_time_req} to {end_time_req}).")
                continue # Skip to next segment

            if start_sec >= end_sec:
                 print(f" Skipping segment {i+1}: Start time ({start_time_req}) is not before end time ({end_time_req}).")
                 continue

            if start_sec >= full_duration:
                print(f" Skipping segment {i+1}: Start time ({start_time_req} / {start_sec:.2f}s) is beyond video duration ({full_duration:.2f}s).")
                continue

            # Adjust end_sec if it exceeds duration, and inform the user
            effective_end_sec = end_sec
            if end_sec > full_duration:
                print(f" Warning for segment {i+1}: Requested end time ({end_time_req} / {end_sec:.2f}s) exceeds video duration ({full_duration:.2f}s). Cutting until the end.")
                effective_end_sec = full_duration # Cut until the actual end

            print(f" Cutting segment {i+1}: {start_time_req} to {end_time_req} (effective: {start_sec:.2f}s to {effective_end_sec:.2f}s) into '{segment_output_name}'")


            # --- Build the ffmpeg command ---
            if use_accurate_cutting:
                # Accurate (re-encoding) command
                 cut_cmd = [
                    "ffmpeg",
                    "-y", # Overwrite output
                    "-i", downloaded_path,      # Input file *first*
                    "-ss", str(start_sec),      # Start time *after* input for accuracy
                    "-to", str(effective_end_sec), # End time
                    # Choose your re-encoding options (examples)
                    "-c:v", "libx264",       # Video codec
                    "-preset", "fast",       # Speed/compression trade-off
                    "-crf", "22",            # Quality (lower means better/larger)
                    "-c:a", "aac",           # Audio codec
                    "-b:a", "128k",          # Audio bitrate
                    segment_output_name
                ]
            else:
                 # Fast (stream copy) command
                 cut_cmd = [
                    "ffmpeg",
                    "-y", # Overwrite output
                    "-ss", str(start_sec), # Start time *before* input for speed
                    "-i", downloaded_path,
                    "-to", str(effective_end_sec), # End time
                    "-c", "copy",          # Stream copy
                    "-avoid_negative_ts", "make_zero",
                    segment_output_name
                 ]

            # Execute the ffmpeg command
            process_ffmpeg = subprocess.run(cut_cmd, check=True, capture_output=True, text=True)
            print(f" Successfully created '{segment_output_name}'")

            # Optional: Check actual duration of the created segment
            segment_duration = get_video_duration(segment_output_name)
            if segment_duration is not None:
                 expected_duration = effective_end_sec - start_sec
                 print(f"   Actual segment duration: {segment_duration:.2f}s (Expected: ~{expected_duration:.2f}s)")
                 # Add a warning if using -c copy and duration differs significantly
                 if not use_accurate_cutting and abs(segment_duration - expected_duration) > 1.0: # Example threshold: 1 second difference
                     print(f"   Warning: Actual duration differs significantly from expected, likely due to '-c copy' and keyframes.")


        except FileNotFoundError:
            print("\nError: ffmpeg/ffprobe command not found.")
            print("Please ensure ffmpeg is installed and in your system's PATH.")
            ffmpeg_errors = True
            break # Cannot continue without ffmpeg/ffprobe
        except subprocess.CalledProcessError as e:
            print(f"\nError processing segment {i+1} ('{segment_output_name}'):")
            print(f"Command: {' '.join(e.cmd)}")
            print(f"Return Code: {e.returncode}")
            print(f"Stderr:\n{e.stderr}") # Stderr is usually most informative
            ffmpeg_errors = True
            print(" Continuing with next segment (if any)...")
        except Exception as e:
            print(f"\nAn unexpected error occurred during segment processing for {segment_output_name}: {e}")
            import traceback
            traceback.print_exc() # Print full traceback for unexpected errors
            ffmpeg_errors = True
            print(" Continuing with next segment (if any)...")


    # --- Step 3: Clean up --- (Keep your existing cleanup logic)
    if cleanup_full_video:
         if downloaded_path and os.path.exists(downloaded_path):
            print(f"\nCleaning up full video file: '{downloaded_path}'...")
            try:
                os.remove(downloaded_path)
                print("Full video file removed.")
            except OSError as e:
                print(f"Error removing full video file '{downloaded_path}': {e}")
         elif downloaded_path:
             print(f"\nCleanup skipped: Full video file '{downloaded_path}' not found.")
         else:
             print("\nCleanup skipped: Full video path was not determined.")


    print("\nScript finished.")
    if ffmpeg_errors:
        print("Warning: One or more errors occurred during segment processing.")


# --- Example Usage ---
if __name__ == "__main__":
    #video_url = "https://www.youtube.com/watch?v=jNQXAC9IVRw" # Big Buck Bunny (Short: ~1 min)
    video_url = "https://www.youtube.com/watch?v=0VdUmQ_XjJg" # Your previous example
    #video_url = "https://www.youtube.com/watch?v=aqz-KE-bpKQ" # Example: Jellyfish (Longer)

    # Define the segments - Use strings or numbers for times
    time_segments = [
        ("0:00", "0:05"),       # First 5 seconds
        ("0:15", "0:30"),       # 15s to 30s (This might exceed duration in short videos)
        (65, 75),               # 1m5s to 1m15s (Using seconds)
        ("01:10", "01:19.5"),   # Example with minutes and fractional seconds
    ]

    output_prefix = "test_video_segments"

    # Run the function
    # Set use_accurate_cutting=True for precise cuts (slower)
    # Set use_accurate_cutting=False for fast cuts (default, potentially imprecise)
    download_and_cut_segments(
        video_url,
        time_segments,
        output_prefix,
        cleanup_full_video=True,
        use_accurate_cutting=True # <<--- CHANGE THIS TO True FOR ACCURACY
    )