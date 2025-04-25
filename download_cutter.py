import subprocess
import os
import glob
import sys

def download_and_cut_segments(youtube_url, segments, output_base_name, cleanup_full_video=True):
    """
    Downloads a full YouTube video and cuts specified time segments using ffmpeg.

    Args:
        youtube_url (str): The URL of the YouTube video.
        segments (list): A list of tuples or lists, where each inner element
                         is a pair of strings representing the start and end
                         time of a segment (e.g., [("0:00", "0:05"), ("0:15", "0:30")]).
                         Time format can be HH:MM:SS, MM:SS, or seconds.
        output_base_name (str): The base name for the output files (e.g., "my_video").
                                Segments will be named like my_video_segment_1.mp4, etc.
                                The full video will be named like my_video_full.mp4 (if not cleaned up).
        cleanup_full_video (bool): If True, the full downloaded video file
                                   will be removed after cutting the segments.
                                   Defaults to True.
    """

    # --- Step 1: Download the full video ---
    # Use a template for the full video filename. %(ext)s will be replaced by yt-dlp
    full_video_template = f"{output_base_name}_full.%(ext)s"
    # We'll use glob to find the exact filename after download
    full_video_pattern = f"{output_base_name}_full.*"

    print(f"Attempting to download full video from {youtube_url}...")

    # yt-dlp command
    # -o specifies the output template
    download_cmd = [sys.executable, "-m", "yt_dlp", "-o", full_video_template, youtube_url]

    try:
        # Execute the command
        # capture_output=True and text=True are useful for debugging if needed
        # check=True will raise CalledProcessError if the command fails
        process = subprocess.run(download_cmd, check=True, capture_output=True, text=True)
        print("Full video download command executed. Checking for file...")

        # --- Find the exact downloaded filename ---
        # yt-dlp prints the final destination path to stdout
        # Let's parse the output to be sure, although glob is often sufficient
        downloaded_path = None
        for line in process.stdout.splitlines():
             if "[download] Destination:" in line:
                 # Extract the path after "Destination: "
                 downloaded_path = line.split("Destination: ", 1)[-1].strip()
                 break

        if not downloaded_path:
             # Fallback to glob if parsing stdout fails (less reliable)
             print("Could not parse destination path from yt-dlp stdout. Using glob...")
             downloaded_files = glob.glob(full_video_pattern)
             if not downloaded_files:
                 print(f"Error: Could not find the downloaded file matching pattern {full_video_pattern}")
                 return
             downloaded_path = downloaded_files[0] # Assume the first match is correct
             print(f"Found file using glob: {downloaded_path}")
        else:
             print(f"Downloaded file destination: {downloaded_path}")


    except FileNotFoundError:
        print("\nError: yt-dlp command not found.")
        print("Please ensure yt-dlp is installed (`pip install yt-dlp`)")
        print("and that your Python environment's scripts directory is in your PATH.")
        return
    except subprocess.CalledProcessError as e:
        print(f"\nError during yt-dlp download:")
        print(f"Command: {' '.join(e.cmd)}")
        print(f"Return Code: {e.returncode}")
        print(f"Stdout:\n{e.stdout}")
        print(f"Stderr:\n{e.stderr}")
        return
    except Exception as e:
        print(f"\nAn unexpected error occurred during download: {e}")
        return


    # --- Step 2: Cut segments using ffmpeg ---
    print(f"\nCutting segments from '{downloaded_path}'...")

    # Check if the full video file exists before trying to cut
    if not os.path.exists(downloaded_path):
        print(f"Error: Full video file '{downloaded_path}' not found after download.")
        return

    # Determine output file extension. Assume same as input or force .mp4 for copy
    input_ext = os.path.splitext(downloaded_path)[1]
    output_ext = ".mp4" # Using mp4 as it's common and works well with -c copy

    for i, (start_time, end_time) in enumerate(segments):
        segment_output_name = f"{output_base_name}_segment_{i+1}{output_ext}"
        print(f"  Cutting segment {i+1}: {start_time} to {end_time} into '{segment_output_name}'")

        # ffmpeg command for cutting
        # -i input file
        # -ss start time
        # -to end time (alternative: -t duration)
        # -c copy copies streams without re-encoding (fast, preserves quality, but cut points might be approximate)
        cut_cmd = [
            "ffmpeg",
            "-i", downloaded_path,
            "-ss", str(start_time), # Ensure times are strings
            "-to", str(end_time),
            "-c", "copy",
            segment_output_name
        ]

        try:
            # Execute the ffmpeg command
            subprocess.run(cut_cmd, check=True, capture_output=True, text=True)
            print(f"  Successfully created '{segment_output_name}'")

        except FileNotFoundError:
            print("\nError: ffmpeg command not found.")
            print("Please ensure ffmpeg is installed and in your system's PATH.")
            # If ffmpeg isn't found, no further segments can be cut.
            return
        except subprocess.CalledProcessError as e:
            print(f"\nError cutting segment {i+1} ('{segment_output_name}'):")
            print(f"Command: {' '.join(e.cmd)}")
            print(f"Return Code: {e.returncode}")
            print(f"Stdout:\n{e.stdout}")
            print(f"Stderr:\n{e.stderr}")
            # Decide if you want to stop or try to cut the next segment
            # For now, let's continue trying to cut the other segments
            print("  Continuing with next segment (if any)...")
        except Exception as e:
             print(f"\nAn unexpected error occurred during segment cutting: {e}")
             print("  Continuing with next segment (if any)...")


    # --- Step 3: Clean up the full video file (Optional) ---
    if cleanup_full_video and os.path.exists(downloaded_path):
        print(f"\nCleaning up full video file: '{downloaded_path}'...")
        try:
            os.remove(downloaded_path)
            print("Full video file removed.")
        except OSError as e:
            print(f"Error removing full video file '{downloaded_path}': {e}")

    print("\nScript finished.")

# --- Example Usage ---
if __name__ == "__main__":
    video_url = "https://www.youtube.com/watch?v=0VdUmQ_XjJg"

    # Define the segments you want to cut
    # Format: [("start_time1", "end_time1"), ("start_time2", "end_time2"), ...]
    # Time can be in HH:MM:SS, MM:SS, or seconds (ffmpeg handles various formats)
    time_segments = [
        ("0:00", "0:05"),  # First 5 seconds
        ("0:15", "0:30"),  # From 15 seconds to 30 seconds
        # Add more segments here if needed
        # ("1:00", "1:15"), # From 1 minute to 1 minute 15 seconds
        # ("65", "70")      # Same as above, using seconds
    ]

    # Base name for the output files
    output_prefix = "barca_mallorca_segments"

    # Run the function
    download_and_cut_segments(video_url, time_segments, output_prefix, cleanup_full_video=True)