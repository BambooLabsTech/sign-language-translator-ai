import csv
import os
import cv2
import mediapipe as mp
import numpy as np
import pandas as pd
from tqdm import tqdm # For progress bar

# --- Configuration ---
BASE_DIR = os.getcwd() # Assumes script is in sign-language-translator-ai/
VIDEOS_DIR = os.path.join(BASE_DIR, "videos")
METADATA_FILE = os.path.join(BASE_DIR, "splitted_asl.csv")

OUTPUT_HANDS_DIR = os.path.join(BASE_DIR, "hands_landmarks_original")
OUTPUT_HOLISTIC_DIR = os.path.join(BASE_DIR, "holistic_landmarks_original")
LOG_FILE = os.path.join(BASE_DIR, "landmark_extraction_log.txt")

# Test mode: 'full' or an integer (e.g., 5 for 5 videos)
# If you set an integer, it will process the first N videos from the CSV.
PROCESSING_MODE = 'full' # Options: 'full' or an integer for number of videos to test

# MediaPipe constants
MP_HANDS = mp.solutions.hands
MP_HOLISTIC = mp.solutions.holistic
MP_DRAWING = mp.solutions.drawing_utils # Only if you wanted to visualize, not needed for saving

# Number of landmarks (for pre-allocation or verification, if needed)
NUM_POSE_LANDMARKS = 33
NUM_FACE_LANDMARKS_REFINE = 478 # With refine_face_landmarks=True
NUM_HAND_LANDMARKS = 21

def setup_directories():
    """Creates output directories if they don't exist."""
    os.makedirs(OUTPUT_HANDS_DIR, exist_ok=True)
    os.makedirs(OUTPUT_HOLISTIC_DIR, exist_ok=True)

def log_error(message):
    """Logs an error message to the log file and prints it."""
    print(f"ERROR: {message}")
    with open(LOG_FILE, "a") as f:
        f.write(f"{message}\n")

def extract_landmarks_to_array(landmark_list, num_landmarks, num_dimensions=3):
    """
    Converts MediaPipe landmark list to a NumPy array (num_landmarks, num_dimensions).
    Returns None if landmark_list is None.
    """
    if landmark_list is None:
        return None
    
    # Ensure all landmarks are present, otherwise it might indicate an issue
    # or an unexpected MediaPipe version. For robustness, we can extract
    # what's available up to num_landmarks.
    
    coords = np.zeros((num_landmarks, num_dimensions))
    for i, landmark in enumerate(landmark_list.landmark):
        if i < num_landmarks: # Ensure we don't go out of bounds if MP provides more than expected for some reason
            coords[i, 0] = landmark.x
            coords[i, 1] = landmark.y
            coords[i, 2] = landmark.z
        else:
            break # Should not happen with standard MediaPipe outputs
    return coords

def main():
    setup_directories()
    
    print(f"Base directory: {BASE_DIR}")
    print(f"Videos directory: {VIDEOS_DIR}")
    print(f"Metadata file: {METADATA_FILE}")
    print(f"Output Hands directory: {OUTPUT_HANDS_DIR}")
    print(f"Output Holistic directory: {OUTPUT_HOLISTIC_DIR}")
    print(f"Log file: {LOG_FILE}")
    print(f"Processing mode: {PROCESSING_MODE}")

    if not os.path.exists(METADATA_FILE):
        log_error(f"Metadata file not found: {METADATA_FILE}")
        return

    if not os.path.exists(VIDEOS_DIR):
        log_error(f"Videos directory not found: {VIDEOS_DIR}")
        return

    try:
        df = pd.read_csv(METADATA_FILE)
    except Exception as e:
        log_error(f"Failed to read metadata CSV: {e}")
        return

    if PROCESSING_MODE != 'full':
        try:
            num_videos_to_process = int(PROCESSING_MODE)
            df = df.head(num_videos_to_process)
            print(f"--- Running in TEST MODE: Processing first {num_videos_to_process} videos ---")
        except ValueError:
            log_error(f"Invalid PROCESSING_MODE: {PROCESSING_MODE}. Must be 'full' or an integer.")
            return
    else:
        print(f"--- Running in FULL MODE: Processing all {len(df)} videos ---")


    # Initialize MediaPipe solutions
    # Using `with` ensures resources are closed automatically
    with MP_HANDS.Hands(
        static_image_mode=False,
        max_num_hands=2,
        min_detection_confidence=0.5, # Default
        min_tracking_confidence=0.5   # Default
    ) as hands_model, \
    MP_HOLISTIC.Holistic(
        static_image_mode=False,
        refine_face_landmarks=True, # As discussed
        min_detection_confidence=0.5,
        min_tracking_confidence=0.5
    ) as holistic_model:

        for index, row in tqdm(df.iterrows(), total=df.shape[0], desc="Processing videos"):
            video_id = str(row['id'])
            video_filename = f"{video_id}.mp4"
            video_path = os.path.join(VIDEOS_DIR, video_filename)

            output_hands_file = os.path.join(OUTPUT_HANDS_DIR, f"{video_id}.npy")
            output_holistic_file = os.path.join(OUTPUT_HOLISTIC_DIR, f"{video_id}.npy")

            # --- Resumability Check ---
            if os.path.exists(output_hands_file) and os.path.exists(output_holistic_file):
                # print(f"Skipping Video ID {video_id}: Output files already exist.")
                continue
            
            if not os.path.exists(video_path):
                log_error(f"Video ID {video_id}: File not found at {video_path}")
                continue

            cap = cv2.VideoCapture(video_path)
            if not cap.isOpened():
                log_error(f"Video ID {video_id}: Could not open video file {video_path}")
                continue

            video_hands_landmarks = []
            video_holistic_landmarks = []
            
            frame_count = 0
            while cap.isOpened():
                ret, frame = cap.read()
                if not ret:
                    break # End of video or error reading frame

                frame_count += 1
                # Convert the BGR image to RGB.
                image_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                
                # To improve performance, optionally mark the image as not writeable to
                # pass by reference.
                image_rgb.flags.writeable = False

                # Process with Hands model
                hands_results = hands_model.process(image_rgb)
                
                # Process with Holistic model
                holistic_results = holistic_model.process(image_rgb)

                image_rgb.flags.writeable = True # Make it writeable again if you were to draw or use it further

                # --- Store Hands Landmarks for current frame ---
                frame_hands_data = []
                if hands_results.multi_hand_landmarks:
                    for hand_landmarks, handedness_obj in zip(hands_results.multi_hand_landmarks, hands_results.multi_handedness):
                        landmarks_arr = extract_landmarks_to_array(hand_landmarks, NUM_HAND_LANDMARKS)
                        if landmarks_arr is not None:
                            frame_hands_data.append({
                                "landmarks": landmarks_arr,
                                "handedness": handedness_obj.classification[0].label # 'Left' or 'Right'
                            })
                video_hands_landmarks.append(frame_hands_data) # Append list of hands for this frame

                # --- Store Holistic Landmarks for current frame ---
                pose_arr = extract_landmarks_to_array(holistic_results.pose_landmarks, NUM_POSE_LANDMARKS)
                face_arr = extract_landmarks_to_array(holistic_results.face_landmarks, NUM_FACE_LANDMARKS_REFINE)
                
                # Holistic model also provides hand landmarks. Use these for the holistic output.
                # These might be more consistent within the holistic model's context.
                left_hand_arr = extract_landmarks_to_array(holistic_results.left_hand_landmarks, NUM_HAND_LANDMARKS)
                right_hand_arr = extract_landmarks_to_array(holistic_results.right_hand_landmarks, NUM_HAND_LANDMARKS)
                
                video_holistic_landmarks.append({
                    "pose": pose_arr,
                    "face": face_arr,
                    "left_hand": left_hand_arr,
                    "right_hand": right_hand_arr
                })

            cap.release()

            if not video_hands_landmarks and not video_holistic_landmarks and frame_count > 0:
                 log_error(f"Video ID {video_id}: Processed {frame_count} frames but no landmarks were extracted at all.")
            elif frame_count == 0 and os.path.exists(video_path): # Video exists but no frames read
                 log_error(f"Video ID {video_id}: Video file exists but could not read any frames.")


            # Save the collected landmarks for the video
            if video_hands_landmarks: # Only save if there's some data
                try:
                    np.save(output_hands_file, np.array(video_hands_landmarks, dtype=object))
                except Exception as e:
                    log_error(f"Video ID {video_id}: Failed to save hands landmarks to {output_hands_file}. Error: {e}")
            
            if video_holistic_landmarks: # Only save if there's some data
                try:
                    np.save(output_holistic_file, np.array(video_holistic_landmarks, dtype=object))
                except Exception as e:
                    log_error(f"Video ID {video_id}: Failed to save holistic landmarks to {output_holistic_file}. Error: {e}")
            
            # Minor progress update within the loop if needed, but tqdm handles overall
            # if (index + 1) % 10 == 0 :
            #     print(f"Processed {index + 1} / {len(df)} videos...")

    print("Landmark extraction process finished.")
    if os.path.exists(LOG_FILE):
        print(f"Check '{LOG_FILE}' for any errors.")

if __name__ == '__main__':
    main()