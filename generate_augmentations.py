import os
import numpy as np
import pandas as pd
import random
import math
import argparse
from tqdm import tqdm

# --- CONFIGURATION ---
METADATA_FILE = 'splitted_asl.csv'
HANDS_INPUT_DIR = 'hands_landmarks_original'
HOLISTIC_INPUT_DIR = 'holistic_landmarks_original'

HANDS_OUTPUT_DIR = 'hands_landmarks_augmented'
HOLISTIC_OUTPUT_DIR = 'holistic_landmarks_augmented'
AUGMENTED_METADATA_FILE = 'splitted_asl_augmented.csv'

# Number of augmented versions to create for each training sample
AUGMENTATION_FACTOR = 4

# Augmentation Parameters
SCALE_RANGE = (0.85, 1.15)
ROTATION_RANGE_DEG = (-15, 15)
TRANSLATION_RANGE = (-0.05, 0.05) # As a fraction of the coordinate system
NOISE_STD_DEV = 0.001

# --- HELPER FUNCTIONS ---

def get_shoulder_midpoint(pose_landmarks):
    """Calculates the midpoint between the shoulders."""
    # MediaPipe pose landmarks: 11 is left shoulder, 12 is right shoulder
    if pose_landmarks is not None and len(pose_landmarks) > 12:
        left_shoulder = pose_landmarks[11][:3] # Use x,y,z
        right_shoulder = pose_landmarks[12][:3]
        return (left_shoulder + right_shoulder) / 2
    return None

def normalize_and_augment_landmarks(landmarks, center_point, scale, angle_rad, trans_x, trans_y, noise_std):
    """Normalizes, augments, and adds noise to a set of landmarks."""
    if landmarks is None or center_point is None:
        return None

    # Ensure landmarks have at least 3 columns (x, y, z)
    coords = landmarks[:, :3].copy()
    
    # 1. Normalize by subtracting the center point
    coords -= center_point
    
    # 2. Augment: Scale
    coords *= scale
    
    # 3. Augment: Rotate (2D rotation in the XY plane)
    cos_a, sin_a = math.cos(angle_rad), math.sin(angle_rad)
    rotation_matrix = np.array([[cos_a, -sin_a, 0],
                                [sin_a,  cos_a, 0],
                                [0,      0,     1]])
    coords = coords @ rotation_matrix.T
    
    # 4. Augment: Translate
    coords[:, 0] += trans_x
    coords[:, 1] += trans_y
    
    # 5. Augment: Add Noise
    noise = np.random.normal(0, noise_std, coords.shape)
    coords += noise
    
    # Create a new array to hold the potentially modified data
    new_landmarks = landmarks.copy()
    new_landmarks[:, :3] = coords

    return new_landmarks


def process_video(video_id):
    """
    Loads a video's landmarks, generates augmented versions, saves them,
    and returns metadata for the new files.
    """
    hands_path = os.path.join(HANDS_INPUT_DIR, f'{video_id}.npy')
    holistic_path = os.path.join(HOLISTIC_INPUT_DIR, f'{video_id}.npy')

    try:
        hands_data = np.load(hands_path, allow_pickle=True)
        holistic_data = np.load(holistic_path, allow_pickle=True)
    except FileNotFoundError:
        print(f"Warning: Skipping ID {video_id} - file not found.")
        return []

    num_frames = len(holistic_data)
    new_metadata_rows = []

    for i in range(AUGMENTATION_FACTOR):
        # Determine a random set of augmentations for this version
        aug_scale = random.uniform(*SCALE_RANGE)
        aug_angle_deg = random.uniform(*ROTATION_RANGE_DEG)
        aug_angle_rad = math.radians(aug_angle_deg)
        aug_trans_x = random.uniform(*TRANSLATION_RANGE)
        aug_trans_y = random.uniform(*TRANSLATION_RANGE)
        
        aug_name = f"aug_{i}_s{aug_scale:.2f}_r{aug_angle_deg:.1f}_t{aug_trans_x:.2f}"
        
        new_hands_sequence = np.empty(num_frames, dtype=object)
        new_holistic_sequence = np.empty(num_frames, dtype=object)

        for frame_idx in range(num_frames):
            # --- Process Holistic Data ---
            holistic_frame = holistic_data[frame_idx].copy()
            shoulder_midpoint = get_shoulder_midpoint(holistic_frame.get('pose_landmarks'))
            
            if shoulder_midpoint is not None:
                # Augment all parts of the holistic data with the same transformation
                for key in ['pose_landmarks', 'face_landmarks', 'left_hand_landmarks', 'right_hand_landmarks']:
                    holistic_frame[key] = normalize_and_augment_landmarks(
                        holistic_frame.get(key), shoulder_midpoint, aug_scale, aug_angle_rad,
                        aug_trans_x, aug_trans_y, NOISE_STD_DEV
                    )
            new_holistic_sequence[frame_idx] = holistic_frame

            # --- Process Hands Data ---
            hands_frame = hands_data[frame_idx]
            new_hands_frame = []
            if hands_frame is not None and len(hands_frame) > 0:
                for hand in hands_frame:
                    new_hand = hand.copy()
                    wrist_point = new_hand['landmarks'][0][:3] # Use wrist as center
                    new_hand['landmarks'] = normalize_and_augment_landmarks(
                        new_hand['landmarks'], wrist_point, aug_scale, aug_angle_rad,
                        aug_trans_x, aug_trans_y, NOISE_STD_DEV
                    )
                    new_hands_frame.append(new_hand)
            new_hands_sequence[frame_idx] = new_hands_frame

        # Save the new augmented sequences
        new_id = f"{video_id}_{aug_name}"
        new_hands_path = os.path.join(HANDS_OUTPUT_DIR, f"{new_id}.npy")
        new_holistic_path = os.path.join(HOLISTIC_OUTPUT_DIR, f"{new_id}.npy")
        
        np.save(new_hands_path, new_hands_sequence)
        np.save(new_holistic_path, new_holistic_sequence)

        # Return metadata for this new augmented file
        new_metadata_rows.append({'id': new_id})

    return new_metadata_rows


def main(mode):
    """
    Main execution function.
    """
    print(f"--- Starting Augmentation in '{mode.upper()}' Mode ---")

    # Create output directories if they don't exist
    os.makedirs(HANDS_OUTPUT_DIR, exist_ok=True)
    os.makedirs(HOLISTIC_OUTPUT_DIR, exist_ok=True)
    print("Output directories ensured.")

    df = pd.read_csv(METADATA_FILE)
    
    if mode == 'test':
        # In test mode, use a small slice of the data for quick validation
        train_samples = df[df['dataset_split'] == 'train'].head(5)
        other_samples = df[df['dataset_split'] != 'train'].head(10)
        df_to_process = pd.concat([train_samples, other_samples])
        print(f"TEST MODE: Processing {len(df_to_process)} total samples.")
    else:
        df_to_process = df
        print(f"FULL MODE: Processing {len(df_to_process)} total samples.")

    new_metadata = []

    for _, row in tqdm(df_to_process.iterrows(), total=len(df_to_process), desc="Processing videos"):
        video_id = row['id']
        
        if row['dataset_split'] == 'train':
            augmented_rows_info = process_video(video_id)
            for aug_info in augmented_rows_info:
                new_row = row.to_dict()
                new_row['id'] = aug_info['id']
                # new_row['filename'] = f"{aug_info['id']}.mp4" # Optional: update filename if needed
                new_metadata.append(new_row)
        else:
            # For val/test sets, just copy the files and metadata
            for input_dir, output_dir in [(HANDS_INPUT_DIR, HANDS_OUTPUT_DIR), (HOLISTIC_INPUT_DIR, HOLISTIC_OUTPUT_DIR)]:
                src = os.path.join(input_dir, f"{video_id}.npy")
                dst = os.path.join(output_dir, f"{video_id}.npy")
                if os.path.exists(src):
                    os.system(f'cp "{src}" "{dst}"') # Using cp for efficiency
            
            new_metadata.append(row.to_dict())
            
    # Create and save the new augmented metadata file
    augmented_df = pd.DataFrame(new_metadata)
    augmented_df.to_csv(AUGMENTED_METADATA_FILE, index=False)

    print("\n--- Augmentation Complete ---")
    print(f"Original dataset size: {len(df)}")
    print(f"Augmented dataset size: {len(augmented_df)}")
    print(f"New metadata saved to '{AUGMENTED_METADATA_FILE}'")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate augmented landmark data for sign language recognition.")
    parser.add_argument(
        "--mode", 
        type=str, 
        choices=['test', 'full'], 
        default='test',
        help="Run in 'test' mode on a small subset or 'full' mode on the entire dataset."
    )
    args = parser.parse_args()
    main(args.mode)