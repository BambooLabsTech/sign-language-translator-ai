import os
import gc
import csv
import math
import random
import numpy as np
import pandas as pd
from tqdm import tqdm
from pathlib import Path
import cv2  # OpenCV for 3D rotation
from scipy.interpolate import interp1d

# ==============================================================================
# 1. CONFIGURATION
# ==============================================================================
BASE_DIR = Path.cwd()
INPUT_CSV_PATH = BASE_DIR / "splitted_asl.csv"
INPUT_HOLISTIC_DIR = BASE_DIR / "holistic_landmarks_original"
INPUT_HANDS_DIR = BASE_DIR / "hands_landmarks_original"
OUTPUT_HOLISTIC_DIR = BASE_DIR / "holistic_landmarks_augmented_v1"
OUTPUT_HANDS_DIR = BASE_DIR / "hands_landmarks_augmented_v1"
OUTPUT_CSV_PATH = BASE_DIR / "augmented_asl_v1.csv"

TOP_N_CLASSES = 10
AUGMENTATION_MULTIPLIER = 15

# --- RELIABLE AUGMENTATION PARAMETERS ---
GEOMETRIC_PROB = 0.8
ROTATION_RANGE_DEG = ((-10, 10), (-10, 10), (-15, 15))
SCALE_RANGE = (0.8, 1.2)
TRANSLATION_RANGE = (-0.1, 0.1)

HAND_SWAP_PROB = 0.5
NOISE_PROB = 0.4
NOISE_STD_DEV = 0.002

# NOTE: Temporal Warp and Time Mask have been REMOVED as they were too destructive.

LANDMARK_MAP = {"pose": 33, "left_hand": 21, "right_hand": 21, "hand": 21}
COORDINATES = 3

# ==============================================================================
# 2. HELPER FUNCTIONS
# ==============================================================================

def load_and_graft_landmarks(sample_id: str):
    holistic_path = INPUT_HOLISTIC_DIR / f"{sample_id}.npy"
    hands_path = INPUT_HANDS_DIR / f"{sample_id}.npy"
    if not holistic_path.exists(): return None, False
    holistic_data = np.load(holistic_path, allow_pickle=True)
    hands_data = np.load(hands_path, allow_pickle=True) if hands_path.exists() else None
    repaired_sequence = []
    has_any_hand_data = False
    for i, frame_data in enumerate(holistic_data):
        new_frame = frame_data.copy()
        if new_frame.get('left_hand') is None and hands_data is not None and i < len(hands_data):
            for hand in hands_data[i]:
                if hand.get('handedness', '').lower() == 'left': new_frame['left_hand'] = hand.get('landmarks'); break
        if new_frame.get('right_hand') is None and hands_data is not None and i < len(hands_data):
            for hand in hands_data[i]:
                if hand.get('handedness', '').lower() == 'right': new_frame['right_hand'] = hand.get('landmarks'); break
        if new_frame.get('left_hand') is not None or new_frame.get('right_hand') is not None:
            has_any_hand_data = True
        repaired_sequence.append(new_frame)
    return repaired_sequence, has_any_hand_data

def interpolate_missing_landmarks(sequence: list) -> list:
    for key in ['left_hand', 'right_hand']:
        series = [frame.get(key) for frame in sequence]
        valid_indices = [i for i, v in enumerate(series) if v is not None and v.shape == LANDMARK_MAP[key]]
        if len(valid_indices) < 2: continue
        invalid_indices = [i for i, v in enumerate(series) if v is None or v.shape != LANDMARK_MAP[key]]
        valid_data = np.array([series[i] for i in valid_indices])
        interp_funcs = [interp1d(valid_indices, valid_data[:, p, d], kind='linear', bounds_error=False, fill_value="extrapolate") for p in range(LANDMARK_MAP[key][0]) for d in range(LANDMARK_MAP[key][1])]
        for i in invalid_indices:
            sequence[i][key] = np.array([f(i) for f in interp_funcs]).reshape(LANDMARK_MAP[key])
    return sequence

def apply_geometric_augmentations(sequence: list) -> list:
    rot_x, rot_y, rot_z = [np.deg2rad(random.uniform(*r)) for r in ROTATION_RANGE_DEG]
    scale = random.uniform(*SCALE_RANGE); trans_x, trans_y = [random.uniform(*TRANSLATION_RANGE) for _ in range(2)]
    rot_vec = np.array([rot_x, rot_y, rot_z]); rotation_matrix, _ = cv2.Rodrigues(rot_vec)
    anchor = next(((f['pose'][11] + f['pose'][12]) / 2 for f in sequence if f.get('pose') is not None), None)
    if anchor is None: return sequence
    for frame in sequence:
        for key in LANDMARK_MAP.keys():
            if frame.get(key) is not None:
                landmarks = frame[key]
                centered = landmarks - anchor; rotated = centered @ rotation_matrix.T
                scaled = rotated * scale; frame[key] = scaled + anchor
                frame[key][:, 0] += trans_x; frame[key][:, 1] += trans_y
    return sequence

def apply_hand_swap(sequence: list) -> list:
    for frame in sequence:
        for key in LANDMARK_MAP.keys():
            if frame.get(key) is not None: frame[key][:, 0] = 1.0 - frame[key][:, 0]
        frame['left_hand'], frame['right_hand'] = frame.get('right_hand'), frame.get('left_hand')
    return sequence

def apply_noise(sequence: list) -> list:
    for frame in sequence:
        for key in LANDMARK_MAP.keys():
            if frame.get(key) is not None: frame[key] += np.random.normal(0, NOISE_STD_DEV, frame[key].shape)
    return sequence

def is_sequence_valid(sequence, components):
    if not sequence or len(sequence) == 0: return False
    for frame in sequence:
        for component in components:
            landmarks = None;
            if component == 'hand':
                if isinstance(frame, list) and len(frame) > 0 and 'landmarks' in frame[0]: landmarks = frame[0]['landmarks']
            elif isinstance(frame, dict): landmarks = frame.get(component)
            if isinstance(landmarks, np.ndarray) and np.any(landmarks != 0): return True
    return False

# ==============================================================================
# 3. MAIN EXECUTION SCRIPT
# ==============================================================================
def main():
    print("--- Starting Sign Language Augmentation Pipeline (v4 - Bulletproof) ---")
    OUTPUT_HOLISTIC_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_HANDS_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(INPUT_CSV_PATH)
    top_classes = df['category'].value_counts().nlargest(TOP_N_CLASSES).index.tolist()
    train_df = df[(df['dataset_split'] == 'train') & (df['category'].isin(top_classes))].copy()
    print(f"Found {len(train_df)} training samples to augment.")

    augmented_data_list = []
    generated_count = 0
    discarded_count = 0

    for _, row in tqdm(train_df.iterrows(), total=len(train_df), desc="Augmenting Samples"):
        sample_id = str(row['id'])
        repaired_sequence, original_had_hands = load_and_graft_landmarks(sample_id)
        if repaired_sequence is None: continue
        repaired_sequence = interpolate_missing_landmarks(repaired_sequence)

        # Final check on the source data before augmentation
        if not is_sequence_valid(repaired_sequence, ["pose", "left_hand", "right_hand"]):
            discarded_count += AUGMENTATION_MULTIPLIER # Discard all potential augs for this bad source
            continue

        for i in range(AUGMENTATION_MULTIPLIER):
            aug_sequence = [frame.copy() for frame in repaired_sequence]
            
            # --- APPLYING ONLY RELIABLE AUGMENTATIONS ---
            if random.random() < GEOMETRIC_PROB: aug_sequence = apply_geometric_augmentations(aug_sequence)
            if random.random() < HAND_SWAP_PROB: aug_sequence = apply_hand_swap(aug_sequence)
            if random.random() < NOISE_PROB: aug_sequence = apply_noise(aug_sequence)
            
            aug_id = f"{sample_id}_aug_{i}"
            np.save(OUTPUT_HOLISTIC_DIR / f"{aug_id}.npy", np.array(aug_sequence, dtype=object))
            
            if original_had_hands:
                hands_only_sequence = []
                for frame in aug_sequence:
                    hands_in_frame = []
                    if frame.get('left_hand') is not None: hands_in_frame.append({'landmarks': frame['left_hand']})
                    if frame.get('right_hand') is not None: hands_in_frame.append({'landmarks': frame['right_hand']})
                    hands_only_sequence.append(hands_in_frame)
                np.save(OUTPUT_HANDS_DIR / f"{aug_id}.npy", np.array(hands_only_sequence, dtype=object))
            
            augmented_data_list.append({'id': aug_id, 'category': row['category'], 'dataset_split': 'train'})
            generated_count += 1
            
    pd.DataFrame(augmented_data_list).to_csv(OUTPUT_CSV_PATH, index=False)
    
    print("\n--- Augmentation Complete! ---")
    print(f"Successfully generated {generated_count} valid augmented samples.")
    print(f"Discarded {discarded_count} samples due to invalid source data.")
    print(f"New CSV created at: {OUTPUT_CSV_PATH}")

if __name__ == "__main__":
    main()