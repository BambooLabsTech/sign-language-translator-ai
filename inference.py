import os
import numpy as np
import tensorflow as tf
from pathlib import Path
import mediapipe as mp
import cv2
import argparse

# ==============================================================================
# 1. CONFIGURATION
# ==============================================================================
# --- Paths ---
# These paths should point to your trained model and its corresponding label map.
# Update them if they are in a different location.
BASE_DIR = Path.cwd() # Assumes script is in the main project directory
MODEL_DIR = BASE_DIR / "models"
MODEL_PATH = MODEL_DIR / "model_combined_hands_17cls_v1.h5"
LABEL_MAP_PATH = MODEL_DIR / "label_map_combined_17cls.npy"

# --- Feature Extraction ---
# This must match the feature dimension used during training.
FEATURE_DIM = 21 * 3

# --- MediaPipe Hands Model ---
mp_hands = mp.solutions.hands

# ==============================================================================
# 2. CORE FUNCTIONS
# ==============================================================================

def process_video_to_landmarks(video_path: Path) -> list:
    """
    Processes a video file using MediaPipe Hands and extracts landmark sequences.
    
    Args:
        video_path: The path to the video file.
        
    Returns:
        A list of flattened landmark vectors, one for each frame.
    """
    print(f"Processing video: {video_path.name}...")
    
    # We use max_num_hands=1 for isolated sign inference, assuming one dominant hand.
    hands_model = mp_hands.Hands(
        static_image_mode=False,
        max_num_hands=1,
        min_detection_confidence=0.5,
        min_tracking_confidence=0.5
    )
    
    cap = cv2.VideoCapture(str(video_path))
    if not cap.isOpened():
        print("Error: Could not open video file.")
        return []
        
    all_frame_vectors = []
    
    while cap.isOpened():
        ret, frame = cap.read()
        if not ret:
            break
            
        # Convert the BGR image to RGB and process it with MediaPipe
        image_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
        results = hands_model.process(image_rgb)
        
        # Extract landmarks if a hand is detected
        if results.multi_hand_landmarks:
            # Take the first detected hand
            hand_landmarks = results.multi_hand_landmarks[0]
            
            # Convert landmark objects to a NumPy array and flatten
            landmarks_arr = np.array([[lm.x, lm.y, lm.z] for lm in hand_landmarks.landmark]).flatten()
            all_frame_vectors.append(landmarks_arr)
        else:
            # If no hand is detected, append a zero vector
            all_frame_vectors.append(np.zeros(FEATURE_DIM, dtype=np.float32))
            
    cap.release()
    hands_model.close()
    
    print(f"-> Extracted landmarks from {len(all_frame_vectors)} frames.")
    return all_frame_vectors

def preprocess_sequence(sequence: list) -> np.ndarray:
    """
    Applies the same preprocessing (back-fill, front-trim) as the training generator.
    
    Args:
        sequence: A list of landmark vectors.
        
    Returns:
        A preprocessed NumPy array ready for the model.
    """
    if not sequence:
        return np.array([])
    
    # Back-fill logic: Fill missing frames (zeros) with the last known good frame
    last_valid_vector = np.zeros(FEATURE_DIM, dtype=np.float32)
    for i in range(len(sequence)):
        if not np.all(sequence[i] == 0):
            last_valid_vector = sequence[i]
        else:
            sequence[i] = last_valid_vector
            
    # Front-trim logic: Remove any leading zero frames that couldn't be filled
    first_valid_idx = next((i for i, v in enumerate(sequence) if not np.all(v == 0)), 0)
    
    return np.array(sequence[first_valid_idx:], dtype=np.float32)


# ==============================================================================
# 3. MAIN INFERENCE SCRIPT
# ==============================================================================
if __name__ == "__main__":
    # Setup command-line argument parser
    parser = argparse.ArgumentParser(description="Sign Language Recognition Inference Script")
    parser.add_argument("video_path", type=str, help="Path to the input video file (e.g., 'hello.mp4').")
    args = parser.parse_args()
    
    video_file = Path(args.video_path)
    
    # --- Step 1: Check if all necessary files exist ---
    if not video_file.exists():
        print(f"Error: Video file not found at '{video_file}'")
        exit()
    if not MODEL_PATH.exists():
        print(f"Error: Model file not found at '{MODEL_PATH}'")
        exit()
    if not LABEL_MAP_PATH.exists():
        print(f"Error: Label map file not found at '{LABEL_MAP_PATH}'")
        exit()
        
    # --- Step 2: Load Model and Labels ---
    print("\n--- Loading Model and Labels ---")
    model = tf.keras.models.load_model(MODEL_PATH)
    label_map = np.load(LABEL_MAP_PATH, allow_pickle=True)
    print(f"Model '{MODEL_PATH.name}' loaded successfully.")
    print(f"Loaded {len(label_map)} class labels.")
    
    # --- Step 3: Process Video and Extract Landmarks ---
    print("\n--- Processing Video ---")
    landmark_sequence = process_video_to_landmarks(video_file)
    
    if not landmark_sequence or not np.any(landmark_sequence):
        print("\nCould not detect any hand landmarks in the video. Cannot make a prediction.")
        exit()
        
    # --- Step 4: Preprocess Landmarks and Predict ---
    print("\n--- Making Prediction ---")
    processed_sequence = preprocess_sequence(landmark_sequence)
    
    # Add a batch dimension: (num_frames, features) -> (1, num_frames, features)
    model_input = np.expand_dims(processed_sequence, axis=0)
    
    prediction = model.predict(model_input)
    
    # --- Step 5: Display Results ---
    predicted_class_index = np.argmax(prediction)
    prediction_confidence = np.max(prediction)
    predicted_class_label = label_map[predicted_class_index]
    
    print("\n" + "="*40)
    print("           INFERENCE RESULT")
    print("="*40)
    print(f"  Predicted Sign:    {predicted_class_label.upper()}")
    print(f"  Confidence:        {prediction_confidence:.2%}")
    print("="*40 + "\n")