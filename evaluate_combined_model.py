import os
import numpy as np
import pandas as pd
import tensorflow as tf
from pathlib import Path
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import classification_report, accuracy_score
from tensorflow.keras.models import load_model
from tensorflow.keras.utils import Sequence
from tqdm import tqdm
import math

# ==============================================================================
# 1. GPU AND ENVIRONMENT CONFIGURATION
# ==============================================================================
BASE_DIR = Path('/home/kp/explore/sign-language-translator-ai') 
os.environ["CUDA_VISIBLE_DEVICES"] = "2"
gpus = tf.config.experimental.list_physical_devices('GPU')
if gpus:
    try:
        tf.config.experimental.set_memory_growth(gpus[0], True)
        print(f"--- Using GPU: {gpus[0].name} with Memory Growth enabled ---\n")
    except RuntimeError as e: print(f"GPU configuration error: {e}")
else: print("No GPUs found. Running on CPU.")

# ==============================================================================
# 2. CONFIGURATION (Must match the training script)
# ==============================================================================
# --- Data Paths ---
COMBINED_DATASET_DIR = BASE_DIR / "dataset_combined_v1"
METADATA_PATH = COMBINED_DATASET_DIR / "combined_metadata_v1.csv"
LANDMARKS_DIR = COMBINED_DATASET_DIR / "landmarks_hands"

# --- Model & Label Map Paths ---
MODEL_SAVE_DIR = BASE_DIR / "models"
MODEL_NAME = "model_combined_hands_17cls_v1.h5" # Name of the SAVED model
MODEL_PATH = MODEL_SAVE_DIR / MODEL_NAME
LABEL_MAP_PATH = MODEL_SAVE_DIR / "label_map_combined_17cls.npy"

# --- Feature Extraction ---
FEATURE_DIM = 21 * 3
BATCH_SIZE = 32 # Use the same batch size for consistency

# The same generator class from your training script is needed here.
# ==============================================================================
# 4. DATA GENERATOR (Keras Sequence)
# ==============================================================================
class SignLanguageGenerator(Sequence):
    def __init__(self, df, landmarks_base_dir, batch_size, feature_dim, shuffle=False): # Shuffle is OFF for evaluation
        self.df, self.landmarks_base_dir, self.batch_size, self.feature_dim, self.shuffle = df, landmarks_base_dir, batch_size, feature_dim, shuffle
        self.indices = self.df.index.tolist()
        self.on_epoch_end()
    def __len__(self): return math.ceil(len(self.df) / self.batch_size)
    def __getitem__(self, index):
        batch_indices = self.indices[index * self.batch_size:(index + 1) * self.batch_size]
        X_batch_list, y_batch_list, source_list = [], [], []
        for i in batch_indices:
            row = self.df.loc[i]
            landmark_path = self.landmarks_base_dir / row['category'] / f"{row['id']}.npy"
            if not landmark_path.exists(): continue
            try:
                sequence_data = np.load(landmark_path, allow_pickle=True)
                if len(sequence_data) == 0: continue
                all_vectors = []
                for frame in sequence_data:
                    landmarks = frame[0].get('landmarks') if isinstance(frame, list) and len(frame) > 0 and isinstance(frame[0], dict) else None
                    all_vectors.append(landmarks.flatten() if landmarks is not None and isinstance(landmarks, np.ndarray) else np.zeros(self.feature_dim, dtype=np.float32))
                if not np.any(all_vectors): continue
                last_valid_vector = np.zeros(self.feature_dim, dtype=np.float32)
                for i in range(len(all_vectors)):
                    if not np.all(all_vectors[i] == 0): last_valid_vector = all_vectors[i]
                    else: all_vectors[i] = last_valid_vector
                first_valid_idx = next((i for i, v in enumerate(all_vectors) if not np.all(v == 0)), 0)
                X_batch_list.append(np.array(all_vectors[first_valid_idx:], dtype=np.float32))
                y_batch_list.append(row['label_id'])
                source_list.append(row['source_dataset'])
            except Exception: continue
        if not X_batch_list: return np.zeros((0, 0, self.feature_dim)), np.zeros((0,)), np.array([])
        X_padded = tf.keras.preprocessing.sequence.pad_sequences(X_batch_list, dtype='float32', padding='post', truncating='post')
        # Return source as well to make evaluation easier
        return X_padded, np.array(y_batch_list, dtype=np.int64), np.array(source_list)
    def on_epoch_end(self): pass # No need to shuffle for evaluation

# ==============================================================================
# 5. MAIN EVALUATION SCRIPT
# ==============================================================================
def main():
    print("--- Starting Standalone Model Evaluation ---")
    
    # --- Step 1: Load Model and Metadata ---
    print(f"Loading model from: {MODEL_PATH}")
    if not MODEL_PATH.exists():
        print(f"FATAL: Model file not found. Please ensure the path is correct.")
        return
    model = load_model(MODEL_PATH)

    print(f"Loading label map from: {LABEL_MAP_PATH}")
    class_names = np.load(LABEL_MAP_PATH, allow_pickle=True)
    
    print(f"Loading metadata from: {METADATA_PATH}")
    df = pd.read_csv(METADATA_PATH)
    
    # Filter for the test set
    test_df = df[df['split'] == 'test'].reset_index(drop=True)
    
    # Re-create the label IDs to be safe
    label_encoder = LabelEncoder().fit(class_names)
    test_df['label_id'] = label_encoder.transform(test_df['category'])
    
    # --- Step 2: Create Test Generator ---
    print("Creating data generator for the test set...")
    test_gen = SignLanguageGenerator(test_df, LANDMARKS_DIR, BATCH_SIZE, FEATURE_DIM)

    # --- Step 3: Robust Prediction Loop ---
    print("Making predictions on test set (this may take a moment)...")
    all_y_true = []
    all_y_pred = []
    all_sources = []

    for i in tqdm(range(len(test_gen)), desc="Evaluating Batches"):
        X_batch, y_batch, source_batch = test_gen[i]
        
        # Skip if the generator produced an empty batch
        if X_batch.shape[0] == 0:
            continue
            
        y_pred_probs = model.predict(X_batch, verbose=0)
        y_pred_batch = np.argmax(y_pred_probs, axis=1)
        
        all_y_true.extend(y_batch)
        all_y_pred.extend(y_pred_batch)
        all_sources.extend(source_batch)

    # Convert to numpy arrays for easy filtering
    all_y_true = np.array(all_y_true)
    all_y_pred = np.array(all_y_pred)
    all_sources = np.array(all_sources)

    print(f"\nSuccessfully processed {len(all_y_true)} out of {len(test_df)} test samples.")

    # --- Step 4: Detailed Evaluation ---
    print("\n" + "="*60 + "\n---              IN-DEPTH TEST SET EVALUATION              ---\n" + "="*60)

    # 1. Overall Performance
    print("\n\n--- [1/3] Overall Performance (Mixed HQ + LQ Data) ---")
    print(f"Overall Accuracy: {accuracy_score(all_y_true, all_y_pred):.4f}")
    print(classification_report(all_y_true, all_y_pred, target_names=class_names, zero_division=0))

    # 2. High-Quality Performance
    hq_mask = (all_sources == 'handmade')
    if np.any(hq_mask):
        print("\n\n--- [2/3] Performance on High-Quality (Handmade) Data ONLY ---")
        print(f"HQ Test Accuracy: {accuracy_score(all_y_true[hq_mask], all_y_pred[hq_mask]):.4f}")
        print(classification_report(all_y_true[hq_mask], all_y_pred[hq_mask], target_names=class_names, zero_division=0))
    else:
        print("\nNo high-quality samples were processed in the test set.")
        
    # 3. Low-Quality Performance
    lq_mask = (all_sources == 'wlasl')
    if np.any(lq_mask):
        print("\n\n--- [3/3] Performance on Low-Quality (WLASL) Data ONLY ---")
        print(f"LQ Test Accuracy: {accuracy_score(all_y_true[lq_mask], all_y_pred[lq_mask]):.4f}")
        print(classification_report(all_y_true[lq_mask], all_y_pred[lq_mask], target_names=class_names, zero_division=0))
    else:
        print("\nNo low-quality samples were processed in the test set.")

    print("="*60 + "\n---                 EVALUATION SCRIPT COMPLETE               ---\n" + "="*60)


if __name__ == "__main__":
    main()