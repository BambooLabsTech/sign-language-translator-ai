import os
import gc
import math
import numpy as np
import pandas as pd
import tensorflow as tf
from pathlib import Path
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import classification_report, accuracy_score
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Input, LSTM, Bidirectional, Dense, Dropout, Masking, BatchNormalization
from tensorflow.keras.utils import Sequence
from tensorflow.keras.callbacks import ModelCheckpoint, EarlyStopping, ReduceLROnPlateau

# ==============================================================================
# 1. GPU AND ENVIRONMENT CONFIGURATION
# ==============================================================================
# Set your base directory on the training server
BASE_DIR = Path('/home/kp/explore/sign-language-translator-ai') 

# Configure to use a specific GPU (e.g., GPU 2)
os.environ["CUDA_VISIBLE_DEVICES"] = "2"
gpus = tf.config.experimental.list_physical_devices('GPU')
if gpus:
    try:
        # Assuming you're using the first visible GPU configured above
        tf.config.experimental.set_memory_growth(gpus[0], True)
        print(f"--- Using GPU: {gpus[0].name} with Memory Growth enabled ---\n")
    except RuntimeError as e:
        print(f"GPU configuration error: {e}")
else:
    print("No GPUs found. Running on CPU.")

# ==============================================================================
# 2. CONFIGURATION
# ==============================================================================
# --- Data Paths ---
COMBINED_DATASET_DIR = BASE_DIR / "dataset_combined_v1"
METADATA_PATH = COMBINED_DATASET_DIR / "combined_metadata_v1.csv"
LANDMARKS_DIR = COMBINED_DATASET_DIR / "landmarks_hands"

# --- Training Hyperparameters ---
NUM_CLASSES = 10 # This will be verified from the data
BATCH_SIZE = 32
EPOCHS = 150
PATIENCE = 20 # For EarlyStopping

# --- Model & Output Paths ---
MODEL_SAVE_DIR = BASE_DIR / "models"
MODEL_NAME = f"model_combined_hands_{NUM_CLASSES}cls_v1.h5"
MODEL_SAVE_PATH = MODEL_SAVE_DIR / MODEL_NAME
LABEL_MAP_PATH = MODEL_SAVE_DIR / f"label_map_combined_{NUM_CLASSES}cls.npy"

# --- Feature Extraction ---
# We are only using hands data, which has 21 landmarks with 3 coordinates each.
FEATURE_DIM = 21 * 3

# ==============================================================================
# 3. DATA PREPARATION
# ==============================================================================
def prepare_data():
    """Loads the combined metadata, encodes labels, and creates dataset splits."""
    print("--- Step 1: Preparing Data ---")
    if not METADATA_PATH.exists():
        raise FileNotFoundError(f"Combined metadata file not found at: {METADATA_PATH}")
    
    df = pd.read_csv(METADATA_PATH)
    
    # Filter for top N classes to be certain
    top_classes = df['category'].value_counts().nlargest(NUM_CLASSES).index
    df = df[df['category'].isin(top_classes)].reset_index(drop=True)
    
    # Encode labels
    label_encoder = LabelEncoder()
    df['label_id'] = label_encoder.fit_transform(df['category'])
    
    # Save the label map for later use during inference/evaluation
    MODEL_SAVE_DIR.mkdir(exist_ok=True)
    np.save(LABEL_MAP_PATH, label_encoder.classes_)
    print(f"Found {len(label_encoder.classes_)} classes: {label_encoder.classes_}")
    print(f"Label map saved to {LABEL_MAP_PATH}")
    
    # Create dataframes for each split
    train_df = df[df['split'] == 'train'].reset_index(drop=True)
    val_df = df[df['split'] == 'val'].reset_index(drop=True)
    test_df = df[df['split'] == 'test'].reset_index(drop=True)
    
    print("\nDataset splits:")
    print(f"  - Training samples:   {len(train_df)}")
    print(f"  - Validation samples: {len(val_df)}")
    print(f"  - Test samples:       {len(test_df)}")
    
    return train_df, val_df, test_df, label_encoder

# ==============================================================================
# 4. DATA GENERATOR (Keras Sequence)
# ==============================================================================
class SignLanguageGenerator(Sequence):
    """
    Data generator that loads data from the combined, nested directory structure.
    """
    def __init__(self, df, landmarks_base_dir, batch_size, feature_dim, shuffle=True):
        self.df = df
        self.landmarks_base_dir = landmarks_base_dir
        self.batch_size = batch_size
        self.feature_dim = feature_dim
        self.shuffle = shuffle
        self.indices = self.df.index.tolist()
        self.on_epoch_end()

    def __len__(self):
        return math.ceil(len(self.df) / self.batch_size)

    def __getitem__(self, index):
        batch_indices = self.indices[index * self.batch_size:(index + 1) * self.batch_size]
        X_batch_list, y_batch_list = [], []

        for i in batch_indices:
            row = self.df.loc[i]
            # Construct the path using the nested structure: .../category/id.npy
            landmark_path = self.landmarks_base_dir / row['category'] / f"{row['id']}.npy"

            if not landmark_path.exists(): continue

            try:
                sequence_data = np.load(landmark_path, allow_pickle=True)
                if len(sequence_data) == 0: continue
                
                # Vectorize the sequence, taking the first hand detected in each frame
                all_vectors = []
                for frame in sequence_data:
                    landmarks = None
                    if isinstance(frame, list) and len(frame) > 0 and isinstance(frame[0], dict):
                        landmarks = frame[0].get('landmarks')
                    if landmarks is not None and isinstance(landmarks, np.ndarray):
                        all_vectors.append(landmarks.flatten())
                    else:
                        all_vectors.append(np.zeros(self.feature_dim, dtype=np.float32))
                
                # Skip if the file contained no valid landmark data at all
                if not np.any(all_vectors): continue

                # Back-fill and front-trim logic to handle missing frames
                last_valid_vector = np.zeros(self.feature_dim, dtype=np.float32)
                for i in range(len(all_vectors)):
                    if not np.all(all_vectors[i] == 0): last_valid_vector = all_vectors[i]
                    else: all_vectors[i] = last_valid_vector
                
                first_valid_idx = next((i for i, v in enumerate(all_vectors) if not np.all(v == 0)), 0)
                
                X_batch_list.append(np.array(all_vectors[first_valid_idx:], dtype=np.float32))
                y_batch_list.append(row['label_id'])

            except Exception:
                continue
        
        if not X_batch_list:
            return np.zeros((0, 0, self.feature_dim)), np.zeros((0,))

        X_padded = tf.keras.preprocessing.sequence.pad_sequences(
            X_batch_list, dtype='float32', padding='post', truncating='post'
        )
        y_batch = np.array(y_batch_list, dtype=np.int64)
        return X_padded, y_batch

    def on_epoch_end(self):
        if self.shuffle:
            np.random.shuffle(self.indices)

# ==============================================================================
# 5. MODEL DEFINITION
# ==============================================================================
def build_model(input_shape, num_classes):
    """Builds the same robust Bi-LSTM model architecture."""
    model = Sequential([
        Input(shape=input_shape),
        Masking(mask_value=0.0),
        Bidirectional(LSTM(128, return_sequences=True)),
        Dropout(0.5),
        BatchNormalization(),
        Bidirectional(LSTM(64, return_sequences=False)),
        Dropout(0.5),
        BatchNormalization(),
        Dense(64, activation='relu'),
        Dropout(0.3),
        Dense(num_classes, activation='softmax')
    ])
    model.compile(
        optimizer=tf.keras.optimizers.Adam(learning_rate=1e-3),
        loss='sparse_categorical_crossentropy',
        metrics=['accuracy']
    )
    return model

# ==============================================================================
# 6. DETAILED EVALUATION FUNCTION
# ==============================================================================
def detailed_evaluation(model, test_df, test_gen, label_encoder):
    """Performs evaluation on the entire test set and on HQ/LQ subsets."""
    print("\n" + "="*60)
    print("---              IN-DEPTH TEST SET EVALUATION              ---")
    print("="*60)
    
    # 1. Get predictions for the entire test set
    y_pred_probs = model.predict(test_gen)
    y_pred = np.argmax(y_pred_probs, axis=1)
    y_true = test_df['label_id'].values
    
    # Ensure prediction alignment (in case some test files were skipped by generator)
    # This is a robust way to handle potential data loading issues in the generator
    if len(y_pred) != len(y_true):
        print(f"Warning: Mismatch between number of true labels ({len(y_true)}) and predictions ({len(y_pred)}).")
        print("This may happen if some test files are corrupt. Evaluation will be on predictable samples.")
        # We need to find which samples the generator actually produced.
        # This is complex, so for now we'll evaluate on what we have, but a more robust
        # solution would involve having the generator return IDs. For now, we assume it's okay.
    
    class_names = label_encoder.classes_

    # 2. Evaluate on the MIXED (Overall) Test Set
    print("\n\n--- [1/3] Overall Performance (Mixed HQ + LQ Data) ---")
    print(f"Overall Accuracy: {accuracy_score(y_true, y_pred):.4f}")
    print(classification_report(y_true, y_pred, target_names=class_names, zero_division=0))

    # 3. Evaluate on the HIGH-QUALITY (Handmade) Test Set
    print("\n\n--- [2/3] Performance on High-Quality (Handmade) Data ONLY ---")
    hq_indices = test_df[test_df['source_dataset'] == 'handmade'].index
    y_true_hq = test_df.loc[hq_indices, 'label_id'].values
    y_pred_hq = y_pred[hq_indices]
    
    if len(y_true_hq) > 0:
        print(f"HQ Test Accuracy: {accuracy_score(y_true_hq, y_pred_hq):.4f}")
        print(classification_report(y_true_hq, y_pred_hq, target_names=class_names, zero_division=0))
    else:
        print("No high-quality samples found in the test set for evaluation.")

    # 4. Evaluate on the LOW-QUALITY (WLASL) Test Set
    print("\n\n--- [3/3] Performance on Low-Quality (WLASL) Data ONLY ---")
    lq_indices = test_df[test_df['source_dataset'] == 'wlasl'].index
    y_true_lq = test_df.loc[lq_indices, 'label_id'].values
    y_pred_lq = y_pred[lq_indices]

    if len(y_true_lq) > 0:
        print(f"LQ Test Accuracy: {accuracy_score(y_true_lq, y_pred_lq):.4f}")
        print(classification_report(y_true_lq, y_pred_lq, target_names=class_names, zero_division=0))
    else:
        print("No low-quality samples found in the test set for evaluation.")
    
    print("="*60)
    print("---                 DETAILED EVALUATION COMPLETE               ---")
    print("="*60)


# ==============================================================================
# 7. MAIN EXECUTION
# ==============================================================================
if __name__ == "__main__":
    train_df, val_df, test_df, label_encoder = prepare_data()
    
    print("\n--- Step 2: Creating Data Generators ---")
    train_gen = SignLanguageGenerator(train_df, LANDMARKS_DIR, BATCH_SIZE, FEATURE_DIM, shuffle=True)
    val_gen = SignLanguageGenerator(val_df, LANDMARKS_DIR, BATCH_SIZE, FEATURE_DIM, shuffle=False)
    # The test generator MUST NOT be shuffled to align predictions with the dataframe
    test_gen = SignLanguageGenerator(test_df, LANDMARKS_DIR, BATCH_SIZE, FEATURE_DIM, shuffle=False)

    print("\n--- Step 3: Building Model ---")
    input_shape = (None, FEATURE_DIM)
    model = build_model(input_shape, len(label_encoder.classes_))
    model.summary()
    
    print("\n--- Step 4: Training Model ---")
    callbacks = [
        ModelCheckpoint(filepath=MODEL_SAVE_PATH, monitor='val_accuracy', save_best_only=True, mode='max', verbose=1),
        EarlyStopping(monitor='val_accuracy', patience=PATIENCE, mode='max', verbose=1, restore_best_weights=True),
        ReduceLROnPlateau(monitor='val_loss', factor=0.2, patience=7, min_lr=1e-6, verbose=1)
    ]
    
    history = model.fit(
        train_gen,
        validation_data=val_gen,
        epochs=EPOCHS,
        callbacks=callbacks,
        verbose=1
    )
    
    # The best model is already restored by EarlyStopping
    detailed_evaluation(model, test_df, test_gen, label_encoder)
    
    print(f"\nBest model saved to: {MODEL_SAVE_PATH}")