# train_handmade.py

import os
import gc
import math
import numpy as np
import pandas as pd
import tensorflow as tf
from pathlib import Path
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Input, LSTM, Bidirectional, Dense, Dropout, Masking, BatchNormalization
from tensorflow.keras.utils import Sequence
from tensorflow.keras.callbacks import ModelCheckpoint, EarlyStopping, ReduceLROnPlateau

# ==============================================================================
# 1. GPU AND ENVIRONMENT CONFIGURATION
# ==============================================================================
os.environ["CUDA_VISIBLE_DEVICES"] = "2"
gpus = tf.config.experimental.list_physical_devices('GPU')
if gpus:
    try:
        tf.config.experimental.set_memory_growth(gpus[0], True)
        print(f"--- Using GPU: {gpus[0].name} with Memory Growth enabled ---\n")
    except RuntimeError as e:
        print(f"GPU configuration error: {e}")
else:
    print("No GPUs found. Running on CPU.")

# ==============================================================================
# 2. CONFIGURATION
# ==============================================================================
BASE_DIR = Path('/home/pandu/Documents/explore/sign-language-translator-ai')

# --- DATA ---
# Point to the original and augmented metadata files
METADATA_ORIGINAL_PATH = BASE_DIR / "handmade_metadata.csv"
METADATA_AUGMENTED_PATH = BASE_DIR / "handmade_augmented_metadata.csv"

# Point to the corresponding landmark directories
LANDMARKS_ORIGINAL_DIR = BASE_DIR / "landmarks_handmade" / "hands"
LANDMARKS_AUGMENTED_DIR = BASE_DIR / "landmarks_handmade_augmented" / "hands"

# --- MODEL & TRAINING ---
# All 10 classes from your handmade dataset will be used.
NUM_CLASSES = 10 
BATCH_SIZE = 32
EPOCHS = 150
PATIENCE = 20 # For EarlyStopping

# --- OUTPUTS ---
MODEL_SAVE_DIR = BASE_DIR / "models"
MODEL_NAME = f"model_handmade_hands_{NUM_CLASSES}cls.h5"
MODEL_SAVE_PATH = MODEL_SAVE_DIR / MODEL_NAME
LABEL_MAP_PATH = MODEL_SAVE_DIR / f"label_map_handmade_{NUM_CLASSES}cls.npy"

# ==============================================================================
# 3. DATA PREPARATION
# ==============================================================================

def prepare_data():
    """
    Loads metadata, splits it into train/val/test sets, and combines with augmented data.
    Ensures that augmentations of a video stay in the same split as the original.
    """
    print("--- Step 1: Preparing Data ---")
    
    if not METADATA_ORIGINAL_PATH.exists() or not METADATA_AUGMENTED_PATH.exists():
        raise FileNotFoundError("Metadata files not found. Please run generation and augmentation scripts.")

    df_orig = pd.read_csv(METADATA_ORIGINAL_PATH)
    df_aug = pd.read_csv(METADATA_AUGMENTED_PATH)
    
    # 1. Encode labels
    label_encoder = LabelEncoder()
    df_orig['label_id'] = label_encoder.fit_transform(df_orig['category'])
    print(f"Found {len(label_encoder.classes_)} classes: {label_encoder.classes_}")
    
    # Save the label mapping
    MODEL_SAVE_DIR.mkdir(exist_ok=True)
    np.save(LABEL_MAP_PATH, label_encoder.classes_)
    print(f"Label map saved to {LABEL_MAP_PATH}")

    # 2. Split the ORIGINAL data first
    train_val_df, test_df = train_test_split(
        df_orig, test_size=0.15, random_state=42, stratify=df_orig['category']
    )
    train_df, val_df = train_test_split(
        train_val_df, test_size=0.15, random_state=42, stratify=train_val_df['category']
    )

    # 3. Combine with augmented data
    # Find which augmented samples belong to the training set
    train_ids = set(train_df['id'])
    train_aug_df = df_aug[df_aug['original_id'].isin(train_ids)].copy()
    
    # Add label_id to augmented data
    train_aug_df = train_aug_df.merge(df_orig[['id', 'label_id']], left_on='original_id', right_on='id', suffixes=('', '_y'))
    train_aug_df = train_aug_df.drop(columns=['id_y'])
    
    # Add a 'source' column to know which data generator to use
    train_df['source'] = 'original'
    val_df['source'] = 'original'
    test_df['source'] = 'original'
    train_aug_df['source'] = 'augmented'
    
    # Final combined training set
    final_train_df = pd.concat([train_df, train_aug_df], ignore_index=True)
    
    print("\nDataset splits:")
    print(f"  - Training samples:   {len(final_train_df)} ({len(train_df)} original + {len(train_aug_df)} augmented)")
    print(f"  - Validation samples: {len(val_df)}")
    print(f"  - Test samples:       {len(test_df)}")
    
    return final_train_df, val_df, test_df, len(label_encoder.classes_)


# ==============================================================================
# 4. DATA GENERATOR (Keras Sequence)
# ==============================================================================
FEATURE_DIM = 21 * 3  # Hands-only: 21 landmarks * 3 coordinates

class SignLanguageGenerator(Sequence):
    def __init__(self, df, batch_size, shuffle=True):
        self.df = df
        self.batch_size = batch_size
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
            # Determine which directory to load from based on the 'source'
            if row['source'] == 'original':
                base_path = LANDMARKS_ORIGINAL_DIR
            else: # 'augmented'
                base_path = LANDMARKS_AUGMENTED_DIR
                
            landmark_path = base_path / row['category'] / f"{row['id']}.npy"

            if not landmark_path.exists(): continue

            try:
                sequence_data = np.load(landmark_path, allow_pickle=True)
                if len(sequence_data) == 0: continue
                
                # Simplified feature extraction for hands-only data
                frame_vectors = []
                for frame in sequence_data:
                    if frame and 'landmarks' in frame[0] and frame[0]['landmarks'] is not None:
                        frame_vectors.append(frame[0]['landmarks'].flatten())
                    else: # Append zero vector if a frame is empty
                        frame_vectors.append(np.zeros(FEATURE_DIM, dtype=np.float32))

                if frame_vectors:
                    X_batch_list.append(np.array(frame_vectors, dtype=np.float32))
                    y_batch_list.append(row['label_id'])
            except Exception:
                continue
        
        if not X_batch_list: # Failsafe for an entirely empty batch
            return np.zeros((0, 1, FEATURE_DIM)), np.zeros((0,))

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
# 6. MAIN EXECUTION
# ==============================================================================

if __name__ == "__main__":
    # --- 1. Prepare Data ---
    train_df, val_df, test_df, num_classes = prepare_data()
    
    # --- 2. Create Data Generators ---
    print("\n--- Step 2: Creating Data Generators ---")
    train_gen = SignLanguageGenerator(train_df, BATCH_SIZE, shuffle=True)
    val_gen = SignLanguageGenerator(val_df, BATCH_SIZE, shuffle=False)
    test_gen = SignLanguageGenerator(test_df, BATCH_SIZE, shuffle=False)

    # --- 3. Build Model ---
    print("\n--- Step 3: Building Model ---")
    input_shape = (None, FEATURE_DIM)
    model = build_model(input_shape, num_classes)
    model.summary()

    # --- 4. Train Model ---
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
    
    # --- 5. Evaluate Final Model ---
    print("\n--- Step 5: Evaluating on Test Set ---")
    # Best model is already restored by EarlyStopping
    test_loss, test_accuracy = model.evaluate(test_gen, verbose=1)
    
    print("\n--- TRAINING COMPLETE ---")
    print(f"Final Test Loss:     {test_loss:.4f}")
    print(f"Final Test Accuracy: {test_accuracy:.4f}")
    print(f"Best model saved to: {MODEL_SAVE_PATH}")