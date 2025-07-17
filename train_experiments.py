import os
import gc
import csv
import math
import random
import numpy as np
import pandas as pd
import tensorflow as tf
from pathlib import Path
from tensorflow.keras.models import Sequential, load_model
from tensorflow.keras.layers import Input, LSTM, Bidirectional, Dense, Dropout, Masking, BatchNormalization
from tensorflow.keras.utils import Sequence
from tensorflow.keras.callbacks import ModelCheckpoint, EarlyStopping, ReduceLROnPlateau
from sklearn.preprocessing import LabelEncoder

# ==============================================================================
# 1. GPU AND ENVIRONMENT CONFIGURATION
# ==============================================================================
# Set the specific GPU to use (before TensorFlow initializes)
# This is the most reliable way to select a specific GPU.
os.environ["CUDA_VISIBLE_DEVICES"] = "2"

gpus = tf.config.experimental.list_physical_devices('GPU')
if gpus:
    try:
        # Set memory growth to avoid allocating all memory at once
        tf.config.experimental.set_memory_growth(gpus[0], True)
        print(f"--- GPU Configuration ---")
        print(f"Configured to use GPU: {gpus[0].name}")
        print(f"Visible devices: {tf.config.get_visible_devices('GPU')}")
        print("-------------------------\n")
    except RuntimeError as e:
        print(f"GPU configuration error: {e}")
else:
    print("No GPUs found. Running on CPU.")


# ==============================================================================
# 2. EXPERIMENT DEFINITIONS
# ==============================================================================
TOP_N_CLASSES = 10
RESULTS_FILE = 'training_results.csv'

# Define the four experiments we will run
EXPERIMENTS = [
    {
        "name": "hands_original",
        "landmarks_dir": "hands_landmarks_original",
        "metadata_file": "splitted_asl.csv",
        "components": ["hand"],
        "use_augmented_data": False,
        "model_path": "models/lstm_hands_original.h5"
    },
    {
        "name": "hands_augmented",
        "landmarks_dir": "hands_landmarks_augmented_v1",
        "metadata_file": "augmented_asl_v1.csv",
        "components": ["hand"],
        "use_augmented_data": True,
        "model_path": "models/lstm_hands_augmented.h5"
    },
    {
        "name": "holistic_original",
        "landmarks_dir": "holistic_landmarks_original",
        "metadata_file": "splitted_asl.csv",
        "components": ["pose", "left_hand", "right_hand"], # NOTE: 'face' is excluded as requested
        "use_augmented_data": False,
        "model_path": "models/lstm_holistic_original.h5"
    },
    {
        "name": "holistic_augmented",
        "landmarks_dir": "holistic_landmarks_augmented_v1",
        "metadata_file": "augmented_asl_v1.csv",
        "components": ["pose", "left_hand", "right_hand"], # NOTE: 'face' is excluded as requested
        "use_augmented_data": True,
        "model_path": "models/lstm_holistic_augmented.h5"
    }
]

# Landmark constants for feature extraction
LANDMARK_MAP = {
    "pose": 33,
    "left_hand": 21,
    "right_hand": 21,
    "hand": 21  # Generic key for hands-only data
}
COORDINATES = 3 # x, y, z


# ==============================================================================
# 3. DATA HANDLING AND FEATURE EXTRACTION
# ==============================================================================

def get_feature_vector(frame_data, components):
    """
    Extracts specified landmark components into a single flat vector.
    Handles both 'holistic' (dict) and 'hands' (list of dicts) data structures.
    """
    feature_vector = []
    for component in components:
        num_landmarks = LANDMARK_MAP[component]
        expected_size = num_landmarks * COORDINATES
        landmarks = None

        if isinstance(frame_data, dict): # Holistic data
            landmarks = frame_data.get(component)
        elif isinstance(frame_data, list): # Hands data
            if component == 'hand':
                # Iterate through all detected hands in the frame and take the first valid one.
                for hand_info in frame_data:
                    if 'landmarks' in hand_info and isinstance(hand_info.get('landmarks'), np.ndarray):
                        landmarks = hand_info['landmarks']
                        break # Found a valid hand, stop looking

        if isinstance(landmarks, np.ndarray) and landmarks.shape == (num_landmarks, COORDINATES):
            feature_vector.append(landmarks.flatten())
        else:
            feature_vector.append(np.zeros(expected_size, dtype=np.float32))

    return np.concatenate(feature_vector)


class SignLanguageGenerator(Sequence):
    """Keras Sequence to load and process landmark data batch by batch."""
    def __init__(self, df, config, label_map, batch_size=32, shuffle=True):
        self.df = df
        self.config = config
        self.label_map = label_map
        self.batch_size = batch_size
        self.shuffle = shuffle
        self.landmarks_dir = Path(config["landmarks_dir"])
        self.components = config["components"]
        self.feature_dim = sum(LANDMARK_MAP[c] for c in self.components) * COORDINATES
        self.indices = self.df.index.tolist()
        self.on_epoch_end()

    def __len__(self):
        return math.ceil(len(self.df) / self.batch_size)

    def __getitem__(self, index):
        batch_indices = self.indices[index * self.batch_size:(index + 1) * self.batch_size]
        
        X_batch_list = []
        y_batch_list = []

        for i in batch_indices:
            row = self.df.loc[i]
            video_id = str(row['id'])
            label_id = self.label_map[row['category']]
            
            landmark_path = self.landmarks_dir / f"{video_id}.npy"
            if not landmark_path.exists():
                continue

            try:
                video_landmarks = np.load(landmark_path, allow_pickle=True)
                if video_landmarks.size == 0:
                    continue

                processed_vectors = []
                last_valid_vector = None 

                for frame in video_landmarks:
                    current_vector = get_feature_vector(frame, self.components)
                    is_valid = not np.all(current_vector == 0)

                    if last_valid_vector is None:
                        if is_valid:
                            processed_vectors.append(current_vector)
                            last_valid_vector = current_vector
                    else:
                        if is_valid:
                            processed_vectors.append(current_vector)
                            last_valid_vector = current_vector
                        else:
                            processed_vectors.append(last_valid_vector)
                
                if processed_vectors:
                    X_batch_list.append(np.array(processed_vectors, dtype=np.float32))
                    y_batch_list.append(label_id)

            except Exception as e:
                # print(f"Warning: Error processing {video_id}: {e}")
                continue
        
    # If the batch is completely empty after trying all files, return a batch with 0 samples
        # but a valid (non-zero) time dimension to prevent LSTM errors.
        if not X_batch_list:
             return np.zeros((0, 1, self.feature_dim)), np.zeros((0,))

        X_padded = tf.keras.preprocessing.sequence.pad_sequences(
            X_batch_list, dtype='float32', padding='post', truncating='post'
        )
        y_batch = np.array(y_batch_list, dtype=np.int64)

        return X_padded, y_batch

    def on_epoch_end(self):
        if self.shuffle:
            np.random.shuffle(self.indices)


# ==============================================================================
# 4. MODEL DEFINITION
# ==============================================================================

def build_lstm_model(input_shape, num_classes):
    """Builds and compiles a standard Bidirectional LSTM model."""
    model = Sequential([
        Input(shape=input_shape),
        Masking(mask_value=0.0),
        Bidirectional(LSTM(96, return_sequences=True)),
        Dropout(0.5),
        BatchNormalization(),
        Bidirectional(LSTM(48, return_sequences=False)),
        Dropout(0.5),
        BatchNormalization(),
        Dense(64, activation='relu'),
        Dropout(0.3),
        Dense(num_classes, activation='softmax')
    ])
    
    model.compile(
        optimizer='adam',
        loss='sparse_categorical_crossentropy',
        metrics=['accuracy']
    )
    return model


# ==============================================================================
# 5. EXPERIMENT RUNNER
# ==============================================================================

def run_experiment(config, top_n_classes_list):
    """
    Runs a single training and evaluation experiment based on a configuration.
    """
    print(f"\n{'='*25}")
    print(f"  STARTING EXPERIMENT: {config['name']}  ")
    print(f"{'='*25}\n")

    # --- 1. Load and Prepare Data ---
    print("Step 1: Loading and preparing data...")
    df = pd.read_csv(config['metadata_file'])
    
    # Filter for the pre-defined top N classes to ensure a fair comparison
    df = df[df['category'].isin(top_n_classes_list)].reset_index(drop=True)

    # For augmented data, we need to merge with original for train/val/test splits
    if config['use_augmented_data']:
        original_df = pd.read_csv('splitted_asl.csv')
        original_df = original_df[original_df['category'].isin(top_n_classes_list)]
        
        train_df = pd.concat([
            original_df[original_df['dataset_split'] == 'train'],
            df # The augmented df only contains 'train' samples
        ]).reset_index(drop=True)
        val_df = original_df[original_df['dataset_split'] == 'val'].reset_index(drop=True)
        test_df = original_df[original_df['dataset_split'] == 'test'].reset_index(drop=True)
    else:
        train_df = df[df['dataset_split'] == 'train'].reset_index(drop=True)
        val_df = df[df['dataset_split'] == 'val'].reset_index(drop=True)
        test_df = df[df['dataset_split'] == 'test'].reset_index(drop=True)
    
    label_encoder = LabelEncoder().fit(top_n_classes_list)
    label_map = {label: i for i, label in enumerate(label_encoder.classes_)}
    num_classes = len(label_map)
    
    print(f"  - Train samples: {len(train_df)}")
    print(f"  - Validation samples: {len(val_df)}")
    print(f"  - Test samples: {len(test_df)}")

    # --- 2. Create Data Generators ---
    print("\nStep 2: Creating data generators...")
    train_gen = SignLanguageGenerator(train_df, config, label_map, shuffle=True)
    val_gen = SignLanguageGenerator(val_df, config, label_map, shuffle=False)
    test_gen = SignLanguageGenerator(test_df, config, label_map, shuffle=False)

    # --- 3. Build Model ---
    print("\nStep 3: Building model...")
    input_shape = (None, train_gen.feature_dim)
    model = build_lstm_model(input_shape, num_classes)
    model.summary()
    
    # --- 4. Train Model ---
    print("\nStep 4: Training model...")
    os.makedirs(os.path.dirname(config['model_path']), exist_ok=True)
    
    callbacks = [
        ModelCheckpoint(filepath=config['model_path'], monitor='val_accuracy', save_best_only=True, mode='max', verbose=1),
        EarlyStopping(monitor='val_accuracy', patience=15, mode='max', verbose=1, restore_best_weights=True),
        ReduceLROnPlateau(monitor='val_loss', factor=0.2, patience=5, min_lr=1e-6, verbose=1)
    ]

    history = model.fit(
        train_gen,
        validation_data=val_gen,
        epochs=100,
        callbacks=callbacks,
        verbose=1
    )

    # --- 5. Evaluate Model ---
    print("\nStep 5: Evaluating model on test set...")
    # The best model is already loaded thanks to restore_best_weights=True
    test_loss, test_accuracy = model.evaluate(test_gen, verbose=1)
    best_val_accuracy = max(history.history['val_accuracy'])
    
    print(f"\n--- Experiment '{config['name']}' Complete ---")
    print(f"  - Best Validation Accuracy: {best_val_accuracy:.4f}")
    print(f"  - Final Test Accuracy:      {test_accuracy:.4f}")
    print(f"  - Final Test Loss:          {test_loss:.4f}")

    # Clean up to free GPU memory for the next run
    del model, train_gen, val_gen, test_gen, history
    tf.keras.backend.clear_session()
    gc.collect()

    return {
        "experiment_name": config['name'],
        "test_loss": test_loss,
        "test_accuracy": test_accuracy,
        "best_val_accuracy": best_val_accuracy
    }

# ==============================================================================
# 6. MAIN EXECUTION
# ==============================================================================

if __name__ == "__main__":
    # --- Determine Top N classes ONCE from the original data ---
    print(f"Identifying Top {TOP_N_CLASSES} classes for all experiments...")
    full_df = pd.read_csv('splitted_asl.csv')
    top_n_classes_list = full_df['category'].value_counts().nlargest(TOP_N_CLASSES).index.tolist()
    print(f"Top classes: {top_n_classes_list}\n")

    # --- Setup Results File ---
    file_exists = os.path.isfile(RESULTS_FILE)
    with open(RESULTS_FILE, 'a', newline='') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["experiment_name", "test_loss", "test_accuracy", "best_val_accuracy"])

    # --- Run All Experiments ---
    all_results = []
    for experiment_config in EXPERIMENTS:
        result = run_experiment(experiment_config, top_n_classes_list)
        all_results.append(result)

        # Append result to CSV immediately after each run
        with open(RESULTS_FILE, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([result['experiment_name'], result['test_loss'], result['test_accuracy'], result['best_val_accuracy']])

    # --- Display Final Summary ---
    print("\n\n========================================")
    print("      ALL EXPERIMENTS COMPLETE      ")
    print("========================================")
    results_df = pd.DataFrame(all_results)
    print(results_df.to_string(index=False))
    print(f"\nResults have been saved to '{RESULTS_FILE}'")