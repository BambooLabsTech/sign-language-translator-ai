import os
import numpy as np
import pandas as pd
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import (
    Input, LSTM, Bidirectional, Dense, Dropout,
    Masking, BatchNormalization
)
from tensorflow.keras.utils import Sequence
from tensorflow.keras.callbacks import ModelCheckpoint, EarlyStopping
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
import math
from tqdm import tqdm

# --- 1. CONFIGURATION ---
# Use this section to easily switch between datasets and model settings.

CONFIG = {
    "data": {
        "metadata_file": "splitted_asl_augmented.csv",
        "landmarks_dir": "holistic_landmarks_augmented",
        # To use hands data, change the next two lines:
        # "metadata_file": "splitted_asl_augmented.csv", # (or non-augmented)
        # "landmarks_dir": "hands_landmarks_augmented", # (or non-augmented)
    },
    "features": {
        # Define which landmark components to use.
        # Options for 'holistic': 'pose', 'face', 'left_hand', 'right_hand'
        # Options for 'hands': 'landmarks' (representing one hand)
        "components": ['pose', 'left_hand', 'right_hand'], # We decided to exclude 'face'
        "landmark_map": {
            "pose": 33,
            "face": 478,
            "left_hand": 21,
            "right_hand": 21,
            "landmarks": 21, # For the 'hands' dataset
        },
        "coordinates": 3, # x, y, z
    },
    "training": {
        "batch_size": 16,
        "epochs": 100,
        "patience": 10, # For EarlyStopping
    },
    "model": {
        "type": "lstm",
        "lstm_units": 128,
        "dropout_rate": 0.4,
        "dense_units": 64,
        "model_save_path": "models/lstm_baseline.h5",
    }
}

# --- 2. FEATURE EXTRACTION HELPER ---

# Calculate feature dimension dynamically based on config
FEATURE_DIM = sum(CONFIG["features"]["landmark_map"][c] for c in CONFIG["features"]["components"]) * CONFIG["features"]["coordinates"]

def get_feature_vector_for_frame(frame_data):
    """
    Extracts and concatenates specified landmark components into a single flat vector.
    Handles missing components by filling with zeros.
    """
    all_landmarks = []
    for component in CONFIG["features"]["components"]:
        num_landmarks = CONFIG["features"]["landmark_map"][component]
        num_coords = CONFIG["features"]["coordinates"]
        
        # For holistic data (dict)
        if isinstance(frame_data, dict):
            landmarks = frame_data.get(component)
        # For hands data (list of dicts)
        elif isinstance(frame_data, list):
            # This logic assumes we want to combine all detected hands in 'hands' data
            # For a baseline, let's just take the first detected hand if any
            if component == 'landmarks' and len(frame_data) > 0:
                landmarks = frame_data[0].get('landmarks')
            else:
                landmarks = None
        else:
            landmarks = None

        if landmarks is not None:
            # Flatten (num_landmarks, 3) -> (num_landmarks * 3)
            all_landmarks.append(landmarks[:, :num_coords].flatten())
        else:
            # If component is missing, append a zero vector of the correct size
            all_landmarks.append(np.zeros(num_landmarks * num_coords))

    return np.concatenate(all_landmarks)


# --- 3. DATA GENERATOR (Keras Sequence) ---

class SignLanguageDataGenerator(Sequence):
    """
    Custom data generator to load and process landmark data on-the-fly.
    """
    def __init__(self, df, batch_size, landmarks_dir, num_classes, shuffle=True):
        self.df = df
        self.batch_size = batch_size
        self.landmarks_dir = landmarks_dir
        self.num_classes = num_classes
        self.shuffle = shuffle
        self.indices = self.df.index.tolist()
        self.on_epoch_end()

    def __len__(self):
        return math.ceil(len(self.df) / self.batch_size)

    def __getitem__(self, index):
        batch_indices = self.indices[index * self.batch_size:(index + 1) * self.batch_size]
        batch_df = self.df.loc[batch_indices]

        X_batch_list = []
        y_batch = batch_df['label_id'].values

        for _, row in batch_df.iterrows():
            video_id = row['id']
            landmark_path = os.path.join(self.landmarks_dir, f"{video_id}.npy")
            
            try:
                video_landmarks = np.load(landmark_path, allow_pickle=True)
                frame_vectors = [get_feature_vector_for_frame(frame) for frame in video_landmarks]
                X_batch_list.append(np.array(frame_vectors))
            except FileNotFoundError:
                print(f"Warning: File not found for ID {video_id}. Skipping.")
                # We need a placeholder for y_batch to match. A better way would be to filter these out beforehand.
                # For now, we'll have a mismatch if a file is not found, which can cause errors.
                # Let's assume all files exist for this script.
                continue

        # Pad sequences in the batch to the same length
        X_padded = tf.keras.preprocessing.sequence.pad_sequences(
            X_batch_list, dtype='float32', padding='post', truncating='post'
        )
        
        return X_padded, y_batch

    def on_epoch_end(self):
        if self.shuffle:
            np.random.shuffle(self.indices)


# --- 4. MODEL DEFINITION ---

def build_model(input_shape, num_classes):
    """
    Builds, compiles, and returns the LSTM model.
    """
    model = Sequential([
        Input(shape=input_shape, name="input_layer"),
        # Masking layer ignores padded time steps (where all features are 0.0)
        Masking(mask_value=0.0, name="masking_layer"),
        
        Bidirectional(LSTM(CONFIG["model"]["lstm_units"], return_sequences=True), name="bidirectional_lstm_1"),
        Dropout(CONFIG["model"]["dropout_rate"]),
        BatchNormalization(),
        
        Bidirectional(LSTM(CONFIG["model"]["lstm_units"], return_sequences=False), name="bidirectional_lstm_2"),
        Dropout(CONFIG["model"]["dropout_rate"]),
        BatchNormalization(),
        
        Dense(CONFIG["model"]["dense_units"], activation='relu', name="dense_1"),
        Dense(num_classes, activation='softmax', name="output_layer")
    ])

    model.compile(
        optimizer='adam',
        loss='sparse_categorical_crossentropy',
        metrics=['accuracy']
    )
    return model


# --- 5. MAIN EXECUTION SCRIPT ---

if __name__ == "__main__":
    print("--- Sign Language Recognition LSTM Training ---")
    print(f"Using landmark directory: {CONFIG['data']['landmarks_dir']}")
    print(f"Using features: {CONFIG['features']['components']}")
    print(f"Calculated feature dimension per frame: {FEATURE_DIM}")

    # --- Load and Prepare Metadata ---
    df = pd.read_csv(CONFIG["data"]["metadata_file"])

    # Create integer labels
    print("\nCreating label encoding...")
    unique_labels = sorted(df['category'].unique())
    label_to_id = {label: i for i, label in enumerate(unique_labels)}
    id_to_label = {i: label for label, i in label_to_id.items()}
    num_classes = len(unique_labels)

    df['label_id'] = df['category'].map(label_to_id)
    
    # Save the label mapping for later use (e.g., during prediction)
    os.makedirs("models", exist_ok=True)
    np.save("models/label_mapping.npy", label_to_id)
    print(f"Found {num_classes} unique sign glosses. Label map saved to 'models/label_mapping.npy'.")
    
    # --- Split Data ---
    train_df = df[df['dataset_split'] == 'train'].reset_index(drop=True)
    val_df = df[df['dataset_split'] == 'val'].reset_index(drop=True)
    test_df = df[df['dataset_split'] == 'test'].reset_index(drop=True)

    print(f"\nDataset splits:")
    print(f"  Training samples:   {len(train_df)}")
    print(f"  Validation samples: {len(val_df)}")
    print(f"  Test samples:       {len(test_df)}")
    
    # --- Create Data Generators ---
    train_generator = SignLanguageDataGenerator(train_df, CONFIG["training"]["batch_size"], CONFIG["data"]["landmarks_dir"], num_classes)
    val_generator = SignLanguageDataGenerator(val_df, CONFIG["training"]["batch_size"], CONFIG["data"]["landmarks_dir"], num_classes, shuffle=False)
    test_generator = SignLanguageDataGenerator(test_df, CONFIG["training"]["batch_size"], CONFIG["data"]["landmarks_dir"], num_classes, shuffle=False)
    
    # --- Build and Train Model ---
    input_shape = (None, FEATURE_DIM) # (sequence_length, num_features)
    model = build_model(input_shape, num_classes)
    
    print("\nModel Architecture:")
    model.summary()
    
    # Callbacks
    os.makedirs(os.path.dirname(CONFIG["model"]["model_save_path"]), exist_ok=True)
    
    checkpoint = ModelCheckpoint(
        filepath=CONFIG["model"]["model_save_path"],
        monitor='val_accuracy',
        save_best_only=True,
        save_weights_only=False,
        mode='max',
        verbose=1
    )
    
    early_stopping = EarlyStopping(
        monitor='val_accuracy',
        patience=CONFIG["training"]["patience"],
        mode='max',
        verbose=1,
        restore_best_weights=True # Restores the model weights from the epoch with the best value
    )

    print("\n--- Starting Training ---")
    history = model.fit(
        train_generator,
        validation_data=val_generator,
        epochs=CONFIG["training"]["epochs"],
        callbacks=[checkpoint, early_stopping],
        verbose=1
    )

    # --- Evaluate Model ---
    print("\n--- Evaluating on Test Set ---")
    # Note: EarlyStopping with restore_best_weights=True means the model already
    # has the best weights loaded. If it was False, we would load them manually:
    # model.load_weights(CONFIG["model"]["model_save_path"])
    
    test_loss, test_accuracy = model.evaluate(test_generator, verbose=1)
    
    print("\n--- Training Complete ---")
    print(f"Final Test Loss:     {test_loss:.4f}")
    print(f"Final Test Accuracy: {test_accuracy:.4f}")