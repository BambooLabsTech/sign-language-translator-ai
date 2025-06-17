import os
import numpy as np
import pandas as pd
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.callbacks import ModelCheckpoint, EarlyStopping, ReduceLROnPlateau
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

# --- GPU CONFIGURATION ---
# Configure TensorFlow to use specific GPU
def configure_gpu(gpu_id=2):
    """
    Configure TensorFlow to use a specific GPU with memory growth enabled.
    """
    gpus = tf.config.experimental.list_physical_devices('GPU')
    if gpus:
        try:
            # Restrict TensorFlow to only use the specified GPU
            tf.config.experimental.set_visible_devices(gpus[gpu_id], 'GPU')
            
            # Enable memory growth to avoid allocating all GPU memory at once
            tf.config.experimental.set_memory_growth(gpus[gpu_id], True)
            
            print(f"Configured to use GPU {gpu_id}: {gpus[gpu_id]}")
            print(f"Memory growth enabled for GPU {gpu_id}")
            
            # Verify GPU is available
            print("Available GPUs:", len(tf.config.experimental.list_physical_devices('GPU')))
            print("GPU devices:", tf.config.list_logical_devices('GPU'))
            
        except RuntimeError as e:
            print(f"GPU configuration error: {e}")
    else:
        print("No GPUs found. Running on CPU.")

# Call GPU configuration at the start
configure_gpu(gpu_id=2)  # Use GPU 2 (0-based indexing)

# --- 1. CONFIGURATION ---
# Use this section to easily switch between datasets and model settings.

TOP_N_CLASSES = 10
CONFIG = {
    "data": {
        "metadata_file": "data_v3/metadata_augmented_split.csv",
        "landmarks_dir": "data_v3/holistic_landmarks_augmented",
        # To use hands data, change the next two lines:
        #"metadata_file": "data_v3/metadata_augmented_split.csv", # (or non-augmented)
        #"landmarks_dir": "data_v3/hands_landmarks_augmented", # (or non-augmented)
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
        "top_n_classes": TOP_N_CLASSES,
        "batch_size": 32,
        "epochs": 100,
        "patience": 20, # For EarlyStopping
    },
    "model": {
        "type": "lstm",
        "lstm_units": 96,
        "dropout_rate": 0.5,
        "dense_units": 64,
        "model_save_path": f"models/lstm_baseline_{TOP_N_CLASSES}.h5",
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

        # Build X and y in parallel to avoid mismatches
        X_batch_list = []
        y_batch_list = []

        for _, row in batch_df.iterrows():
            video_id = row['id']
            label = row['label_id']
            landmark_path = os.path.join(self.landmarks_dir, f"{video_id}.npy")
            
            if not os.path.exists(landmark_path):
                # print(f"Warning: File not found for ID {video_id}. Skipping.")
                continue

            try:
                video_landmarks = np.load(landmark_path, allow_pickle=True)
                
                frame_vectors = []
                for frame in video_landmarks:
                    vec = get_feature_vector_for_frame(frame)
                    if np.any(vec):  # Only add frames with actual data
                        frame_vectors.append(vec)
                
                # Only use the video if it has valid frames after cleaning
                if frame_vectors:
                    X_batch_list.append(np.array(frame_vectors))
                    y_batch_list.append(label)
                # else:
                #     print(f"Warning: Video ID {video_id} has no valid frames. Skipping.")

            except Exception as e:
                # print(f"Error loading or processing {video_id}: {e}. Skipping.")
                continue

        # If the entire batch was invalid, return empty arrays
        if not X_batch_list:
            return np.array([]), np.array([])

        X_padded = tf.keras.preprocessing.sequence.pad_sequences(
            X_batch_list, dtype='float32', padding='post', truncating='post'
        )
        
        return X_padded, np.array(y_batch_list)

    def on_epoch_end(self):
        if self.shuffle:
            np.random.shuffle(self.indices)




# --- 4. MODEL DEFINITION ---

def build_model(input_shape, num_classes):
    """
    Builds, compiles, and returns the LSTM model.
    """
    with tf.device('/GPU:0'):
        model = Sequential([
            Input(shape=input_shape, name="input_layer"),
            Masking(mask_value=0.0, name="masking_layer"),
            
            # A slightly more complex first layer can help capture features
            Bidirectional(LSTM(CONFIG["model"]["lstm_units"], return_sequences=True), name="bidirectional_lstm_1"),
            Dropout(CONFIG["model"]["dropout_rate"]),
            BatchNormalization(),
            
            # A simpler second layer
            Bidirectional(LSTM(CONFIG["model"]["lstm_units"] // 2, return_sequences=False), name="bidirectional_lstm_2"),
            Dropout(CONFIG["model"]["dropout_rate"]),
            BatchNormalization(),
            
            Dense(CONFIG["model"]["dense_units"], activation='relu', name="dense_1"),
            Dropout(CONFIG["model"]["dropout_rate"] / 2), # Add dropout after dense layer
            Dense(num_classes, activation='softmax', name="output_layer")
        ])

        model.compile(
            optimizer='adam',
            loss='sparse_categorical_crossentropy',
            metrics=['accuracy']
        )
    return model


## --- 5. MAIN EXECUTION SCRIPT ---
if __name__ == "__main__":
    print("--- Sign Language Recognition LSTM Training ---")
    print(f"Using landmark directory: {CONFIG['data']['landmarks_dir']}")
    print(f"Using features: {CONFIG['features']['components']}")

    # --- Load and Prepare Metadata ---
    df = pd.read_csv(CONFIG["data"]["metadata_file"])
    
    # --- NEW FEATURE: Filter for Top N Classes ---
    top_n = CONFIG["training"]["top_n_classes"]
    if isinstance(top_n, int):
        print(f"\nFiltering dataset for the top {top_n} most frequent classes...")
        top_classes = df['category'].value_counts().nlargest(top_n).index.tolist()
        df = df[df['category'].isin(top_classes)].reset_index(drop=True)
        print(f"Training with {len(top_classes)} classes: {top_classes}")
    else:
        print("\nUsing full dataset...")

    # Create integer labels
    print("\nCreating label encoding...")
    unique_labels = sorted(df['category'].unique())
    label_to_id = {label: i for i, label in enumerate(unique_labels)}
    num_classes = len(unique_labels)
    df['label_id'] = df['category'].map(label_to_id)
    
    os.makedirs("models", exist_ok=True)
    np.save(CONFIG["model"]["model_save_path"].replace('.h5', '_label_mapping.npy'), label_to_id)
    print(f"Found {num_classes} unique sign glosses. Label map saved.")
    
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
    input_shape = (None, FEATURE_DIM)
    model = build_model(input_shape, num_classes)
    
    print("\nModel Architecture:")
    model.summary()
    
    # Callbacks
    os.makedirs(os.path.dirname(CONFIG["model"]["model_save_path"]), exist_ok=True)
    checkpoint = ModelCheckpoint(
        filepath=CONFIG["model"]["model_save_path"],
        monitor='val_accuracy', save_best_only=True, mode='max', verbose=1
    )
    early_stopping = EarlyStopping(
        monitor='val_accuracy', patience=CONFIG["training"]["patience"], mode='max',
        verbose=1, restore_best_weights=True
    )

    reduce_lr = ReduceLROnPlateau(
        monitor='val_accuracy',
        factor=0.2,  # Reduce learning rate by a factor of 5 (1.0 -> 0.2)
        patience=7,  # Reduce if val_accuracy doesn't improve for 3 epochs
        min_lr=1e-6, # Don't let the learning rate go too low
        verbose=1
    )

    print("\n--- Starting Training ---")
    history = model.fit(
        train_generator,
        validation_data=val_generator,
        epochs=CONFIG["training"]["epochs"],
        callbacks=[checkpoint, early_stopping, reduce_lr],
        verbose=1
    )

    # --- Evaluate Model ---
    print("\n--- Evaluating on Test Set ---")
    # --- MODIFIED: Load the best model before final evaluation ---
    print("Loading best model weights from checkpoint...")
    model.load_weights(CONFIG["model"]["model_save_path"])
    test_loss, test_accuracy = model.evaluate(test_generator, verbose=1)
    
    print("\n--- Training Complete ---")
    print(f"Final Test Loss:     {test_loss:.4f}")
    print(f"Final Test Accuracy: {test_accuracy:.4f}")