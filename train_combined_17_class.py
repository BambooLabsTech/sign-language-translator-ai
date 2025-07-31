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
# 2. CONFIGURATION (MODIFIED FOR 17-CLASS EXPERIMENT)
# ==============================================================================
# --- Data Paths ---
COMBINED_DATASET_DIR = BASE_DIR / "dataset_combined_v1"
METADATA_PATH = COMBINED_DATASET_DIR / "combined_metadata_v1.csv"
LANDMARKS_DIR = COMBINED_DATASET_DIR / "landmarks_hands"
LQ_METADATA_MASTER = BASE_DIR / "splitted_asl.csv" # Needed to find Top 10 LQ classes

# --- Class Selection ---
# Explicitly define the two sets of classes we want to combine
HANDMADE_CLASSES = ['what', 'me', 'hello', 'please', 'sorry', 'yes', 'no', 'where', 'you', 'thanks']
# We will dynamically find the Top 10 Low-Quality classes
TOP_N_LQ = 10 

# --- Training Hyperparameters ---
BATCH_SIZE = 32
EPOCHS = 150
PATIENCE = 20

# --- Model & Output Paths ---
MODEL_SAVE_DIR = BASE_DIR / "models"
MODEL_NAME = "model_combined_hands_17cls_v1.h5" # Updated name
MODEL_SAVE_PATH = MODEL_SAVE_DIR / MODEL_NAME
LABEL_MAP_PATH = MODEL_SAVE_DIR / "label_map_combined_17cls.npy" # Updated name

# --- Feature Extraction ---
FEATURE_DIM = 21 * 3

# ==============================================================================
# 3. DATA PREPARATION (MODIFIED)
# ==============================================================================
def prepare_data():
    """Loads metadata and filters for the 17-class set."""
    print("--- Step 1: Preparing Data for 17-Class Experiment ---")
    if not METADATA_PATH.exists() or not LQ_METADATA_MASTER.exists():
        raise FileNotFoundError("A required metadata file is missing.")
    
    df_combined = pd.read_csv(METADATA_PATH)
    df_lq_master = pd.read_csv(LQ_METADATA_MASTER)
    
    # --- *** NEW CLASS SELECTION LOGIC *** ---
    # 1. Get the Top 10 classes from the low-quality dataset
    lq_top_10_classes = df_lq_master['category'].value_counts().nlargest(TOP_N_LQ).index.tolist()
    
    # 2. Combine the two lists and find the unique set of classes
    final_class_list = sorted(list(set(HANDMADE_CLASSES + lq_top_10_classes)))
    num_final_classes = len(final_class_list)
    
    print(f"\nTargeting a total of {num_final_classes} unique classes.")
    print("Class list:", final_class_list)
    
    # 3. Filter the main combined dataframe to only include these classes
    df = df_combined[df_combined['category'].isin(final_class_list)].reset_index(drop=True)
    # --- *** END OF NEW LOGIC *** ---

    label_encoder = LabelEncoder()
    df['label_id'] = label_encoder.fit_transform(df['category'])
    
    MODEL_SAVE_DIR.mkdir(exist_ok=True)
    np.save(LABEL_MAP_PATH, label_encoder.classes_)
    print(f"\nLabel map for {len(label_encoder.classes_)} classes saved to {LABEL_MAP_PATH}")
    
    train_df = df[df['split'] == 'train'].reset_index(drop=True)
    val_df = df[df['split'] == 'val'].reset_index(drop=True)
    test_df = df[df['split'] == 'test'].reset_index(drop=True)
    
    print("\nDataset splits:")
    print(f"  - Training samples:   {len(train_df)}")
    print(f"  - Validation samples: {len(val_df)}")
    print(f"  - Test samples:       {len(test_df)}")
    
    return train_df, val_df, test_df, label_encoder

# Data Generator and Model Definition are UNCHANGED from your working script.
# They are included here for completeness.

# ==============================================================================
# 4. DATA GENERATOR (Keras Sequence) - UNCHANGED
# ==============================================================================
class SignLanguageGenerator(Sequence):
    def __init__(self, df, landmarks_base_dir, batch_size, feature_dim, shuffle=True):
        self.df, self.landmarks_base_dir, self.batch_size, self.feature_dim, self.shuffle = df, landmarks_base_dir, batch_size, feature_dim, shuffle
        self.indices = self.df.index.tolist()
        self.on_epoch_end()
    def __len__(self): return math.ceil(len(self.df) / self.batch_size)
    def __getitem__(self, index):
        batch_indices = self.indices[index * self.batch_size:(index + 1) * self.batch_size]
        X_batch_list, y_batch_list = [], []
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
            except Exception: continue
        if not X_batch_list: return np.zeros((0, 0, self.feature_dim)), np.zeros((0,))
        return tf.keras.preprocessing.sequence.pad_sequences(X_batch_list, dtype='float32', padding='post', truncating='post'), np.array(y_batch_list, dtype=np.int64)
    def on_epoch_end(self):
        if self.shuffle: np.random.shuffle(self.indices)

# ==============================================================================
# 5. MODEL DEFINITION - UNCHANGED
# ==============================================================================
def build_model(input_shape, num_classes):
    model = Sequential([
        Input(shape=input_shape), Masking(mask_value=0.0),
        Bidirectional(LSTM(128, return_sequences=True)), Dropout(0.5), BatchNormalization(),
        Bidirectional(LSTM(64, return_sequences=False)), Dropout(0.5), BatchNormalization(),
        Dense(64, activation='relu'), Dropout(0.3),
        Dense(num_classes, activation='softmax')
    ])
    model.compile(optimizer=tf.keras.optimizers.Adam(learning_rate=1e-3), loss='sparse_categorical_crossentropy', metrics=['accuracy'])
    return model

# ==============================================================================
# 6. DETAILED EVALUATION FUNCTION - UNCHANGED
# ==============================================================================
def detailed_evaluation(model, test_df, test_gen, label_encoder):
    print("\n" + "="*60 + "\n---              IN-DEPTH TEST SET EVALUATION              ---\n" + "="*60)
    y_pred_probs, y_true = model.predict(test_gen), test_df['label_id'].values
    y_pred = np.argmax(y_pred_probs, axis=1)
    if len(y_pred) != len(y_true): print(f"Warning: Mismatch between labels ({len(y_true)}) and predictions ({len(y_pred)}).")
    class_names = label_encoder.classes_
    print("\n\n--- [1/3] Overall Performance (Mixed HQ + LQ Data) ---\n" + f"Overall Accuracy: {accuracy_score(y_true, y_pred):.4f}\n" + classification_report(y_true, y_pred, target_names=class_names, zero_division=0))
    hq_indices = test_df[test_df['source_dataset'] == 'handmade'].index
    if len(hq_indices) > 0:
        y_true_hq, y_pred_hq = test_df.loc[hq_indices, 'label_id'].values, y_pred[test_df.index.get_indexer(hq_indices)]
        print("\n\n--- [2/3] Performance on High-Quality (Handmade) Data ONLY ---\n" + f"HQ Test Accuracy: {accuracy_score(y_true_hq, y_pred_hq):.4f}\n" + classification_report(y_true_hq, y_pred_hq, target_names=class_names, zero_division=0))
    lq_indices = test_df[test_df['source_dataset'] == 'wlasl'].index
    if len(lq_indices) > 0:
        y_true_lq, y_pred_lq = test_df.loc[lq_indices, 'label_id'].values, y_pred[test_df.index.get_indexer(lq_indices)]
        print("\n\n--- [3/3] Performance on Low-Quality (WLASL) Data ONLY ---\n" + f"LQ Test Accuracy: {accuracy_score(y_true_lq, y_pred_lq):.4f}\n" + classification_report(y_true_lq, y_pred_lq, target_names=class_names, zero_division=0))
    print("="*60 + "\n---                 DETAILED EVALUATION COMPLETE               ---\n" + "="*60)

# ==============================================================================
# 7. MAIN EXECUTION - UNCHANGED
# ==============================================================================
if __name__ == "__main__":
    train_df, val_df, test_df, label_encoder = prepare_data()
    print("\n--- Step 2: Creating Data Generators ---")
    train_gen = SignLanguageGenerator(train_df, LANDMARKS_DIR, BATCH_SIZE, FEATURE_DIM, shuffle=True)
    val_gen = SignLanguageGenerator(val_df, LANDMARKS_DIR, BATCH_SIZE, FEATURE_DIM, shuffle=False)
    test_gen = SignLanguageGenerator(test_df, LANDMARKS_DIR, BATCH_SIZE, FEATURE_DIM, shuffle=False)
    print("\n--- Step 3: Building Model ---")
    model = build_model((None, FEATURE_DIM), len(label_encoder.classes_))
    model.summary()
    print("\n--- Step 4: Training Model ---")
    callbacks = [
        ModelCheckpoint(filepath=MODEL_SAVE_PATH, monitor='val_accuracy', save_best_only=True, mode='max', verbose=1),
        EarlyStopping(monitor='val_accuracy', patience=PATIENCE, mode='max', verbose=1, restore_best_weights=True),
        ReduceLROnPlateau(monitor='val_loss', factor=0.2, patience=7, min_lr=1e-6, verbose=1)
    ]
    history = model.fit(train_gen, validation_data=val_gen, epochs=EPOCHS, callbacks=callbacks, verbose=1)
    detailed_evaluation(model, test_df, test_gen, label_encoder)
    print(f"\nBest model saved to: {MODEL_SAVE_PATH}")