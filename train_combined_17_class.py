import os
import gc
import math
import numpy as np
import pandas as pd
import tensorflow as tf
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import classification_report, accuracy_score, confusion_matrix, precision_score, recall_score, f1_score
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Input, LSTM, Bidirectional, Dense, Dropout, Masking, BatchNormalization
from tensorflow.keras.utils import Sequence, plot_model
from tensorflow.keras.callbacks import ModelCheckpoint, EarlyStopping, ReduceLROnPlateau
from tqdm import tqdm

# ==============================================================================
# 1. GPU AND ENVIRONMENT CONFIGURATION (UNCHANGED)
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
# 2. CONFIGURATION (UNCHANGED)
# ==============================================================================
# --- Data Paths ---
COMBINED_DATASET_DIR = BASE_DIR / "dataset_combined_v1"
METADATA_PATH = COMBINED_DATASET_DIR / "combined_metadata_v1.csv"
LANDMARKS_DIR = COMBINED_DATASET_DIR / "landmarks_hands"
LQ_METADATA_MASTER = BASE_DIR / "splitted_asl.csv"

# --- Class Selection ---
HANDMADE_CLASSES = ['what', 'me', 'hello', 'please', 'sorry', 'yes', 'no', 'where', 'you', 'thanks']
TOP_N_LQ = 10 

# --- Training Hyperparameters ---
BATCH_SIZE = 32
EPOCHS = 150
PATIENCE = 20

# --- Model & Output Paths ---
REPORT_SAVE_DIR = BASE_DIR / "training_reports" / "run_17_class_v1"
MODEL_SAVE_DIR = BASE_DIR / "models"
MODEL_NAME = "model_combined_hands_17cls_v1.h5"
MODEL_SAVE_PATH = MODEL_SAVE_DIR / MODEL_NAME
LABEL_MAP_PATH = MODEL_SAVE_DIR / "label_map_combined_17cls.npy"

# --- Feature Extraction ---
FEATURE_DIM = 21 * 3

# Create reporting directory
REPORT_SAVE_DIR.mkdir(parents=True, exist_ok=True)

# ==============================================================================
# 3. DATA PREPARATION (UNCHANGED)
# ==============================================================================
def prepare_data():
    # This function is identical to your working script.
    print("--- Step 1: Preparing Data for 17-Class Experiment ---")
    if not METADATA_PATH.exists() or not LQ_METADATA_MASTER.exists():
        raise FileNotFoundError("A required metadata file is missing.")
    df_combined, df_lq_master = pd.read_csv(METADATA_PATH), pd.read_csv(LQ_METADATA_MASTER)
    lq_top_10_classes = df_lq_master['category'].value_counts().nlargest(TOP_N_LQ).index.tolist()
    final_class_list = sorted(list(set(HANDMADE_CLASSES + lq_top_10_classes)))
    df = df_combined[df_combined['category'].isin(final_class_list)].reset_index(drop=True)
    label_encoder = LabelEncoder()
    df['label_id'] = label_encoder.fit_transform(df['category'])
    MODEL_SAVE_DIR.mkdir(exist_ok=True)
    np.save(LABEL_MAP_PATH, label_encoder.classes_)
    print(f"\nTargeting {len(final_class_list)} classes. Label map saved to {LABEL_MAP_PATH}")
    train_df = df[df['split'] == 'train'].reset_index(drop=True)
    val_df = df[df['split'] == 'val'].reset_index(drop=True)
    test_df = df[df['split'] == 'test'].reset_index(drop=True)
    return train_df, val_df, test_df, label_encoder

# ==============================================================================
# 4. DATA GENERATOR (UNCHANGED)
# ==============================================================================
class SignLanguageGenerator(Sequence):
    # This class is identical to your working script.
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
# 5. MODEL DEFINITION (UNCHANGED)
# ==============================================================================
def build_model(input_shape, num_classes):
    # This function is identical to your working script.
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
# 6. *** NEW *** REPORTING FUNCTION
# ==============================================================================
def generate_reports(model, history, test_df, test_gen, label_encoder):
    """
    Generates and saves all requested plots and metrics.
    """
    print("\n" + "="*60)
    print("---               GENERATING FINAL REPORTS               ---")
    print("="*60)
    
    # --- 1, 2, 3: Plot Accuracy, Loss, and Learning Rate vs. Epoch ---
    print("Generating training history plots...")
    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(12, 18), sharex=True)
    fig.suptitle('Training and Validation Metrics', fontsize=16)

    # Accuracy Plot
    ax1.plot(history.history['accuracy'], label='Training Accuracy')
    ax1.plot(history.history['val_accuracy'], label='Validation Accuracy')
    ax1.set_ylabel('Accuracy')
    ax1.legend(loc='lower right')
    ax1.set_title('Accuracy vs. Epochs')
    ax1.grid(True)

    # Loss Plot
    ax2.plot(history.history['loss'], label='Training Loss')
    ax2.plot(history.history['val_loss'], label='Validation Loss')
    ax2.set_ylabel('Loss')
    ax2.legend(loc='upper right')
    ax2.set_title('Loss vs. Epochs')
    ax2.grid(True)

    # Learning Rate Plot
    if 'learning_rate' in history.history:
        ax3.plot(history.history['learning_rate'], label='Learning Rate')
    elif 'lr' in history.history: # Fallback for older TensorFlow versions
        ax3.plot(history.history['lr'], label='Learning Rate')
    ax3.set_ylabel('Learning Rate')
    ax3.set_xlabel('Epoch')
    ax3.legend(loc='upper right')
    ax3.set_title('Learning Rate vs. Epochs')
    ax3.grid(True)
    
    plt.tight_layout(rect=[0, 0.03, 1, 0.98])
    history_plot_path = REPORT_SAVE_DIR / "training_history.png"
    plt.savefig(history_plot_path)
    plt.close()
    print(f"-> Saved history plot to: {history_plot_path}")

    # --- 4, 5: Confusion Matrix and Detailed Metrics ---
    # Robust evaluation loop to handle skipped files
    print("\nRunning robust evaluation to generate metrics...")
    all_y_true, all_y_pred, all_sources = [], [], []
    for i in tqdm(range(len(test_gen)), desc="Evaluating Test Batches"):
        # The generator needs to be modified to return sources, or we can do it here.
        # Let's align it with the dataframe manually for simplicity in this script.
        start_idx = i * test_gen.batch_size
        end_idx = start_idx + test_gen.batch_size
        batch_df = test_gen.df.iloc[start_idx:end_idx]
        
        X_batch, y_true_batch = test_gen[i]
        
        if X_batch.shape[0] > 0:
            y_pred_probs = model.predict(X_batch, verbose=0)
            y_pred_batch = np.argmax(y_pred_probs, axis=1)
            all_y_true.extend(y_true_batch)
            all_y_pred.extend(y_pred_batch)
            # Find the corresponding sources from the dataframe
            all_sources.extend(batch_df['source_dataset'].values[:len(y_true_batch)])

    all_y_true, all_y_pred, all_sources = np.array(all_y_true), np.array(all_y_pred), np.array(all_sources)
    class_names = label_encoder.classes_

    # 5: Overall Precision, Recall, F1-Score
    print("\n--- Overall Model Performance ---")
    overall_precision = precision_score(all_y_true, all_y_pred, average='weighted', zero_division=0)
    overall_recall = recall_score(all_y_true, all_y_pred, average='weighted', zero_division=0)
    overall_f1 = f1_score(all_y_true, all_y_pred, average='weighted', zero_division=0)
    print(f"  - Overall Weighted Precision: {overall_precision:.4f}")
    print(f"  - Overall Weighted Recall:    {overall_recall:.4f}")
    print(f"  - Overall Weighted F1-Score:  {overall_f1:.4f}")

    # 4: Confusion Matrix
    print("\nGenerating confusion matrix...")
    cm = confusion_matrix(all_y_true, all_y_pred)
    plt.figure(figsize=(15, 12))
    sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', xticklabels=class_names, yticklabels=class_names)
    plt.title('Confusion Matrix', fontsize=16)
    plt.ylabel('True Label')
    plt.xlabel('Predicted Label')
    plt.xticks(rotation=45, ha='right')
    plt.yticks(rotation=0)
    plt.tight_layout()
    cm_plot_path = REPORT_SAVE_DIR / "confusion_matrix.png"
    plt.savefig(cm_plot_path)
    plt.close()
    print(f"-> Saved confusion matrix to: {cm_plot_path}")

    # --- 6: LSTM Architecture ---
    print("\nGenerating model architecture plot...")
    try:
        arch_plot_path = REPORT_SAVE_DIR / "model_architecture.png"
        plot_model(model, to_file=arch_plot_path, show_shapes=True, show_layer_names=True)
        print(f"-> Saved architecture plot to: {arch_plot_path}")
    except Exception as e:
        print(f"Could not generate model plot (is graphviz/pydot installed?): {e}")
        print("Falling back to text summary.")
        with open(REPORT_SAVE_DIR / 'model_summary.txt', 'w') as f:
            model.summary(print_fn=lambda x: f.write(x + '\n'))
        print(f"-> Saved text summary to: {REPORT_SAVE_DIR / 'model_summary.txt'}")

    print("\n" + "="*60 + "\n---                 ALL REPORTS GENERATED                ---\n" + "="*60)
    # Also print the detailed classification report to console
    print("\n--- Detailed Classification Report ---")
    print(classification_report(all_y_true, all_y_pred, target_names=class_names, zero_division=0))


# ==============================================================================
# 7. MAIN EXECUTION
# ==============================================================================
if __name__ == "__main__":
    train_df, val_df, test_df, label_encoder = prepare_data()
    print("\n--- Step 2: Creating Data Generators ---")
    train_gen = SignLanguageGenerator(train_df, LANDMARKS_DIR, BATCH_SIZE, FEATURE_DIM, shuffle=True)
    val_gen = SignLanguageGenerator(val_df, LANDMARKS_DIR, BATCH_SIZE, FEATURE_DIM, shuffle=False)
    test_gen = SignLanguageGenerator(test_df, LANDMARKS_DIR, BATCH_SIZE, FEATURE_DIM, shuffle=False)

    print("\n--- Step 3: Building Model ---")
    model = build_model((None, FEATURE_DIM), len(label_encoder.classes_))
    
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
    
    # --- Step 5: Generate All Reports ---
    # The model is already the best version thanks to restore_best_weights=True
    generate_reports(model, history, test_df, test_gen, label_encoder)
    
    print(f"\nBest model saved to: {MODEL_SAVE_PATH}")
    print(f"All reports saved in: {REPORT_SAVE_DIR}")