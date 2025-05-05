import os
import json
import pandas as pd
from tqdm import tqdm 

# --- Configuration ---
WLASL_DATASET_DIR = '/home/pandu/.cache/kagglehub/datasets/risangbaskoro/wlasl-processed/versions/5'
WLASL_JSON_FILE = os.path.join(WLASL_DATASET_DIR, 'WLASL_v0.3.json')

MSASL_DATASET_DIR = '/home/pandu/.cache/kagglehub/datasets/nadayoussefamrawy/ms-asl/versions/1/MS-ASL'
MSASL_TRAIN_JSON = os.path.join(MSASL_DATASET_DIR, 'MSASL_train.json')
MSASL_VAL_JSON = os.path.join(MSASL_DATASET_DIR, 'MSASL_val.json')
MSASL_TEST_JSON = os.path.join(MSASL_DATASET_DIR, 'MSASL_test.json')

OUTPUT_CSV = 'combined_asl.csv'

# --- Helper Function to Load JSON ---
def load_json(filepath):
    print(f"Loading JSON data from: {filepath}")
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
        print(f" -> Loaded {len(data)} entries.")
        return data
    except FileNotFoundError:
        print(f"Error: JSON file not found at {filepath}")
        return None
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from {filepath}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred loading {filepath}: {e}")
        return None

# --- Load Datasets ---
wlasl_data = load_json(WLASL_JSON_FILE)
msasl_train_data = load_json(MSASL_TRAIN_JSON)
msasl_val_data = load_json(MSASL_VAL_JSON)
msasl_test_data = load_json(MSASL_TEST_JSON)

# Exit if essential data failed to load
if wlasl_data is None or msasl_train_data is None or msasl_val_data is None or msasl_test_data is None:
    print("\nOne or more essential JSON files could not be loaded. Exiting.")
    exit()

# Combine MSASL splits
msasl_data = msasl_train_data + msasl_val_data + msasl_test_data
print(f"\nCombined MSASL data: {len(msasl_data)} entries.")

# --- Process Data into a List of Dictionaries ---
all_video_data = []
print("\nProcessing WLASL data...")

# Process WLASL
for entry in tqdm(wlasl_data, desc="Processing WLASL Glosses"):
    gloss = entry.get('gloss')
    if not gloss or not isinstance(entry.get('instances'), list):
        continue

    # Category is already lowercased here, spaces will be handled later in the DataFrame
    category = gloss.lower()

    for instance in entry['instances']:
        required_keys = ['url', 'video_id', 'fps', 'frame_start', 'frame_end']
        if not all(key in instance for key in required_keys):
            continue

        frame_start_orig = instance['frame_start']
        frame_end_orig = instance['frame_end']
        frame_start_0based = frame_start_orig
        frame_end_0based_exclusive = frame_end_orig

        instance_dict = {
            'category': category, # Store the lowercased category (with spaces for now)
            'dataset_type': 'WLASL',
            'url': instance.get('url'),
            'fps': instance.get('fps'),
            'frame_start': frame_start_0based,
            'frame_end': frame_end_0based_exclusive,
        }
        all_video_data.append(instance_dict)

print(f"Processed {len(all_video_data)} instances from WLASL.")
wlasl_count = len(all_video_data)

# Process MSASL
print("\nProcessing MSASL data...")
for item in tqdm(msasl_data, desc="Processing MSASL Instances"):
    required_keys = ['clean_text', 'url', 'fps', 'start', 'end']
    if not all(key in item for key in required_keys):
         continue
    if item.get('label') is None: # Although not used directly, good check
         continue

    frame_start_0based = item['start']
    frame_end_0based_exclusive = item['end']

    if not isinstance(frame_start_0based, int) or not isinstance(frame_end_0based_exclusive, int):
        continue

    instance_dict = {
        'category': item.get('clean_text', '').lower(), # Store the lowercased category (with spaces for now)
        'dataset_type': 'MSASL',
        'url': item.get('url'),
        'fps': item.get('fps'),
        'frame_start': frame_start_0based,
        'frame_end': frame_end_0based_exclusive,
    }
    all_video_data.append(instance_dict)

msasl_count = len(all_video_data) - wlasl_count
print(f"Processed {msasl_count} instances from MSASL.")
print(f"Total instances processed: {len(all_video_data)}")

# --- Create DataFrame ---
print("\nCreating DataFrame...")
df = pd.DataFrame(all_video_data)

if df.empty:
    print("No data was processed. Exiting.")
    exit()

# --- Format Category Column ---
print("Formatting category names (lowercase, underscore for spaces, remove '#')...") # Updated print message
# Ensure string type, apply lowercase, replace spaces, AND remove '#'
df['category'] = (df['category'].astype(str)
                  .str.lower()
                  .str.replace(' ', '_', regex=False)
                  .str.replace('#', '', regex=False) # <<< ADD THIS PART
                 )
# Optional: Use regex=True for more complex whitespace:
# df['category'] = (df['category'].astype(str)
#                   .str.lower()
#                   .str.replace(r'\s+', '_', regex=True)
#                   .str.replace('#', '', regex=False) # Add hash removal here too if using regex for spaces
#                  )


# --- Add and Populate Columns ---

# 1. Generate unique 'id'
df.reset_index(inplace=True)
df.rename(columns={'index': 'id'}, inplace=True)

# 2. Generate 'filename' based on 'id'
df['filename'] = df['id'].astype(str) + '.mp4'

# 3. Add placeholder columns
df['dataset_split'] = None
df['is_valid'] = None
df['is_duplicate'] = None # Using None as requested

# --- Final DataFrame Structure ---
# Define desired column order (category_num removed)
final_columns = [
    'id',
    'category',        # Now lowercase with underscores
    'dataset_type',    # WLASL or MSASL
    'url',
    'frame_start',     # 0-based inclusive start frame index
    'frame_end',       # 0-based exclusive end frame index (-1 for end of video)
    'fps',
    'filename',        # Proposed filename (e.g., {id}.mp4)
    'dataset_split',   # To be populated later (train/val/test)
    'is_valid',        # To be populated after download/cut (True/False/None)
    'is_duplicate',    # To be populated after checking URLs (True/False/None)
]

# Ensure all desired columns exist
for col in final_columns:
    if col not in df.columns:
        df[col] = None # Add if missing

# Reorder DataFrame
df = df[final_columns]

# Populate is_duplicate column
df['is_duplicate'] = df.duplicated(subset=['url', 'category', 'frame_start', 'frame_end'], keep='first')

uniqueness_dict = {}

# Populate dict with WLASL first
for _, row in df.iterrows():
    if row["dataset_type"] == 'WLASL':
        key = f"WLASL-{row['category']}-{row['url']}"
        uniqueness_dict[key] = uniqueness_dict.get(key, 0) + 1

# Check MSASL against WLASL
for index, row in df.iterrows():
    if row["dataset_type"] == 'MSASL' and row["is_duplicate"] == False:
        key = f"WLASL-{row['category']}-{row['url']}"
        if uniqueness_dict.get(key, 0) > 0:
            df.at[index, 'is_duplicate'] = True

msasl_total = df[df['dataset_type'] == 'MSASL'].shape[0]
print(f"Total number of MSASL data: {msasl_total}")
wlasl_total = df[df['dataset_type'] == 'WLASL'].shape[0]
print(f"Total number of MSASL data: {wlasl_total}")
youtube_count = df[(df['dataset_type'] == 'WLASL') & (df['url'].str.contains('youtube', case=False, na=False))].shape[0]
print(f"Number of youtube occorance on WLASL: {youtube_count}")
youtube_count_msasl = df[(df['dataset_type'] == 'MSASL') & (df['url'].str.contains('youtube', case=False, na=False))].shape[0]
print(f"Number of youtube occorance on MSASL: {youtube_count_msasl}")
# --- Display Info and Save ---
print("\n--- Combined DataFrame Info ---")
df.info()

print("\n--- DataFrame Head ---")
print(df.head())

print("Number of row with is_duplicate==False")
count = df[df["is_duplicate"] == False].shape[0]
print(f"Count: {count}")

print("\n--- Value Counts ---")
print("Dataset Type:")
print(df['dataset_type'].value_counts())
print("\nTop 10 Categories (Formatted):")
print(df['category'].value_counts().head(10)) # Will now show formatted names

# Save the DataFrame
print(f"\nSaving DataFrame to: {OUTPUT_CSV}")
try:
    df.to_csv(OUTPUT_CSV, index=False)
    print("DataFrame saved successfully.")
except Exception as e:
    print(f"Error saving DataFrame: {e}")

print("\n--- DataFrame Creation Complete ---")