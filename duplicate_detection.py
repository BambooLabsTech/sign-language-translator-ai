import os
import json
from collections import defaultdict
import pandas as pd

# --- Configuration ---
# IMPORTANT: Update these paths to the correct locations on your system
WLASL_DIR = '/home/pandu/.cache/kagglehub/datasets/risangbaskoro/wlasl-processed/versions/5'
MSASL_DIR = '/home/pandu/.cache/kagglehub/datasets/nadayoussefamrawy/ms-asl/versions/1/MS-ASL'

WLASL_JSON_FILE = os.path.join(WLASL_DIR, 'WLASL_v0.3.json')
MSASL_TRAIN_FILE = os.path.join(MSASL_DIR, 'MSASL_train.json')
MSASL_VAL_FILE = os.path.join(MSASL_DIR, 'MSASL_val.json')
MSASL_TEST_FILE = os.path.join(MSASL_DIR, 'MSASL_test.json')

# Check if directories exist
if not os.path.isdir(WLASL_DIR):
    print(f"Error: WLASL directory not found at {WLASL_DIR}")
    exit()
if not os.path.isdir(MSASL_DIR):
    print(f"Error: MS-ASL directory not found at {MSASL_DIR}")
    exit()

# --- Step 1: Load WLASL data and extract URLs ---
print(f"Loading WLASL data from: {WLASL_JSON_FILE}")
wlasl_urls_set = set()
# Store details: url -> list of (gloss, video_id) tuples
wlasl_url_details = defaultdict(list)

try:
    with open(WLASL_JSON_FILE, 'r') as f:
        wlasl_data = json.load(f)

    print("Extracting WLASL URLs...")
    wlasl_instance_count = 0
    for entry in wlasl_data:
        gloss = entry.get('gloss')
        for instance in entry.get('instances', []):
            wlasl_instance_count += 1
            url = instance.get('url')
            video_id = instance.get('video_id')
            if url: # Check if URL is not None or empty
                wlasl_urls_set.add(url)
                wlasl_url_details[url].append({'gloss': gloss, 'video_id': video_id, 'split': instance.get('split')})

    print(f"Found {len(wlasl_urls_set)} unique URLs from {wlasl_instance_count} instances in WLASL.")

except FileNotFoundError:
    print(f"Error: WLASL JSON file not found at {WLASL_JSON_FILE}")
    exit()
except json.JSONDecodeError:
    print(f"Error: Could not decode JSON from {WLASL_JSON_FILE}")
    exit()
except Exception as e:
    print(f"An error occurred while processing WLASL data: {e}")
    exit()

# --- Step 2: Load MS-ASL data ---
print("\nLoading MS-ASL data...")
msasl_data = []
msasl_files = {
    'train': MSASL_TRAIN_FILE,
    'val': MSASL_VAL_FILE,
    'test': MSASL_TEST_FILE
}

total_msasl_instances = 0
for split, filepath in msasl_files.items():
    try:
        print(f"Loading {split} data from: {filepath}")
        if not os.path.exists(filepath):
            print(f"Warning: MS-ASL file not found at {filepath}. Skipping.")
            continue
        with open(filepath, 'r') as f:
            data = json.load(f)
            for item in data:
                item['msasl_split'] = split # Add split info to each item
            msasl_data.extend(data)
            total_msasl_instances += len(data)
            print(f"Loaded {len(data)} instances from {split} split.")
    except FileNotFoundError:
         print(f"Error: MS-ASL JSON file not found at {filepath}")
         # Decide if you want to exit or continue without this split
         # exit()
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from {filepath}")
        # exit()
    except Exception as e:
        print(f"An error occurred while processing {filepath}: {e}")
        # exit()

print(f"Total MS-ASL instances loaded: {total_msasl_instances}")
if not msasl_data:
    print("Error: No MS-ASL data loaded. Exiting.")
    exit()

# --- Step 3: Find duplicate URLs ---
print("\nFinding duplicate URLs between WLASL and MS-ASL...")
duplicate_videos = []
checked_msasl_urls = 0

for msasl_item in msasl_data:
    url = msasl_item.get('url')
    if url:
        checked_msasl_urls += 1
        if url in wlasl_urls_set:
            # Found a duplicate URL!
            # Get all WLASL entries associated with this URL
            wlasl_entries = wlasl_url_details.get(url, [])
            for wlasl_entry in wlasl_entries:
                duplicate_entry = {
                    'url': url,
                    'wlasl_gloss': wlasl_entry.get('gloss'),
                    'wlasl_video_id': wlasl_entry.get('video_id'),
                    'wlasl_split': wlasl_entry.get('split'),
                    'msasl_text': msasl_item.get('clean_text', msasl_item.get('text')),
                    'msasl_label': msasl_item.get('label'),
                    'msasl_split': msasl_item.get('msasl_split') # Use the split we added earlier
                }
                duplicate_videos.append(duplicate_entry)

print(f"Checked {checked_msasl_urls} URLs from MS-ASL.")

# --- Step 4: Report Results ---
num_duplicate_entries = len(duplicate_videos)
unique_duplicate_urls = set(d['url'] for d in duplicate_videos)
num_unique_duplicate_urls = len(unique_duplicate_urls)

print(f"\n--- Results ---")
print(f"Found {num_duplicate_entries} duplicate video entries based on URL.")
print(f"These correspond to {num_unique_duplicate_urls} unique URLs present in both datasets.")

if num_duplicate_entries > 0:
    print("\nPotential issues to consider:")
    print("- Same video used for potentially different glosses/labels.")
    print("- Same video appearing in different splits (e.g., train in WLASL, test in MS-ASL).")

    # Display first few duplicates for inspection
    print("\n--- Sample Duplicate Entries ---")
    df_duplicates = pd.DataFrame(duplicate_videos)
    print(df_duplicates.head(10).to_string()) # Display first 10 rows without truncation

    # Save duplicates to a CSV file for easier analysis (optional)
    output_csv_path = 'duplicate_videos_wlasl_msasl.csv'
    try:
        df_duplicates.to_csv(output_csv_path, index=False)
        print(f"\nFull list of duplicate entries saved to: {output_csv_path}")
    except Exception as e:
        print(f"\nCould not save duplicate list to CSV: {e}")

    # Further analysis: Check for split conflicts
    split_conflicts = []
    for entry in duplicate_videos:
        if entry['wlasl_split'] and entry['msasl_split']:
             # Simple check: if one is train and the other is test or val
             if (entry['wlasl_split'] == 'train' and entry['msasl_split'] in ['test', 'val']) or \
                (entry['msasl_split'] == 'train' and entry['wlasl_split'] in ['test', 'val']):
                 split_conflicts.append(entry)

    print(f"\nFound {len(split_conflicts)} instances with potential train/test(val) split conflicts.")
    if split_conflicts:
         print("Sample split conflicts:")
         df_conflicts = pd.DataFrame(split_conflicts)
         print(df_conflicts.head(5).to_string())


else:
    print("\nNo duplicate URLs found between the two datasets.")

print("\nScript finished.")