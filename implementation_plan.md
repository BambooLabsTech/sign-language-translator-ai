# Technical Implementation Plan: Combined WLASL & MS-ASL Dataset

**Document Version:** 1.1
**Date:** 2023-10-27
**Author:** [Your Name/Assistant]

## 1. Objective

To create a unified American Sign Language (ASL) video dataset by merging WLASL and MS-ASL. This involves:
1.  Identifying and resolving duplicate video usage and split conflicts between datasets.
2.  Downloading missing MS-ASL videos from URLs and trimming segments accurately.
3.  Consolidating all required video files (original WLASL, downloaded MS-ASL, trimmed segments).
4.  Generating a unified metadata file mapping instances to video files and labels.
5.  Assigning final dataset splits (train/validation/test) rigorously, preventing data leakage and aiming for a balanced ~75:15:15 ratio.
6.  Uploading the final, curated video files and metadata CSV to a specified Google Drive location.

## 2. Prerequisites

*   **Data Access:**
    *   WLASL dataset directory containing:
        *   `WLASL_v0.3.json`
        *   `videos/` subdirectory with all existing WLASL `.mp4` files.
    *   MS-ASL dataset directory containing:
        *   `MSASL_train.json`, `MSASL_val.json`, `MSASL_test.json`.
    *   `duplicate_videos_wlasl_msasl.csv` file generated previously, detailing URL overlaps.
*   **Google Drive:**
    *   `rclone` configured for the target Google Drive account OR `credentials.json` + `token.json` for Google API Python client.
    *   Sufficient Google Drive storage.
*   **Software:**
    *   Python (3.7+)
    *   Pandas (`pip install pandas`)
    *   yt-dlp (`pip install -U yt-dlp`)
    *   ffmpeg (Installed system-wide and accessible in PATH)
    *   rclone (Optional but recommended for upload)
    *   Google API Client Library for Python (`pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib`) (if not using `rclone`)
*   **Environment:**
    *   Sufficient local disk space for temporary downloads and processed videos.
    *   Reliable internet connection.

## 3. Target Google Drive Structure
GDrive:/Target_Base_Folder/CombinedASL/

├── videos/ # Contains all final .mp4 video files

└── combined_asl_metadata.csv # The final metadata file describing video files and splits

## 4. Implementation Steps & Proposed Scripts

We recommend breaking the implementation into logical scripts for clarity and maintainability.

---

### Phase 1: Metadata Aggregation, Analysis & Planning

**Script:** `01_prepare_metadata.py`

**Goal:** Load all source metadata, merge overlap information, determine processing actions for each video instance, and define final output filenames.

**Steps:**

1.  **Load WLASL Metadata:**
    *   Read `WLASL_v0.3.json`.
    *   Create `wlasl_df` DataFrame.
    *   Essential Columns: `wlasl_instance_id` (create unique ID, e.g., `gloss_videoID_instanceIdx`), `gloss`, `wlasl_video_id`, `url`, `wlasl_split` (original split), `local_video_path` (full path to existing WLASL video).
2.  **Load MS-ASL Metadata:**
    *   Read `MSASL_train.json`, `MSASL_val.json`, `MSASL_test.json`.
    *   Combine into `msasl_df` DataFrame.
    *   Add/Standardize Columns: `msasl_instance_id` (create unique ID), `clean_text`, `label`, `url`, `start_time`, `end_time`, `fps`, `msasl_split` (original split).
3.  **Load Overlap Data:**
    *   Read `duplicate_videos_wlasl_msasl.csv` into `duplicates_df`. This DataFrame directly maps overlapping URLs and provides context (splits, glosses) from both datasets.
4.  **Flag Processing Needs for MS-ASL:**
    *   Create `wlasl_urls_set = set(wlasl_df['url'].dropna())`.
    *   Add boolean columns `needs_download`, `needs_trimming` to `msasl_df`.
    *   Iterate through `msasl_df`:
        *   If `url` not in `wlasl_urls_set`: `needs_download=True`, `needs_trimming=True`.
        *   If `url` in `wlasl_urls_set`: `needs_download=False`, `needs_trimming=True` (assume MS-ASL always specifies a potentially different segment).
5.  **Define Action Strategy (Using `duplicates_df`):**
    *   Add `final_action` column to `wlasl_df` and `msasl_df` (e.g., 'keep_wlasl_original', 'keep_msasl_trimmed', 'remove_msasl_duplicate', 'keep_msasl_full_download', 'keep_both_distinct'). Initialize appropriately (e.g., 'keep_wlasl_original', 'keep_msasl_unprocessed').
    *   Iterate through `duplicates_df` or unique URLs present in it. For each shared URL:
        *   Identify the corresponding rows in `wlasl_df` and `msasl_df`.
        *   **Scenario A (Likely Exact Match):** MS-ASL `start_time` near 0 and `end_time` near WLASL video duration (needs duration check - potentially add later) AND `gloss` approx equals `clean_text`.
            *   Decision: Prioritize WLASL. Mark MS-ASL row `final_action = 'remove_msasl_duplicate'`. Keep WLASL `final_action`.
        *   **Scenario B (Partial Match / Trim Needed):** MS-ASL needs a specific segment (`start_time` > 0 or `end_time` < duration) from a shared URL.
            *   Decision: Keep both as distinct data points. Mark MS-ASL row `final_action = 'keep_msasl_trimmed'`. Keep WLASL `final_action`.
        *   **Scenario C (Content Mismatch):** Shared URL but `gloss` clearly different from `clean_text`.
            *   Decision: Treat as distinct. Mark MS-ASL row `final_action = 'keep_msasl_trimmed'`. Keep WLASL `final_action`.
    *   For MS-ASL rows *not* involved in duplicates: Set `final_action = 'keep_msasl_full_download'` if `needs_download`, or 'keep_msasl_trimmed' if `needs_trimming` but not download (this case might be rare if trimming always implies download or using WLASL source). Clarify this logic based on data.
6.  **Define Final Video Filenames & Paths:**
    *   Create `final_video_filename` column in both DataFrames.
    *   WLASL (`keep_wlasl_original`): e.g., `WLASL_<wlasl_instance_id>.mp4`.
    *   MSASL (`keep_msasl_trimmed`, `keep_msasl_full_download`): e.g., `MSASL_<msasl_instance_id>_<label>.mp4`. Ensure uniqueness.
    *   Add `source_video_path_for_processing` column to `msasl_df`:
        *   If `needs_download`: Path to expected downloaded file in `temp_download/` (e.g., `temp_download/<youtube_id>.mp4`).
        *   If not `needs_download`: Path from the corresponding WLASL entry's `local_video_path`.
    *   Calculate `start_frame = round(start_time * fps)`, `end_frame = round(end_time * fps)` for MS-ASL entries needing trimming.

**Output:**
*   `intermediate/wlasl_metadata_processed.csv`
*   `intermediate/msasl_metadata_processed.csv` (Containing `final_action`, flags, filenames, paths etc.)

---

### Phase 2: Video Acquisition and Processing

**Script:** `02_process_videos.py`

**Goal:** Download necessary YouTube videos and trim segments as defined in Phase 1 metadata. Leverage logic from `download_cutter.py`.

**Steps:**

1.  **Setup Directories:** Create `temp_download/` and `final_processed_videos/`.
2.  **Load Processed Metadata:** Read `intermediate/msasl_metadata_processed.csv`.
3.  **Download Unique Videos:**
    *   Filter `msasl_df` for `needs_download == True` and relevant `final_action`.
    *   Get the unique list of URLs to download.
    *   Use `yt-dlp` (adapt `download_cutter.py` download function) to download into `temp_download/`. Name files predictably (e.g., `<youtube_id>.<ext>`). Handle errors robustly (log failures, update metadata if needed).
4.  **Trim Video Segments:**
    *   Filter `msasl_df` for rows requiring trimming (`needs_trimming == True` and relevant `final_action`).
    *   Iterate through these rows:
        *   Get `source_video_path_for_processing`, `start_time`, `end_time`.
        *   Get the target `final_video_filename`.
        *   Use `ffmpeg` (adapt `download_cutter.py` cutting function) to extract the segment. Use the `use_accurate_cutting=True` option for better results, accepting the speed trade-off.
        *   Save the output to `final_processed_videos/<final_video_filename>`.
        *   Handle `ffmpeg` errors (log, potentially skip).
5.  **Consolidate WLASL Videos:**
    *   Load `intermediate/wlasl_metadata_processed.csv`.
    *   Filter for `final_action == 'keep_wlasl_original'`.
    *   For each row, copy the video from `local_video_path` to `final_processed_videos/<final_video_filename>` (this standardizes naming).

**Output:**
*   `final_processed_videos/` directory populated with all necessary `.mp4` files (original WLASL renamed, MS-ASL downloaded/trimmed).
*   Log files detailing download/trimming successes and failures.

---

### Phase 3: Final Metadata Assembly & Split Assignment

**Script:** `03_finalize_metadata_and_split.py`

**Goal:** Combine metadata for kept instances, assign final train/val/test splits preventing leakage, and save the final metadata file.

**Steps:**

1.  **Load Processed Metadata:** Read `intermediate/wlasl_metadata_processed.csv` and `intermediate/msasl_metadata_processed.csv`.
2.  **Filter Kept Instances:** Select rows based on `final_action` indicating they are part of the final dataset.
3.  **Combine and Standardize:**
    *   Concatenate filtered WLASL and MS-ASL data into `final_metadata_df`.
    *   Create/Standardize Columns: `instance_id` (globally unique), `source_dataset` ('WLASL'/'MSASL'), `label_text` (from `gloss` or `clean_text`), `label_numeric` (map if needed), `video_filename` (basename, e.g., `WLASL_xyz.mp4`), `original_wlasl_split`, `original_msasl_split`, `url`.
4.  **Assign Final Splits (CRITICAL):**
    *   Add `final_split` column.
    *   **Rule 1 (Conflict Resolution):** Load `duplicates_df`. Identify unique URLs present in `duplicates_df` where entries have conflicting original splits (e.g., one is 'train', another is 'test' or 'val'). For *all* rows in `final_metadata_df` that share such a conflict-URL, set `final_split` to the strictest split ('test' > 'val').
    *   **Rule 2 (Default Assignment):** For rows *not* assigned by Rule 1, set `final_split` based on their original split (`original_wlasl_split` or `original_msasl_split`). If source was MS-ASL, use `msasl_split`; if WLASL, use `wlasl_split`. (Overlap cases with non-conflicting splits handled here).
    *   **Rule 3 (Re-balancing):**
        *   Calculate current split ratios based on assignments from Rules 1 & 2.
        *   Determine target counts for ~75:15:15.
        *   Identify instances currently assigned 'train' (and *not* forced to 'val'/'test' by Rule 1).
        *   Randomly select (consider stratification by `label_text`) 'train' instances and change their `final_split` to 'val' or 'test' until target ratios are met. **Never move Rule 1 'val'/'test' instances back to 'train'.**
5.  **Add Google Drive Path:**
    *   Add `video_gdrive_path` column. Populate it by prefixing `video_filename` with `videos/`.
6.  **Save Final Metadata:**
    *   Select final essential columns.
    *   Save `final_metadata_df` to `combined_asl_metadata.csv`.

**Output:**
*   `combined_asl_metadata.csv`

---

### Phase 4: Upload to Google Drive

**Script:** `04_upload_to_gdrive.py`

**Goal:** Upload the processed videos and the final metadata file to Google Drive.

**Steps:**

1.  **Authenticate:** Use `rclone` config or Google API client authentication (adapt logic from `upload_wlasl.py`).
2.  **Create Drive Folders:** Programmatically create the target structure (`GDrive:/<Target Base Folder>/CombinedASL/` and `GDrive:/<Target Base Folder>/CombinedASL/videos/`) if it doesn't exist (adapt `get_or_create_folder` from `upload_wlasl.py`).
3.  **Upload Videos:**
    *   Use `rclone copy final_processed_videos/ GDrive:/<Target Base Folder>/CombinedASL/videos/ --progress` (Recommended).
    *   *Alternatively*, iterate through `final_processed_videos/` and use the Google API client's `MediaFileUpload` (adapt `upload_file_to_folder` from `upload_wlasl.py`), potentially slower for many files.
4.  **Upload Metadata:**
    *   Upload `combined_asl_metadata.csv` to `GDrive:/<Target Base Folder>/CombinedASL/`.

**Output:**
*   Files uploaded to Google Drive.
*   Console logs indicating upload progress and status.

---

## 5. Tools & Technologies Summary

*   **Core:** Python 3.x, Pandas
*   **Video Download:** yt-dlp
*   **Video Processing:** ffmpeg
*   **Cloud Storage:** Google Drive
*   **Upload Client:** rclone (recommended) or Google API Client for Python

## 6. Deliverables

1.  All curated video files (`.mp4`) in `GDrive:/<Target Base Folder>/CombinedASL/videos/`.
2.  Final metadata file `combined_asl_metadata.csv` in `GDrive:/<Target Base Folder>/CombinedASL/`.

## 7. Key Considerations & Risks

*   **Duplicate Resolution Logic (Step 1.5):** Defining "equivalent" labels and checking video durations for Scenario A requires careful implementation. Misclassification affects final dataset content.
*   **Split Leakage Prevention (Step 3.4):** Rule 1 logic must be strictly enforced to ensure valid model evaluation.
*   **Video Availability & Errors:** `yt-dlp` and `ffmpeg` can fail. Implement robust error logging and potentially allow skipping problematic videos while updating metadata.
*   **Trimming Accuracy:** Using accurate `ffmpeg` flags (`use_accurate_cutting=True` in the plan) is important but slower.
*   **Resource Requirements:** Significant local disk space and processing time needed for Phase 2. Upload (Phase 4) can also be lengthy. Check GDrive quotas.
*   **Label Consistency:** Final `label_text`/`label_numeric` may require further mapping or normalization depending on the downstream task.
*   **Reproducibility:** Using separate scripts and clear intermediate files helps. Consider adding versioning or logging parameters used.