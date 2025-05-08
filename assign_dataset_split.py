import pandas as pd
import numpy as np

df1 = pd.read_csv('combined_asl_part1.csv')
df2 = pd.read_csv('combined_asl_part2.csv')
df3 = pd.read_csv('combined_asl_part3.csv')

df = pd.concat([df1, df2, df3], ignore_index=True)

df_ok = df[(df['is_valid'] == True) & (df['is_duplicate'] == False)].copy()
df_ok["dataset_split"] = None

for category, group in df_ok.groupby('category'):
    # Shuffle the index of the group
    shuffled_index = group.sample(frac=1, random_state=42).index
    n = len(shuffled_index)

    if n >= 3:
        n_test = max(1, int(np.floor(n * 0.10)))
        n_val = max(1, int(np.floor(n * 0.10)))
        n_train = n - n_test - n_val

        # Use the shuffled index to select rows for each split
        train_indices = shuffled_index[:n_train]
        val_indices = shuffled_index[n_train:n_train+n_val]
        test_indices = shuffled_index[n_train+n_val:]

        df_ok.loc[train_indices, "dataset_split"] = "train"
        df_ok.loc[val_indices, "dataset_split"] = "val"
        df_ok.loc[test_indices, "dataset_split"] = "test"
    else:
        df_ok.loc[shuffled_index, "dataset_split"] = "train"

df_ok.to_csv('splitted_asl.csv', index=False)

print("\nSample split counts by category (up to 10):")
split_counts = df_ok.groupby(['category', 'dataset_split']).size().unstack(fill_value=0)

# Limit to 10 categories for display
print(split_counts.head(10))