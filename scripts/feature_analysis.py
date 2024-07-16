from yaat.maester import Maester
import pandas as pd, dcor
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.feature_selection import mutual_info_regression

m = Maester('mongodb://Earl:pink-Flamingo1317@52.91.137.11/')
#m = Maester('localhost:27017')

# ['SQQQ', 'TQQQ', 'QQQ', 'SPY', 'NIO', 'TSLA', 'AAPL', 'UVXY', 'AMD', 'SOXL', 'IWM', 'NVDA', 'AMC', 'SOXS', 'BABA', 'CCL', 'F', 'AAL', 'MSFT', 'TNA', 'SPXU', 'SPXL', 'SPXS', 'PLUG']

# get the dataset
print("getting the dataset")
dataset_size, dataset_path, _ = m.get_dataset(['SQQQ', 'TQQQ', 'QQQ', 'SPY', 'NIO', 'TSLA', 'AAPL', 'UVXY', 'AMD', 'SOXL', 'IWM', 'NVDA', 'AMC', 'SOXS', 'BABA', 'CCL', 'F', 'AAL', 'MSFT', 'TNA', 'SPXU', 'SPXL', 'SPXS', 'PLUG'])

# Initialize the StandardScaler
scaler = StandardScaler()

# create the dataframe
df = pd.read_csv(dataset_path)

# drop some columns
df.drop([col for col in df.columns if '_open' not in col], axis=1, inplace=True)

# Fit and transform the data
scaler = StandardScaler()
normalized_data = scaler.fit_transform(df)

# Convert back to DataFrame for easier handling
df = pd.DataFrame(normalized_data, columns=df.columns)

# calculate mutual information
print("calculating mis")
mis = {}
for dep_var_col in df.columns:
    # calculate mutual information
    ind_vars_df = df.drop(dep_var_col, axis=1)
    mi = mutual_info_regression(ind_vars_df, df[dep_var_col])

    # add the "diagonal" back
    mi_list = list(mi)
    insert_position = list(df.columns).index(dep_var_col)
    mi_list.insert(insert_position, np.nan)  # Use np.nan or 0 depending on how you want to handle this
    mis[dep_var_col] = mi_list

# create the matrix
mi_matrix = pd.DataFrame(mis, index=df.columns)

# Enforce symmetry by averaging
for i in mi_matrix.columns:
    for j in mi_matrix.index:
        if i != j and not np.isnan(mi_matrix.at[i, j]) and not np.isnan(mi_matrix.at[j, i]):
            avg_value = (mi_matrix.at[i, j] + mi_matrix.at[j, i]) / 2
            mi_matrix.at[i, j] = avg_value
            mi_matrix.at[j, i] = avg_value

# get the upper triangle (one combination per pair)
mi_matrix_tri = pd.DataFrame(np.triu(mi_matrix), columns=mi_matrix.columns, index=mi_matrix.index)

# get the top k

k = 50

# Flatten the matrix to a list of (value, row_name, col_name)

mi_flat_list = []
for col in mi_matrix_tri.columns:
    for idx in mi_matrix_tri.index:
        if not np.isnan(mi_matrix_tri.at[idx, col]):
            mi_flat_list.append((mi_matrix_tri.at[idx, col], idx, col))

# Sort by mutual information value in descending order and select top K

top_k = sorted(mi_flat_list, key=lambda x: x[0], reverse=True)[:k]

# Return a DataFrame for nicer output
topk = pd.DataFrame(top_k, columns=['MI Value', 'Row', 'Column'])

combined = pd.concat([topk['Row'], topk['Column']])
most_common = combined.value_counts()

print(most_common)