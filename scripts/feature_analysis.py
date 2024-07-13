from yaat.maester import Maester
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler

# m = Maester('mongodb://Earl:pink-Flamingo1317@52.91.137.11/')
m = Maester('localhost:27017')

# get the dataset
dataset_size, dataset_path = m.get_dataset(['SPY', 'AAPL', 'XLB'])

# Initialize the StandardScaler
scaler = StandardScaler()

# create the dataframe
df = pd.read_csv(dataset_path)

# drop some columns
cols2drop = set(col for col in df.columns if '_close' in col or '_high' in col or '_low' in col or '_transactions' in col)
df.drop(cols2drop, axis=1, inplace=True)
df.drop('date', axis=1, inplace=True)

# Fit and transform the data
scaler = StandardScaler()
normalized_data = scaler.fit_transform(df)

# Convert back to DataFrame for easier handling
df = pd.DataFrame(normalized_data, columns=df.columns)

# lets see the correleation
correlation_matrix = df.corr(method='spearman')  # You can change to 'spearman' or 'kendall'
print(correlation_matrix)

sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm')
plt.show()