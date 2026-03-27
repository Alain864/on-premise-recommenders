import pandas as pd

# Load the parquet file
df = pd.read_parquet('data.parquet')

# If you only need specific columns (saves memory)
df_subset = pd.read_parquet('data.parquet', columns=['user_id', 'transaction_amount'])

df.head()

df.info()

df.describe()

df.shape

