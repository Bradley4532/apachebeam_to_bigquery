import pandas

data = r'C:\\Users\Brad\Documents\code\test_files\test1.csv'


df = pandas.read_csv(data)
print(df)
df.to_parquet(r'C:\\Users\Brad\Documents\code\test_files\test3.parquet')