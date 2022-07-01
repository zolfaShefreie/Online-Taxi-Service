import pandas as pd

path = '/mnt/f/Mine/terms/masters/term2/bigData/hw/5/'
df = pd.read_csv(path+'data.csv', header=[0])
df = df.sort_values(by="Date/Time")
df.to_csv(path+'sort_data.csv', sep=',', encoding='utf-8', index=False)
