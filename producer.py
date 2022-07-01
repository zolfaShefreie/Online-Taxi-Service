from kafka import KafkaProducer
import pandas as pd
import json

path = '/mnt/f/Mine/terms/masters/term2/bigData/hw/5/'

# Messages will be serialized as JSON
def serializer(data):
    return json.dumps(data).encode('utf-8')

# create producer to kafka connection
producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=serializer)

df = pd.read_csv(path+'sort_data.csv',header=[0])

if __name__ == '__main__':
    for row in range(len(df)):
        data = dict()
        data['Date/Time'] = df.loc[row,'Date/Time']
        data['Lat'] = df.loc[row,'Lat']
        data['Lon'] = df.loc[row,'Lon']
        data['Base'] = df.loc[row,'Base']

        print(f'Stream data = {str(data)}')
        producer.send('data', data)
