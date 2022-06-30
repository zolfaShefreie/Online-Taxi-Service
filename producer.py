from kafka import KafkaProducer
import time
import pandas as pd
import json

# Messages will be serialized as JSON
def serializer(data):
    return json.dumps(data).encode('utf-8')

# create producer to kafka connection
producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=serializer)

df = pd.read_csv('/mnt/f/Mine/terms/masters/term2/bigData/hw/5/data.csv',header=[0])

if __name__ == '__main__':
    for row in range(len(df)):
        data = dict()
        data['Date/Time'] = df.loc[row,'Date/Time']
        data['Lat'] = df.loc[row,'Lat']
        data['Lon'] = df.loc[row,'Lon']
        data['Base'] = df.loc[row,'Base']

        print(f'Stream data = {str(data)}')
        producer.send('data', data)
        #time.sleep(1)
