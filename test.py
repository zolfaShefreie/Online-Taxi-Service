from prophet import Prophet

import pandas as pd
import matplotlib.pyplot as plt

data = [
    {'ds': '2004-04-01 12:00:00', 'y': 12},
{'ds': '2004-04-08 12:00:00', 'y': 42},
{'ds': '2004-04-15 12:00:00', 'y': 12},
{'ds': '2004-04-22 12:00:00', 'y': 45},
{'ds': '2004-04-29 12:00:00', 'y': 13},
{'ds': '2004-05-6 12:00:00', 'y': 43},
{'ds': '2004-05-13 12:00:00', 'y': 12},
{'ds': '2004-05-20 12:00:00', 'y': 42},
{'ds': '2004-05-27 12:00:00', 'y': 11},
{'ds': '2004-06-03 12:00:00', 'y': 41},

]

df = pd.DataFrame(data)
model = Prophet(seasonality_mode='multiplicative', yearly_seasonality=True)
# model.add_seasonality(name='monthly', period=31, fourier_order=)
model.fit(df)
future = model.make_future_dataframe(periods=1, freq='w')
forecast = model.predict(future)
model.plot(forecast).savefig('2.png')
model.plot_components(forecast).savefig('1.png')