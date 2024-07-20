from kafka import KafkaConsumer, KafkaProducer
import json
import statsmodels.api as sm

def arima_forecast(prices, n_periods):
    model = sm.tsa.ARIMA(prices, order=(1, 1, 1))
    model_fit = model.fit()
    forecast = model_fit.forecast(steps=n_periods)
    return forecast[0].tolist()

consumer = KafkaConsumer(
    'arima_request',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='group_id',
    value_deserializer=lambda x: x.decode('utf-8')
)

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

for message in consumer:
    symbol = message.value
    print(f"Received request for symbol: {symbol}")
    
    prices = [1, 2, 3, 4, 5]
    
    forecast = arima_forecast(prices, 10)
    
    response = {
        'symbol': symbol,
        'forecast': forecast
    }
    
    producer.send('arima_response', value=response)
    print(f"Sent response for symbol: {symbol}")