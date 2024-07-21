from kafka import KafkaConsumer, KafkaProducer
import json
import statsmodels.api as sm
from dataclasses import dataclass
from typing import List

@dataclass
class PriceData:
    timestamp: str
    price: float

@dataclass
class HistoricalPriceMessage:
    symbol: str
    prices: List[PriceData]

def arima_forecast(prices, n_periods):
    model = sm.tsa.ARIMA(prices, order=(1, 1, 1))
    model_fit = model.fit()
    forecast = model_fit.forecast(steps=n_periods)
    return forecast[0].tolist()

def process_message(message):
    try:
        data = json.loads(message)
        symbol = data['symbol']
        prices = [PriceData(p['timestamp'], p['price']) for p in data['prices']]

        price_values = [p.price for p in prices]

        forecast = arima_forecast(price_values, 10)

        response = {
            'symbol': symbol,
            'forecast': forecast
        }

        producer.send('arima_response', value=response)
        print(f"Sent response for symbol: {symbol} with forecast: {forecast}")
    except json.JSONDecodeError as e:
        print(f"Failed to decode JSON message: {message}")
        print(f"Error: {e}")
    except Exception as e:
        print(f"Error processing message: {message}")
        print(f"Error: {e}")

consumer = KafkaConsumer(
    'arima_request',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='myGroup',
    value_deserializer=lambda x: x.decode('utf-8')
)

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

for message in consumer:
    print(f"Received raw message: {message.value}")
    process_message(message.value)