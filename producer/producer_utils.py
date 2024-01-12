from dotenv import load_dotenv
from datetime import datetime, timedelta, time, timezone
import sys
import socket
from script.utils import load_environment_variables
from confluent_kafka import Producer
import yfinance as yf
import json
import time as t
from seleniumbase import Driver
import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime
driver = Driver(browser="chrome")


load_dotenv()

env_vars = load_environment_variables()
# Configuration for Kafka Producer
conf = {
    # Pointing to all three brokers
    'bootstrap.servers': env_vars.get("KAFKA_BROKERS"),
    'client.id': socket.gethostname(),
    'enable.idempotence': True,
}
producer = Producer(conf)


def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(
            msg.topic(), msg.partition()))


def send_to_kafka(producer, topic, key, partition, message):
    # Sending a message to Kafka
    # producer.produce(topic, key=key, partition=0, value=json.dumps(message).encode("utf-8"))
    producer.produce(topic, key=key, value=json.dumps(
        message).encode("utf-8"), callback=delivery_report)
    producer.flush()


def retrieve_real_time_data(producer, stock_symbol, kafka_topic):
    # stock_symbol = 'BTC-USD,ETH-USD,USDT-USD,BNB-USD,BCC'
    stock_symbols = stock_symbol.split(",") if stock_symbol else []
    print(stock_symbols)
    if not stock_symbols:
        print(f"No stock symbols provided in the environment variable.")
        exit(1)
    while True:
        # Fetch real-time data for the last 1 minute
        is_market_open_bool = True
        if is_market_open_bool:
            end_time = datetime.now()
            start_time = end_time - timedelta(days=1)
            for symbol_index, stock_symbol in enumerate(stock_symbols):
                real_time_data = yf.download(
                    stock_symbol, start=start_time, end=end_time, interval="1m")
                if not real_time_data.empty:
                    # Convert and send the latest real-time data point to Kafka
                    stock_symbol_new = stock_symbol
                    if '.' in stock_symbol:
                        stock_symbol_new = stock_symbol.replace('.', '-')
                    latest_data_point = real_time_data.iloc[-1]
                    real_time_data_point = {
                        'stock': stock_symbol_new,
                        'date': latest_data_point.name.isoformat(),
                        'open': latest_data_point['Open'],
                        'high': latest_data_point['High'],
                        'low': latest_data_point['Low'],
                        'close': latest_data_point['Close'],
                        'volume': latest_data_point['Volume']
                    }
                    print(symbol_index)
                    print(real_time_data_point)
                    send_to_kafka(producer, kafka_topic, stock_symbol,
                                  symbol_index, real_time_data_point)
        else:
            print("Market is closing")
        try:

            driver.get("https://banggia.vndirect.com.vn/chung-khoan/vn30")
            sourceCode = driver.page_source
            soup = BeautifulSoup(sourceCode, "html.parser")
            items = soup.select('#banggia-khop-lenh-body tr')
            for row in items:
                row_data = {}
                symbols = row.select('td span')
                stock = row.select_one('.has-symbol').text[1:-1] + '.VN'
                if not stock in stock_symbols:
                    continue
                stock = stock[:-3]
                for symbol in symbols:
                    row_data[symbol.get('id')] = symbol.text
                real_time_data_point = {
                    'stock': stock+'.VN',
                    'date': datetime.now().strftime("%Y-%m-%dT%H:%M:%S%z"),
                    'open': None,
                    'high': row_data[f'{stock}ceil'],
                    'low': row_data[f'{stock}floor'],
                    'close': None,
                    'volume': None
                }
                print(real_time_data_point)
                send_to_kafka(producer, kafka_topic + '-vn', stock_symbol,
                              symbol_index, real_time_data_point)
        except:
            print('error')
        t.sleep(5)


# driver.quit()
