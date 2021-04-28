import pandas as pd
from kafka import KafkaProducer
from time import sleep
import os

if __name__ == '__main__':
    topic = 'twitter-tweets'
    brokers = os.environ.get('KAFKA_BROKERS').split(',')
    producer = KafkaProducer(bootstrap_servers=brokers)

    tweets = pd.read_csv('./data/training.1600000.processed.noemoticon.csv',
                         names=['target', 'id', 'date', 'flag', 'user', 'text'])

    for i, tweet in tweets.iterrows():
        producer.send(topic, tweet.to_json().encode('utf-8'))

        if i % 1000:
            print(f"Producer published {i} messages.")

        sleep(0.05)
