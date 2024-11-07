import time
import random
from kafka import KafkaProducer
import json
import numpy as np
from datetime import datetime, timedelta



class TransactionProducer:
    def __init__(self, brokers, topic):
        self.producer = KafkaProducer(
            bootstrap_servers = brokers,
            value_serializer = lambda v: json.dumps(v).encode('utf-8')
        )
        self.topic = topic



        def produce_transaction(self):
            while True:
                transaction = {
                    'distance_from_home': np.random.uniform(0, 0.5),
                    'distance_from_last_transaction' : np.random.uniform(0, 0.5),
                    'ratio_to_median_purchase_price': np.random.uniform(0, 0.5),
                    'repeat_retailer': np.random.choice([0,1]),
                    'used_chip': np.random.choice([0,1]),
                    'used_pin_number': np.random.choice([0,1]),
                    'online_order': np.random.choice([0,1]),
                }

                #Send transaction data to kafka topic
                self.producer.send(self.topic, value = transaction)
                print(f'Produced: {transaction}')
                
                time.sleep(1000)


if __name__ == '__main__':
    brokers = ['broker:29092']
    topic = 'raw_transaction'

    producer = TransactionProducer(brokers, topic)
    producer.produce_transaction()