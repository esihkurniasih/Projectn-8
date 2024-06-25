from wonderwords import RandomSentence
from confluent_kafka import Producer
import json
import time
import logging

# Setup logging
logging.basicConfig(format='%(asctime)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='producer.log',
                    filemode='w')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize Kafka Producer
p = Producer({'bootstrap.servers': 'localhost:9092'})
print('Kafka Producer has been initiated...')

def receipt(err, msg):
    if err is not None:
        print('Error: {}'.format(err))
        logger.error('Error: {}'.format(err))
    else:
        message = 'Produced message on topic {} with value of {}\n'.format(msg.topic(), msg.value().decode('utf-8'))
        logger.info(message)
        print(message)

# Generate and send random sentences
def main():
    s = RandomSentence()
    try:
        while True:
            data = {
                'sentence': s.sentence()
            }
            m = json.dumps(data)
            p.poll(1)
            p.produce('sentences', m.encode('utf-8'), callback=receipt)
            
            # Introduce a delay to control the production rate
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        p.flush()

if __name__ == '__main__':
    main()

    docker-compose up
    source env/bin/activate  # On macOS/Linux
# or
.\env\Scripts\activate   # On Windows
python3 sentences_producer.py

import signal
import sys
from confluent_kafka import Consumer, KafkaException
import sentiment_analysis as sa
import ast

# Configuration for the Kafka Consumer
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'python-consumer',
    'auto.offset.reset': 'latest'
}

# Initialize the Consumer
consumer = Consumer(consumer_conf)

def shutdown():
    print("Shutting down gracefully...")
    consumer.close()
    sys.exit(0)

def handle_signal(signal, frame):
    shutdown()

# Register signal handlers
signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

def consume_messages():
    print('Available topics to consume:', consumer.list_topics().topics)
    consumer.subscribe(['sentences'])

    while True:
        try:
            msg = consumer.poll(1.0)  # timeout
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    print('%% %s [%d] reached end at offset %d\n' %
                          (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                data = msg.value().decode('utf-8')
                try:
                    data = ast.literal_eval(data)
                    output = sa.SentimentAnalysis(data['sentence']).execute()
                    print(output)
                except (ValueError, SyntaxError) as e:
                    print(f"Error processing message: {e}")
        except KeyboardInterrupt:
            break

    shutdown()

if __name__ == '__main__':
    consume_messages()
source env/bin/activate  # On macOS/Linux
# or
.\env\Scripts\activate   # On Windows
python3 analytics.py

