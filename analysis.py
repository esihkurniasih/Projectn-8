from textblob import TextBlob

class SentimentAnalysis:
    def __init__(self, tweet):
        self.tweet = tweet

    def execute(self):
        # create TextBlob object of passed tweet text
        analysis = TextBlob(self.tweet)

        # set sentiment
        if analysis.sentiment.polarity > 0:
            data = {'text': self.tweet, 'sentiment': 'positive'}
        elif analysis.sentiment.polarity == 0:
            data = {'text': self.tweet, 'sentiment': 'neutral'}
        else:
            data = {'text': self.tweet, 'sentiment': 'negative'}

        return data

if __name__ == "__main__":
    result = SentimentAnalysis('hard to learn NLTK').execute()
    print(result)

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
docker-compose up
source env/bin/activate  # On macOS/Linux
# or
.\env\Scripts\activate   # On Windows
pip install textblob
python3 sentences_producer.py
python3 analytics.py
