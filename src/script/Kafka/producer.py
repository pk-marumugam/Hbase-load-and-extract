from json import dumps, loads
from kafka import KafkaProducer
from kafka.errors import KafkaError
import csv

if __name__ == '__main__':
    # Define server with port
    bootstrap_servers = ['localhost:9092']

    # Define topic name where the message will publish
    topic_name = 'order'

    # Initialize producer variable
    producer = KafkaProducer(bootstrap_servers = bootstrap_servers, value_serializer=lambda   K:dumps(K).encode('utf-8'))
    print("start..")
    with open('/home/cloudera/orders_data.csv', 'r') as file:
       reader = csv.reader(file, delimiter = ',')
       for messages in reader:
           producer.send(topic_name, messages)
           producer.flush()
    # Print message
    print("Message Sent")
