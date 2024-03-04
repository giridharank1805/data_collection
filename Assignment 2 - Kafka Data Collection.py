# -*- coding: utf-8 -*-
"""
Created on Sun Mar  3 16:55:10 2024

@author: GK
"""

import requests
from confluent_kafka import Producer, Consumer
import mysql.connector
import time

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPICS = ['date', 'year', 'math']

# MySQL configuration
MYSQL_HOST = 'localhost'
MYSQL_USER = 'root'
MYSQL_PASSWORD = 'root'
MYSQL_DATABASE = 'sys'

# Set up Kafka producer
producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})

# Set up Kafka consumer
consumer = Consumer({
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest'
})

# Set up MySQL connection
db_connection = mysql.connector.connect(
    host=MYSQL_HOST,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    database=MYSQL_DATABASE
)
cursor = db_connection.cursor()

# Function to create MySQL table
def create_mysql_table():
    create_table_query = """
    CREATE TABLE IF NOT EXISTS data_facts (
        id INT AUTO_INCREMENT PRIMARY KEY,
        topic VARCHAR(50),
        fact TEXT
    )
    """
    cursor.execute(create_table_query)
    db_connection.commit()

# Function to fetch data from API and produce to Kafka topic
def fetch_and_produce(url, topic):
    response = requests.get(url)
    if response.status_code == 200:
        data = response.text
        producer.produce(topic, value=data)
        producer.flush()
    else:
        print("Error fetching data from", url)

# Function to consume messages from Kafka and insert into MySQL
def consume_and_insert():
    consumer.subscribe(KAFKA_TOPICS)
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue
        data = msg.value().decode('utf-8')
        # Insert data into MySQL table
        insert_query = "INSERT INTO data_facts (topic, fact) VALUES (%s, %s)"
        cursor.execute(insert_query, (msg.topic(), data))
        db_connection.commit()
        print("Inserted data into MySQL table")
        time.sleep(5)

if __name__ == "__main__":
    # Create MySQL table
    create_mysql_table()
    
    # Fetch data from APIs and produce to Kafka topics
    fetch_and_produce("http://numbersapi.com/random/date", "date")
    fetch_and_produce("http://numbersapi.com/random/year", "year")
    fetch_and_produce("http://numbersapi.com/random/math", "math")
    
    # Consume messages from Kafka and insert into MySQL
    consume_and_insert()
