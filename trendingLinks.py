# coding=utf-8
import datetime
from kafka import KafkaClient, SimpleConsumer
import json,logging, time, os, sys

#cpp.pages -> process

IN_KAFKA_HOST = '172.31.20.238:9092'
CONSUMER_TOPIC = 'trends'
CONSUMER_GROUP = 'trends_links'


def fetchFrom():
    in_kafka = KafkaClient(IN_KAFKA_HOST)
    consumer = SimpleConsumer(in_kafka, CONSUMER_GROUP, CONSUMER_TOPIC, max_buffer_size=20*1024*1024)
    query = {}
    f = open("trend_links", "w")
    while True:
        current = int(time.time()) - 100
        for msg in consumer:
            try:
                record = json.loads(msg.message.value)
                for link in record["links"]:
                    f.write(link["url"])
            except Exception as e:
                logging.warning(e)
fetchFrom()