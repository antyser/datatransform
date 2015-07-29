# coding=utf-8
import datetime
from kafka import KafkaClient, SimpleConsumer
import json,logging, time, os, sys

#cpp.pages -> process

IN_KAFKA_HOST = '172.31.20.238:9092'
CONSUMER_TOPIC = 'trends'
CONSUMER_GROUP = 'trends_report_test'


def dumpTsv(query, filename):
    with open(filename, "w+") as file:
        for key in query:
            for line in query[key]:
                file.write('\t'.join(key) + "\t")
                file.write(line + "\n")

def fetchFrom():
    in_kafka = KafkaClient(IN_KAFKA_HOST)
    consumer = SimpleConsumer(in_kafka, CONSUMER_GROUP, CONSUMER_TOPIC, max_buffer_size=20*1024*1024)
    query = {}
    while True:
        current = int(time.time()) - 100
        for msg in consumer:
            try:
                record = json.loads(msg.message.value)
                stream = record["stream"].encode()
                rank = str(record["metadata"]["trending_rank"])
                source = record["metadata"]["trending_source"]
                word = record["metadata"]["trending_word"]
                links = []
                for link in record["links"]:
                    links.append(word + '\t' + str(link["metadata"]["linkrank"]) + '\t' + link["url"])
                t = (stream, source, rank)
                query[t] = links
                if current < record["timestamp"]:
                    filename = datetime.datetime.fromtimestamp(current).strftime('%Y-%m-%d_%H:%M:%S')
                    dumpTsv(query, os.path.join(sys.argv[1],filename))
                    break
            except Exception as e:
                logging.warning(e)
        time.sleep(100)

fetchFrom()