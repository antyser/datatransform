# coding=utf-8
from kafka import KafkaClient, SimpleConsumer
import json,logging, os, sys

IN_KAFKA_HOST = '172.31.30.233:9092,172.31.30.234:9092,172.31.30.235:9092'
CONSUMER_TOPIC = 'schedule'
CONSUMER_GROUP = 'trends_counter'


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
        for msg in consumer:
            try:
                record = json.loads(msg.message.value)
                if "tags" not in record or "_trends" not in record["tags"]:
                    continue
		if record["metadata"]["trending_time"] == 0:
		    continue
                stream = record["stream"].encode()
                rank = str(record["metadata"]["trending_rank"])
                source = record["metadata"]["trending_source"]
                word = record["metadata"]["trending_word"]
                ts = record["metadata"]["trending_time"]
                with open(os.path.join(sys.argv[1], str(int(ts))), 'a+') as out:
                    for link in record["links"]:
                        out.write(stream + '\t' + rank + '\t' + source + '\t' + word + '\t')
                        out.write(str(link["metadata"]["linkrank"]) + '\t' + link["url"].encode('utf-8') + '\n')
            except Exception as e:
                logging.warning(e)
fetchFrom()
