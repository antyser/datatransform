__author__ = 'junliu'
from kafka import SimpleProducer, KafkaClient, SimpleConsumer
import json,logging, time
import beanstalkc


def fetchFrom(kafka_host):
    kafka = KafkaClient(kafka_host)
    consumer = SimpleConsumer(kafka, 'bsfetcher', 'topsite.links')

    for msg in consumer:
        topsites = json.loads(msg.message.value)
        for url in topsites['links']:
            seed = {}
            seed['url'] = url
            seed['ts_task'] = int(time.time())
            seed['label'] = 'toppage'
            beanstalk.put(json.dumps(seed), priority=2)

    kafka.close()


if __name__ == '__main__':
    print 'USAGE:  python genSeedTopSite.py'
    logging.basicConfig(file='fetch.log', level=logging.INFO)
    kafka_host = '172.31.10.154:9092'
    beanstalk = beanstalkc.Connection(host='172.31.10.154', port=11300)
    fetchFrom(kafka_host)




