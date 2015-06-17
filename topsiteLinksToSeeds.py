__author__ = 'junliu'
from kafka import SimpleProducer, KafkaClient, SimpleConsumer
import json,logging, time
import beanstalkc

#topsite.links -> seeds

CONSUMER_TOPIC = 'topsite.links'
PRODUCE_CUBE = 'seeds'
KAFKA_HOST = '172.31.10.154:9092'
BEANSTALK_HOST = '172.31.10.154'
BEANSTALK_PORT = 11300

if __name__ == '__main__':
    print 'USAGE:  python genSeedTopSite.py'
    logging.basicConfig(file='fetch.log', level=logging.INFO)
    kafka_host = KAFKA_HOST
    beanstalk = beanstalkc.Connection(host=BEANSTALK_HOST, port=BEANSTALK_PORT)
    kafka = KafkaClient(kafka_host)
    consumer = SimpleConsumer(kafka, 'bsfetcher', CONSUMER_TOPIC)

    for msg in consumer:
        topsites = json.loads(msg.message.value)
        for url in topsites['links']:
            seed = {}
            seed['url'] = url
            seed['ts_task'] = int(time.time())
            seed['label'] = 'cpp'
            beanstalk.put(json.dumps(seed), priority=2)
            print url
    kafka.close()




