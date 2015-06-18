__author__ = 'junliu'
from kafka import SimpleProducer, KafkaClient, SimpleConsumer
import json, logging, time
import urllib2
import beanstalkc

# topsite.links -> seeds

CONSUMER_TOPIC = 'topsite.links'
KAFKA_HOST = '172.31.10.154:9092'
BEANSTALK_HOST = '172.31.10.154'
BEANSTALK_PORT = 11300
DEDUP_HOST = '172.31.10.154:5000'


def is_dup(url):
    query = "http://" + DEDUP_HOST + "/urls/?url=" + url
    try:
        response = urllib2.urlopen(query)
        return True
    except urllib2.HTTPError as e:
        return False


if __name__ == '__main__':
    print 'USAGE:  python genSeedTopSite.py'
    logging.basicConfig(file='fetch.log', level=logging.INFO)
    kafka_host = KAFKA_HOST
    beanstalk = beanstalkc.Connection(host=BEANSTALK_HOST, port=BEANSTALK_PORT)
    kafka = KafkaClient(kafka_host)
    consumer = SimpleConsumer(kafka, 'fetcher', CONSUMER_TOPIC)

    for msg in consumer:
        topsites = json.loads(msg.message.value)
        rank = 0
        for url in topsites['links']:
            rank += 1
            if is_dup(url):
                print "duplicate url " + url
                continue
            seed = {}
            seed['url'] = url
            seed['ts_task'] = int(time.time())
            seed['label'] = 'cpp'
            seed['meta'] = {'source': 'topsite', 'category': topsites['category'], 'rank': rank, 'weight': 1}
            seed['inlink'] = topsites['url']
            beanstalk.put(json.dumps(seed), priority=2)
            print url
    kafka.close()
