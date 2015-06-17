__author__ = 'junliu'
from kafka import SimpleProducer, KafkaClient, SimpleConsumer
import json,logging, time
import beanstalkc

#{"ts_fetch":1434378838,"url":"http://twitter.com/aaronmoskowitz","label":"twitter","ts_task":1434378785,"twitter_meta":{"tweet_id":"483069698309763073","retweet":0,"favorite":1,"pubdate":1404007765,"text":"This is how I spent last week... pic.twitter.com/2UDhGgs6Uj","tiny_urls":[]},"ts_parse":1434502721}

def fetchFrom(kafka_host):
    kafka = KafkaClient(kafka_host)
    consumer = SimpleConsumer(kafka, 'bsfetcher', 'topsite.links')

    for msg in consumer:
        tweet_info = json.loads(msg.message.value)
        for tiny_url in tweet_info['tiny_urls']:
            seed = {}
            seed['url'] = tiny_url
            seed['ts_task'] = int(time.time())
            seed['label'] = 'cpp'
            seed['meta'] = tweet_info['twitter_meta']
            seed['meta']['inlink'] = tweet_info['url']
            seed['meta']['level'] = 1
            beanstalk.put(json.dumps(seed), priority=3)

    kafka.close()


if __name__ == '__main__':
    print 'USAGE:  python genSeedTopSite.py'
    logging.basicConfig(file='fetch.log', level=logging.INFO)
    kafka_host = '172.31.10.154:9092'
    beanstalk = beanstalkc.Connection(host='172.31.10.154', port=11300)
    fetchFrom(kafka_host)