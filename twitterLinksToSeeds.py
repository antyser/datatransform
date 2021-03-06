__author__ = 'junliu'
from kafka import SimpleProducer, KafkaClient, SimpleConsumer
import json,logging, time
import requests
import beanstalkc

#{"ts_fetch":1434378838,"url":"http://twitter.com/aaronmoskowitz","label":"twitter","ts_task":1434378785,"twitter_meta":{"tweet_id":"483069698309763073","retweet":0,"favorite":1,"pubdate":1404007765,"text":"This is how I spent last week... pic.twitter.com/2UDhGgs6Uj","tiny_urls":[]},"ts_parse":1434502721}
#twitter.links -> seeds
CONSUMER_TOPIC = 'twitter.links'
KAFKA_HOST = '172.31.10.154:9092'
BEANSTALK_HOST = '172.31.10.154'
BEANSTALK_PORT = 11300
DEDUP_HOST = '172.31.16.133:8000'

def is_dup(url):
    query = "http://" + DEDUP_HOST + "/urls"
    data = {'add': url}
    try:
        response = requests.post(query, data=data)
        if len(response.text) == 0:
            return False
        else:
            return True
    except Exception as e:
        return False


if __name__ == '__main__':
    print 'USAGE:  python twitterLinksToSeeds.py'
    logging.basicConfig(file='fetch.log', level=logging.INFO)
    kafka_host = KAFKA_HOST
    beanstalk = beanstalkc.Connection(host=BEANSTALK_HOST, port=BEANSTALK_PORT)
    kafka = KafkaClient(kafka_host)
    consumer = SimpleConsumer(kafka, 'fetcher', CONSUMER_TOPIC)

    for msg in consumer:
        tweet_info = json.loads(msg.message.value)
        for tiny_url in tweet_info['twitter_meta']['tiny_urls']:
            if is_dup(tiny_url):
                print str(time.time()) + "url duplicated " + tiny_url.encode('utf-8')
                continue
            seed = {}
            seed['url'] = tiny_url
            seed['ts_task'] = int(time.time())
            seed['label'] = 'twittercpp'
            seed['meta'] = tweet_info['twitter_meta']
            seed['meta']['source'] = 'twitter'
            seed['inlink'] = tweet_info['url']
            print str(time.time()) + json.dumps(seed).encode('utf-8')
            beanstalk.put(json.dumps(seed), priority=3)

    kafka.close()
