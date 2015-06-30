from kafka import SimpleProducer, KafkaClient, SimpleConsumer
import json, logging, time
import beanstalkc
import feedparser
import requests
import urlparse
import urllib
KAFKA_HOST = '172.31.10.154:9092'
BEANSTALK_HOST = '172.31.10.154'
BEANSTALK_PORT = 11300
DEDUP_HOST = '172.31.16.133:8000'

def is_dup(url):
    query = "http://" + DEDUP_HOST + "/urls/"
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
    print 'USAGE:  python googleNewsToSeeds.py'
    logging.basicConfig(file='fetch.log', level=logging.INFO)
    beanstalk = beanstalkc.Connection(host=BEANSTALK_HOST, port=BEANSTALK_PORT)
    with open('google_keywords') as file:
        for keyword in file:
            keyword = keyword.strip().lower()
            line = urllib.quote_plus(keyword.encode('utf-8'))
            url = 'http://news.google.com/news?hl=en&gl=us&authuser=0&q=' + line + '&um=1&ie=UTF-8&output=rss'
            d = feedparser.parse(url)
            for entry in d.entries:
                link = entry.link
                parsed = urlparse.urlparse(link)
                content_url = urlparse.parse_qs(parsed.query)['url'][0]
                seed = {}
                seed['url'] = content_url
                if is_dup(content_url):
                    print "duplicated url: " + content_url
                    continue
                seed['ts_task'] = int(time.time())
                seed['label'] = 'cpp'
                seed['meta'] = {}
                seed['meta']['source'] = 'gn'
                seed['meta']['keyword'] = keyword
                seed['inlink'] = url
                print seed
                beanstalk.put(json.dumps(seed), priority=1)