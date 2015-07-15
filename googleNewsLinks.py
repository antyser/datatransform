from kafka import SimpleProducer, KafkaClient, SimpleConsumer
import json, logging, time
import feedparser
import requests
import urlparse
import urllib
import time

if __name__ == '__main__':
    print 'USAGE:  python googleNewsToSeeds.py'
    logging.basicConfig(file='fetch.log', level=logging.INFO)
    output = open('output.tsv','a')
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
                output.write(keyword.encode('utf-8') + '\t' + content_url.encode('utf-8') + '\t' + entry['title'].encode('utf-8') + '\t' + entry['published'].encode('utf-8') + '\n')
                time.sleep(2)
    output.close()