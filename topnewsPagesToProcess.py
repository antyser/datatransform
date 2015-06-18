__author__ = 'junliu'
from kafka import SimpleProducer, KafkaClient, SimpleConsumer
from kafka.common import MessageSizeTooLargeError
import json,logging, time

#cpp.pages -> process

IN_KAFKA_HOST = '172.31.10.154:9092'
CONSUMER_TOPIC = 'topsite.links'
OUT_KAFKA_HOST = '172.31.1.70:9092'
PRODUCER_TOPIC='process'

def fetchFrom():
    in_kafka = KafkaClient('172.31.10.154:9092')
    consumer = SimpleConsumer(in_kafka, 'fetcher', 'cpp.pages', max_buffer_size=20*1024*1024)
    out_kafka = KafkaClient("172.31.1.70:9092")
    producer = SimpleProducer(out_kafka)

    for msg in consumer:
        page = json.loads(msg.message.value)
	if 'retweet' in page['meta']:
	    print "remove twitter page"
	    continue
        output = {}
        output['inlink']=''
        output['level']=1
        output['url']=page['url']
        output['fts']=page['ts_fetch']
        output['content']=page['content']
        try:
            producer.send_messages("process", json.dumps(output))
            print("pump url" + output['url'])
        except MessageSizeTooLargeError as err:
            logging.warning(err)

    in_kafka.close()
    out_kafka.close()


if __name__ == '__main__':
    logging.basicConfig(file='data.log', level=logging.INFO)
    fetchFrom()
