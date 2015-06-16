__author__ = 'junliu'
from kafka import SimpleProducer, KafkaClient, SimpleConsumer
from kafka.common import MessageSizeTooLargeError
import json,logging, time


def fetchFrom(kafka_host):
    kafka = KafkaClient(kafka_host)
    consumer = SimpleConsumer(kafka, 'fetcher', 'toppage.pages')
    producer = SimpleProducer(kafka)

    for msg in consumer:
        page = json.loads(msg.message.value)
        output = {}
        output['inlink']=''
        output['level']=1
        output['url']=page['orig_url']
        output['fts']=page['ts_fetch']
        output['content']=page['content']
        try:
            producer.send_messages("seeds", json.dumps(output))
        except MessageSizeTooLargeError as err:
            logging.warning(err)

    kafka.close()


if __name__ == '__main__':
    logging.basicConfig(file='fetch.log', level=logging.INFO)

    kafka_host = '172.31.10.154:9092'

    fetchFrom(kafka_host)
