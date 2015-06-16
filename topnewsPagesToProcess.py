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
            producer.send_messages("process", json.dumps(output))
            logging.log("pump url", output['url'])
        except MessageSizeTooLargeError as err:
            logging.warning(err)

    kafka.close()


if __name__ == '__main__':
    logging.basicConfig(file='data.log', level=logging.INFO)

    kafka_host = '172.31.1.70:9092'

    fetchFrom(kafka_host)
