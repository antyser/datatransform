#pipe trending and channel data from schedule

from kafka import SimpleProducer, KafkaClient, SimpleConsumer
from kafka.common import MessageSizeTooLargeError
import json,logging, time

#cpp.pages -> process

IN_KAFKA_HOST = '172.31.20.238:9092'
CONSUMER_TOPIC = 'cpp.pages'
CONSUMER_GROUP = 'trending'
OUT_KAFKA_HOST = '172.31.20.238:9092'
PRODUCER_TOPIC='process'

def fetchFrom():
    in_kafka = KafkaClient(IN_KAFKA_HOST)
    consumer = SimpleConsumer(in_kafka, 'trending', CONSUMER_TOPIC, max_buffer_size=20*1024*1024)
    out_kafka = KafkaClient(OUT_KAFKA_HOST)
    producer = SimpleProducer(out_kafka)

    for msg in consumer:
        record = json.loads(msg.message.value)
        if '_trends' in record['metadata']:
            try:
                producer.send_messages("trends", msg.message.value)
                print(str(time.strftime("%c")) + " pump url " + record['inlink'].encode('utf-8'))
            except MessageSizeTooLargeError as err:
                logging.warning(err)
            continue

        if '_channel' in record['metadata']:
            try:
                producer.send_messages("channels", msg.message.value)
                print(str(time.strftime("%c")) + " pump url " + record['inlink'].encode('utf-8'))
            except MessageSizeTooLargeError as err:
                logging.warning(err)
            continue
    in_kafka.close()
    out_kafka.close()


if __name__ == '__main__':
    logging.basicConfig(file='data.log', level=logging.INFO)
    fetchFrom()
