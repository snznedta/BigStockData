import sys
from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError
from pyspark import SparkContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming import StreamingContext
import json
import logging
import time

topic = 'stock-analyzer'
target_topic = 'average-stock-price'
brokers = 'localhost:9092'
kafka_producer = None

logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('stream-processing')
logger.setLevel(logging.INFO)


def process_stream(stream):

    def send_to_kafka(rdd):
        results = rdd.collect()
        for r in results:
            data = json.dumps(
                {
                    'symbol': r[0],
                    'timestamp': time.time(),
                    'average': r[1]
                }
            )
            try:
                logger.info('Sending average price %s to kafka' % data)
                kafka_producer.send(target_topic, value=data)
            except KafkaError as error:
                logger.warn('Failed to send average stock price to kafka, caused by: %s', error.message)

    def pair(data):
        record = json.loads(data[1].decode('utf-8'))[0]
        return record.get('StockSymbol'), (float(record.get('LastTradePrice')), 1)

    stream.map(pair).reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])).map(lambda (k, v): (k, v[0]/v[1])).foreachRDD(send_to_kafka)


if __name__ == '__main__':
    # kafka broker
    if (len(sys.argv) != 4):
        print("Usage: stream-process.py [topic] [target-topic] [broker-list]")
        exit(1)
    # - create SparkContext and StreamingContext
    sc = SparkContext("local[2]", "StockAveragePrice")
    sc.setLogLevel('INFO')
    ssc = StreamingContext(sc, 5)
    topic, target_topic, brokers = sys.argv[1:]
    directKafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {'metadata.broker.list': brokers})
    #directKafkaStream.foreachRDD(process)
    process_stream(directKafkaStream)
    kafka_producer = KafkaProducer(
        bootstrap_servers=brokers
    )
    ssc.start()
    ssc.awaitTermination()
