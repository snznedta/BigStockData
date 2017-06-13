import argparse
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from cassandra.cluster import Cluster
import logging
import atexit
import json

logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('data-storage')
logger.setLevel(logging.DEBUG)

# - default kafka topic to read from
topic_name = 'stock-analyzer'

# - default kafka broker location
kafka_broker = 'localhost:9092'

# - default cassandra nodes to connect
contact_points = 'localhost'

# - default keyspace to use
key_space = 'stock'

# - default table to use
data_table = 'stock'


def shutdown_hook(consumer, session):
    logger.debug("start to shut down")
    consumer.close()
    session.shutdown()


def persist_data(stock_data, session):
    try:
        logger.debug('Start to persist data to cassandra %s', stock_data)
        parsed = json.loads(stock_data)[0]
        symbol = parsed.get('StockSymbol')
        price = float(parsed.get('LastTradePrice'))
        tradetime = parsed.get('LastTradeDateTime')
        statement = "INSERT INTO %s (stock_symbol, trade_time, trade_price) VALUES ('%s', '%s', %f)" % (
            data_table, symbol, tradetime, price)
        session.execute(statement)
        logger.info(
            'Persistend data to cassandra for symbol: %s, price: %f, tradetime: %s' % (symbol, price, tradetime))
    except Exception:
        logger.error('Failed to persist data to cassandra %s', stock_data)


if __name__ == '__main__':
    # - setup command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('topic_name', help='the kafka topic to subscribe from')
    parser.add_argument('kafka_broker', help='the location of the kafka broker')
    parser.add_argument('key_space', help='the keyspace to use in cassandra')
    parser.add_argument('data_table', help='the data table to use')
    parser.add_argument('contact_points', help='the contact points for cassandra')

    args = parser.parse_args()
    topic_name = args.topic_name
    kafka_broker = args.kafka_broker
    key_space = args.key_space
    data_table = args.data_table
    contact_points = args.contact_points

    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=kafka_broker
    )

    cassandra_cluster = Cluster(contact_points=contact_points.split(','))

    session = cassandra_cluster.connect()
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', "
        "'replication_factor': '3'} AND durable_writes = 'true'" % key_space
    )
    session.set_keyspace(keyspace=key_space)
    session.execute(
        "CREATE TABLE IF NOT EXISTS %s (stock_symbol text, trade_time timestamp, trade_price float, PRIMARY KEY (stock_symbol,trade_time))" % data_table)

    # - setup proper shutdown hook
    atexit.register(shutdown_hook, consumer, session)

    for msg in consumer:
        persist_data(msg.value, session)
