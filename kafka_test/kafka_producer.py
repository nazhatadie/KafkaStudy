from kafka import KafkaProducer
from concurrent.futures import ThreadPoolExecutor, as_completed
import json


class KafkaProducers(object):

    def __init__(self, kafka_host, kafka_port, kafka_topic):
        self._host = kafka_host
        self._port = kafka_port
        self._topic = kafka_topic
        self._producer = None
        try:
            self._producer = KafkaProducer(bootstrap_servers="%s:%s" % (self._host, self._port),
                                           value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        except Exception as e:
            print(e)
        print(self._producer)

    def send_data(self, msg, x):
        try:
            print(x)
            producer = self._producer
            future = producer.send(self._topic, msg)
            result = future.get(timeout=60)
            producer.flush()
        except Exception as e:
            print(e)


def main():
    masg = {"id": 1, "type": "delete"}
    producer = KafkaProducers(kafka_host="10.242.111.211", kafka_port=9092, kafka_topic="test")
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(producer.send_data, masg, x) for x in range(5)]
        for future in as_completed(futures, timeout=10):
            future.result()


if __name__ == '__main__':
    main()
