from kafka import KafkaConsumer


class KafkaConsumers(object):

    def __init__(self, kafka_host, kafka_port, kafka_topic, group_id):
        self._host = kafka_host
        self._port = kafka_port
        self._topic = kafka_topic
        self._group_id = group_id
        self._consumer = None
        try:
            self._consumer = KafkaConsumer(bootstrap_servers="%s:%s" % (self._host, self._port),
                                           group_id=self._group_id)
            self._consumer.subscribe(self._topic)
        except Exception as e:
            print(e)

    def consumer_pull_data(self):
        """

        :return:
        """
        try:
            msg = self._consumer.poll(timeout_ms=2000, max_records=1)
            message = list(msg.values())
            if len(message) > 0:
                message = message[0][0]
                return message
        except Exception as e:
            print(e)


def main():
    consumer = KafkaConsumers(kafka_host="10.242.111.211", kafka_port=9092, kafka_topic="test", group_id="mytest")
    while True:
        consumer.consumer_pull_data()


if __name__ == '__main__':
    main()
