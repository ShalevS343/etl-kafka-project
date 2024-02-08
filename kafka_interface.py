import json

from confluent_kafka import Producer, Consumer

from variables import CLOUDKARAFKA_HOSTNAME, CLOUDKARAFKA_PASSWORD, CLOUDKARAFKA_USERNAME


# KafkaInterface class is responsible for all of the messages send between the Extract part to the
# Transform part in the ETL the messages will be sent in realtime 
class KafkaInterface:
    def __init__(self):
        # Kafka producer configuration
        self.producer_config = {
            'bootstrap.servers': CLOUDKARAFKA_HOSTNAME,
            'session.timeout.ms': 6000,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'SCRAM-SHA-256',
            'sasl.username': CLOUDKARAFKA_USERNAME,
            'sasl.password': CLOUDKARAFKA_PASSWORD
        }


    def produce_to_topic(self, topic, data):
        producer = Producer(self.producer_config)

        try:
            # Produce JSON message to the specified topic
            for title, information in data.items():
                producer.produce(topic, key=None, value=json.dumps({ title: information }))
                producer.flush()
                print(f"Produced to {topic}: {title}: {information}")
        except Exception as e:
            print(f"Error producing to {topic}: {e}")


    def consume_from_topic(self, topic):
        # Kafka consumer configuration
        self.consumer_config = {
            'bootstrap.servers': CLOUDKARAFKA_HOSTNAME,
            'group.id': topic,
            'session.timeout.ms': 6000,
            'default.topic.config': {'auto.offset.reset': 'smallest'},
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'SCRAM-SHA-256',
            'sasl.username': CLOUDKARAFKA_USERNAME,
            'sasl.password': CLOUDKARAFKA_PASSWORD
        }
        
        consumer = Consumer(self.consumer_config)
        consumer.subscribe([topic])

        try:
            while True:
                msg = consumer.poll(1.0)

                if msg is None:
                    continue
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue

                # Process the consumed message
                data = json.loads(msg.value())
                print(f"Consumed from { topic }: {data}")
        except KeyboardInterrupt:
            pass


kafka_interface = KafkaInterface()