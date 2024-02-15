import json
from confluent_kafka import Producer, Consumer
from variables import CLOUDKARAFKA_HOSTNAME, CLOUDKARAFKA_PASSWORD, CLOUDKARAFKA_USERNAME

class KafkaInterface:
    def __init__(self):
        """
        Initializes the KafkaInterface class.

        The class is responsible for handling messages sent between the Extract and Transform parts in the ETL process.
        Messages are sent in real-time.
        """
        
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
        """
        Produces messages to a specified Kafka topic.

        Parameters:
        - topic: The Kafka topic to which messages will be produced.
        - data: A dictionary containing data to be sent as messages.

        Messages are produced in JSON format.
        """
        producer = Producer(self.producer_config)

        try:
            # Produce JSON message to the specified topic
            for title, information in data.items():
                producer.produce(topic, key=None, value=json.dumps({title: information}))
                producer.flush()
                print(f"Produced to {topic}: {title}: {information}")
        except Exception as e:
            print(f"Error producing to {topic}: {e}")

    def consume_from_topic(self, topic):
        """
        Consumes messages from a specified Kafka topic.

        Parameters:
        - topic: The Kafka topic from which messages will be consumed.

        Consumed messages are printed in the console.
        """
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
                print(f"Consumed from {topic}: {data}")
        except KeyboardInterrupt:
            pass

# Singleton instance of KafkaInterface for easy access
kafka_interface = KafkaInterface()