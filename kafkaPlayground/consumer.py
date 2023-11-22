from kafka import KafkaConsumer
from json import loads


def main():
    print("Starting kafka consumer....")
    consumer = KafkaConsumer('numtest', 
                            bootstrap_servers=['localhost:9092'], 
                            auto_offset_reset='earliest',
                            enable_auto_commit=True,
                            group_id='my_group',
                            value_deserializer=lambda x: loads(x.decode('utf-8')))

    print("Kafka consumer started.")

    for message in consumer:
        message = message.value
        print(f"{message} is consumed.\n")


if __name__ == "__main__":
    main()
