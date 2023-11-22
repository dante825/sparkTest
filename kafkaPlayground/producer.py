from time import sleep
from json import dumps
from kafka import KafkaProducer


def main():
    print("Starting kafka producer...")
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], 
                            value_serializer=lambda x : dumps(x).encode('utf-8'))

    print("Kafka producer started.")

    for e in range(1000):
        print(f"sending data: {e}")
        data = {'number': e}
        producer.send('numtest', value=data)
        sleep(2)



if __name__ == "__main__":
    main()