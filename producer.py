from time import sleep
from json import dumps
from kafka import KafkaProducer
import time
import re


# Answer to 1.1
def to_dict(line):
    return {"ip_address":"dummy", "date_time":"dummy", "request_type":"dummy", "request_arg":"dummy", "status_code":"dummy", "response_size":"dummy", "referrer":"dummy", "user_agent":"dummy"}

# Answer to 1.2
def stream_file_lines(filename, kafka_producer):
    for i in range(1, 10000):
        kafka_producer.send('topic_test', key="dummykey", value=to_dict("dummyvalue"))
        print(i)
        sleep(1)

# We have already setup a producer for you
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8'),
    key_serializer=lambda x: x.encode('utf-8')
)

stream_file_lines(".\\archive\\short.access.log", producer)