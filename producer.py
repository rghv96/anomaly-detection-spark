"""
For CS 5614
"""

from time import sleep
from json import dumps
from kafka import KafkaProducer
import re

"""

serializes lines into dictionary format, (K, V). 

K: Block # 
V: Event Type

"""

def to_dict(line):
    altered_row = re.split("\s", line)
    split_block = re.split("blk_", line)
    split_block_spaced = re.split("\s", split_block[1])

    if altered_row[5] == "PacketResponder":  # PacketResponder
        return {
            # "id": altered_row[0] + " " + altered_row[1],
            "id": split_block_spaced[0],
            "type": "PacketResponder * from block * terminating"
        }
    elif altered_row[5] == "Received":  # Received
        return {
            # "id": altered_row[0] + " " + altered_row[1],
            "id": split_block_spaced[0],
            "type": "Received block * from *"
        }
    elif altered_row[5] == "Verification" and altered_row[6] == "succeeded":  # Verification Succeeded
        return {
            # "id": altered_row[0] + " " + altered_row[1],
            "id": split_block_spaced[0],
            "type": "Verification succeeded for *"
        }
    elif altered_row[5] == "Receiving":  # Receiving
        return {
            # "id": altered_row[0] + " " + altered_row[1],
            "id": split_block_spaced[0],
            "type": "Receiving block * src: * des: *"
        }

    elif altered_row[6] == "Served":  # Served
        return {
            # "id": altered_row[0] + " " + altered_row[1],
            "id": split_block_spaced[0],
            "type": "Served block * to *"
        }

    elif altered_row[5] == "BLOCK*" and altered_row[6] == "NameSystem.addStoredBlock:":  # BLOCK* might need to be granularized more!
        return {
            # "id": altered_row[0] + " " + altered_row[1],
            "id": split_block_spaced[0],
            "type": "blockMap updated: * is added to *"
        }
    elif altered_row[5] == "BLOCK*" and altered_row[6] == "NameSystem.allocateBlock:":
        return {
            # "id": altered_row[0] + " " + altered_row[1],
            "id": split_block_spaced[0],
            "type": "block * allocated"
        }
    elif altered_row[5] == "BLOCK*" and altered_row[6] == "NameSystem.delete:":
        return {
            # "id": altered_row[0] + " " + altered_row[1],
            "id": split_block_spaced[0],
            "type": "* is added to invalidSet of *"
        }
    elif altered_row[5] == "BLOCK*" and altered_row[6] == "ask":
        return {
            # "id": altered_row[0] + " " + altered_row[1],
            "id": split_block_spaced[0],
            "type": "ask * to replicate * to datanode(s) *"
        }
    elif altered_row[5].endswith("Transmitted"): # Transmitted
        return {
            "id": split_block_spaced[0],
            "type": "Transmitted block * to *"
        }
    elif altered_row[6] == "Starting": 
        return {
            # "id": altered_row[0] + " " + altered_row[1],
            "id": split_block_spaced[0],
            "type": "Starting thread to transfer block * to *"
        }
    elif altered_row[5] == "Deleting":
        return {
            # "id": altered_row[0] + " " + altered_row[1],
            "id": split_block_spaced[0],
            "type": "Deleting block * file *"
        }
    elif altered_row[5] == "Unexpected" and altered_row == "error":
        return {
            # "id": altered_row[0] + " " + altered_row[1],
            "id": split_block_spaced[0],
            "type": "Unexpected error trying to delete block *"
        }
    elif altered_row[6] == "exception":
        return {
            # "id": altered_row[0] + " " + altered_row[1],
            "id": split_block_spaced[0],
            "type": "Got exception while serving * to *"
        }
    else: # Default 
        return {
            # "id": altered_row[0] + " " + altered_row[1],
            "id": split_block_spaced[0],
            "type": altered_row[5]
        }


"""
Streams file passed in by user into dictionary format 
where the id is the key 
"""


def stream_file_lines(filename, kafka_producer):
    file = open(filename, "r")

    i = 0
    for row in file:
        k = to_dict(row).get("id")
        kafka_producer.send("topic_test", key=k, value=to_dict(row))
        print(to_dict(row))
        # This adjusts the rate at which the data is sent. Use a slower rate for testing your code.
        sleep(0.5)
        # For testing purposes only. Adjust i to test the number of lines you want to stream from Kafka
        if i == 2000:
            break
        i = i + 1


# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda x: dumps(x).encode("utf-8"),
    key_serializer=lambda x: x.encode("utf-8"),
)

# Streaming log file
stream_file_lines("archive/HDFS.log", producer)