# Python Kafka Client
An adaptation of Confluent Kafka Python client. Supports Basic and SASL authentication. Tested against IBM Event Stream with both producer and consumer mode. 

python3 consumer-lean.py -b "<List of all supported broker servers, comma separated>" -m PLAIN -u <User ID, this is ‘token’> -s <Password> -t <Topic> | grep -i ‘<Filter String>’ > rawmessage_log.txt
