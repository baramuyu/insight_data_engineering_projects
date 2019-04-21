#!/bin/sh
# delete and create "paid-transaction" kafka topic.

/usr/local/kafka/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic "paid-transaction"
/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 2 --partitions 2 --topic "paid-transaction"
echo "finished"