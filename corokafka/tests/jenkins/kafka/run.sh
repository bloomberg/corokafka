#!/bin/sh

sudo su -c "/home/ioitm/kafka_2.11-1.1.0/bin/zookeeper-server-start.sh -daemon /home/ioitm/kafka_2.11-1.1.0/config/zookeeper.properties"
echo zookeeper up

sudo su -c "/home/ioitm/kafka_2.11-1.1.0/bin/kafka-server-start.sh -daemon /home/ioitm/kafka_2.11-1.1.0/config/server.properties"
echo server up

sleep 5

for i in {1..30}
do
/home/ioitm/kafka_2.11-1.1.0/bin/kafka-topics.sh --create --topic kfk$i --zookeeper 127.0.0.1:2181 --partitions 4 --replication-factor 1
done

#/home/ioitm/kafka_2.11-1.1.0/bin/kafka-topics.sh --create --topic my-kafka-topic --zookeeper 127.0.0.1:2181 --partitions 4 --replication-factor 1

#exec /home/ioitm/kio.t.linux_64.tsk
exec /corokafka/build/tests/CoroKafkaTests.Linux
