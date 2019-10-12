#!/bin/sh
/opt/bb/share/kafka-2.2.0/zookeeper-server-start.sh -daemon /opt/bb/share/kafka-2.2.0/../config/zookeeper.properties
echo zookeeper up

/opt/bb/share/kafka-2.2.0/kafka-server-start.sh -daemon /opt/bb/share/kafka-2.2.0/../config/server.properties
echo server up

sleep 5

for i in {1..30}
do
/opt/bb/share/kafka-2.2.0/kafka-topics.sh --create --topic kfk$i --zookeeper 127.0.0.1:2181 --partitions 4 --replication-factor 1
done

exec /corokafka/build/tests/CoroKafkaTests.Linux
