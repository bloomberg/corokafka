FROM artprod.dev.bloomberg.com/bbenv/dbini:latest
RUN add-dpkg openjdk-java11-linux-x86-64 java-openjdk oracle-java8-jdk-linux-x86-64 java8-jre oracle-java9-jdk-linux-x86-64 java9-jre java-jre java8-jdk java9-jdk java-jdk
RUN apt-get install -y --force-yes libbsl-dev 
RUN apt-get install -y --force-yes libuv-dev
RUN apt-get install -y --force-yes librdkafka-dev
ENV LD_LIBRARY_PATH=/opt/bb/lib64/

RUN mkdir /home/ioitm
COPY kafka_2.11-1.1.0 /home/ioitm/kafka_2.11-1.1.0
RUN mkdir /tmp/kafka-logs
COPY kiocfg_badcommon.json /home/ioitm/kiocfg_badcommon.json
COPY kiocfg.json /home/ioitm/kiocfg.json
#COPY oss/oss.Linux64.tests.tsk /home/ioitm/oss.Linux64.tests.tsk
COPY oss/oss_consumer_config.json /home/ioitm/oss_consumer_config.json
COPY oss/oss_producer_config.json /home/ioitm/oss_producer_config.json

COPY run.sh /
CMD [ "/run.sh" ]
