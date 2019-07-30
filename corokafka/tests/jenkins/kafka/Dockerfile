FROM artprod.dev.bloomberg.com/bbenv/nano

RUN add-dpkg kafka-2.2.0
RUN apt-get install -y --force-yes librdkafka-dev
RUN ldconfig

COPY config/* /opt/bb/share/kafka-2.2.0/../config/
RUN chmod -R 777 /opt/bb/share/kafka-2.2.0/
RUN chmod -R 777 /opt/bb/share/config/

RUN mkdir /opt/bb/share/logs/
RUN chmod -R 777 /opt/bb/share/logs/

COPY run.sh /
CMD [ "/run.sh" ]
