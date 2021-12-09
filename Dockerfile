FROM ubuntu:18.04

RUN apt update && apt install -y libsnappy-dev
WORKDIR /kvrocks

RUN mkdir /data
COPY ./build/bin/kvrocks ./bin/
COPY ./kvrocks.conf  ./conf/
RUN sed  -i -e 's|dir /tmp/kvrocks|dir /var/lib/kvrocks|g' ./conf/kvrocks.conf
VOLUME /var/lib/kvrocks

EXPOSE 6666:6666 
ENTRYPOINT ["./bin/kvrocks", "-c", "./conf/kvrocks.conf"]
