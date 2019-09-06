FROM ubuntu:18.04

RUN apt update && apt install -y libsnappy-dev
WORKDIR /kvrocks

RUN mkdir /data 
COPY ./build/kvrocks ./bin/
COPY ./kvrocks.conf  ./conf/

EXPOSE 6666:6666 
ENTRYPOINT ["./bin/kvrocks", "-c", "./conf/kvrocks.conf"]
