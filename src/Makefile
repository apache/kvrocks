CXX=g++
CFLAGS=-std=c++98
CINCLUDES=-I../deps/glog/src
CLIBS=../deps/glog/.libs/libglog.a -lpthread

CXX+=$(CFLAGS) $(CINCLUDES)

PROG=kvrocks

OBJS= anet.o conn.o ev.o ev_epoll.o kvrocks.o server.o util.o worker.o

all: kvrocks
.PHONY: all

anet.o: anet.cpp anet.h
conn.o: conn.cpp conn.h ev.h kvrocks.h worker.h thread.h util.h
ev.o: ev.cpp ev.h ev_epoll.cpp
ev_epoll.o: ev_epoll.cpp ev.h
kvrocks.o: kvrocks.cpp server.h ev.h conn.h kvrocks.h worker.h thread.h
server.o: server.cpp server.h ev.h conn.h kvrocks.h worker.h thread.h \
 anet.h
util.o: util.cpp util.h
worker.o: worker.cpp worker.h ev.h conn.h kvrocks.h thread.h

kvrocks: $(OBJS)
    $(CXX) -o $(PROG) $(OBJS) $(CLIBS)

clean:
    rm -rf *.o $(GLOG)
