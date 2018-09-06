#include "worker.h"
#include "ev.h"
#include <iostream>
#include <unistd.h>
#include <glog/logging.h>

Worker::Worker(int setsize) {
  _ev = new EV(setsize);
}

Worker::~Worker() {
  delete _ev;
}

int Worker::start() {
  int fds[2];
  if (pipe(fds) == -1) {
    LOG(ERROR) << "Failed to create pipe, err: " << strerror(errno);
    return KVROCKS_ERR;
  }
  // Data written to fds[1] appears on fds[0]
  _receiveFd = fds[0];
  _notifyFd = fds[1];
  if (_ev->createFileEvent(_receiveFd, EV_READABLE, notify, this) == EV_ERR) {
    LOG(ERROR) << "Failed to create notify event in worker";
    return KVROCKS_ERR;
  }
  return Thread::start();
}

void Worker::stop() {
  _ev->stop();
  // TODO: remove all connections
}

int Worker::addConn(int cfd) {
  Conn *c = new Conn(_ev, cfd, this);
  if (_ev->createFileEvent(cfd, EV_READABLE, c->readQueryFromClient, c) == EV_ERR) {
    return KVROCKS_ERR;
  }
  _conns.push_back(c);
  return KVROCKS_OK;
}

void Worker::removeConn(Conn *c) {
  _ev->deleteFileEvent(c->fd(), EV_READABLE|EV_WRITEABLE);
  _conns.remove(c);
  delete c;
}

void Worker::notify(EV *ev, int fd, int mask, void *clientData) {
  char buf[1];

  Worker *worker = reinterpret_cast<Worker*> (clientData);
  int n = read(fd, buf, sizeof(buf));
  if (n <= 0 || buf[0] != 'p') {
    // do nothing when read error
    return;
  }
  worker->consume();
}

void Worker::publish(int cfd) {
  _queues.push_back(cfd);
  char buf[1] = {'p'};
  write(_notifyFd, buf, 1);
}

void Worker::consume() {
  int cfd;
  while(!_queues.empty()) {
    cfd = _queues.front();
    _queues.erase(_queues.begin());
    if (cfd <= 0) {
      continue;
    }
    if (addConn(cfd) == KVROCKS_ERR) {
      close(cfd);
      LOG(INFO) << "Failed to add connection, while unable to create file event";
    }
  }
}

void Worker::run() {
  _ev->dispatch();
}
