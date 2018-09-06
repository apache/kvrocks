#include "server.h"
#include "anet.h"
#include <iostream>
#include <glog/logging.h>

Server::Server(int workers): _stop(false) {
  _ev = new EV(1024);
  for (int i = 0; i < workers; i++) {
    _workers.push_back(new Worker(1024));
  }
}

Server::~Server() {
  // TODO: Log the server was stopped
  std::vector<Worker*>::iterator iter;
  for (iter = _workers.begin(); iter !=_workers.end(); ++iter) {
    delete *iter;
  }
}

int Server::addConn(int cfd) {
  static uint32_t counter = 0;
  Worker * worker = _workers[counter++%_workers.size()];
  worker->publish(cfd);
  return KVROCKS_OK;
}

void Server::_acceptHandler(EV *ev, int sfd, int mask, void *clientData) {
  int port;
  char ip[128], err[ANET_ERR_LEN];

  Server *s = reinterpret_cast<Server *>(clientData);
  int cfd = anetTcpAccept(sfd, ip, sizeof(ip), &port, err);
  if (s->addConn(cfd) == KVROCKS_ERR) {
    // TODO: Log add conn error
  }
}

int Server::start() {
  char err[256];
  int sfd = anetTcpServer(6378, "0.0.0.0", 511, err);
  if (sfd == ANET_ERR) {
    LOG(ERROR) << "Failed to create tcp server, err: " << err;
    return KVROCKS_ERR;
  }
  std::vector<Worker*>::iterator iter;
  for (iter = _workers.begin(); iter !=_workers.end(); ++iter) {
    if ((*iter)->start() == KVROCKS_ERR) {
      LOG(ERROR) << "Failed to start the worker";
      return KVROCKS_ERR;
    }
  }
  if (_ev->createFileEvent(sfd, EV_READABLE, _acceptHandler, this) == EV_ERR) {
    LOG(ERROR) << "Failed to setup the accept handler";
    return KVROCKS_ERR;
  }
  _ev->dispatch();
  return KVROCKS_OK;
}

int Server::stop() {
  _stop = true;
  _ev->stop();
  std::vector<Worker*>::iterator iter;
  for (iter = _workers.begin(); iter !=_workers.end(); ++iter) {
    (*iter)->stop();
    delete *iter;
  }
}
