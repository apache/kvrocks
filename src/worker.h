#ifndef KVROCKS_WORKER_H
#define KVROCKS_WORKER_H

#include "ev.h"
#include "conn.h"
#include "thread.h"
#include <vector>
#include <list>

class Worker : public Thread {
public:
  Worker(int setsize);
  ~Worker();

  int start();
  void stop();
  int addConn(int cfd);
  void removeConn(Conn *c);
  void publish(int cfd);
  void consume();
  void run();

private:
  EV *_ev;
  int _notifyFd;
  int _receiveFd;
  std::vector<int> _queues;
  std::list<Conn*> _conns;

  static void notify(EV *ev, int fd, int mask, void *clientData);
};

#endif //KVROCKS_WORKER_H
