#ifndef KVROCKS_SERVER_H
#define KVROCKS_SERVER_H

#include "ev.h"
#include "conn.h"
#include "worker.h"
#include <vector>

class Server {
public:
  Server(int workers);
  ~Server();
  int start();
  int addConn(int cfd);
  int stop();

private:
  bool _stop;
  EV *_ev;
  std::vector<Worker*> _workers;

  static void _acceptHandler(EV *ev, int sfd, int mask, void *clientData);
};

#endif //KVROCKS_SERVER_H
