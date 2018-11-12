#ifndef KVROCKS_SERVER_H
#define KVROCKS_SERVER_H

#include <string>
#include <vector>

#include "worker.h"
#include "storage.h"

class Server {
 public:
  explicit Server(Engine::Storage *storage, int port, int workers);
  void Start();
  void Stop();
  void Join();

 private:
  std::string master_host_;
  int master_port_;

  std::vector<WorkerThread*> workers_;
  int listen_port_;
  Engine::Storage *storage_;
};

#endif //KVROCKS_SERVER_H
