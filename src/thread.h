#ifndef KVROCKS_THREAD_H
#define KVROCKS_THREAD_H

#include "kvrocks.h"
#include <pthread.h>

class Thread {
public:
  Thread(): _stop(true) {}

  ~Thread() {
    stop();
  }

  virtual void run() = 0;

  int start() {
    if (!_stop) {
      return -1;
    }
    _stop = true;
    return pthread_create(&_th, NULL, startRoutine, (Thread *)this);
  }

  void stop() {
    if (!_stop) {
      _stop = true;
      pthread_join(_th, NULL);
    }
  }

  bool isStop() {
   return _stop;
  }

private:
  pthread_t _th;
  bool _stop;

  static void *startRoutine(void *arg) {
    Thread *t = reinterpret_cast<Thread*>(arg);
    t->run();
    return NULL;
  }
};

#endif //KVROCKS_THREAD_H
