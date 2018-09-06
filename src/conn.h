#ifndef KVROCKS_CONN_H
#define KVROCKS_CONN_H

#include "ev.h"
#include "kvrocks.h"
#include <stdint.h>
#include <vector>
#include <string>

#define REQ_INLINE 1
#define REQ_MULTIBULK 2

class Worker;
class Conn {
public:
  Conn(EV *ev, int fd, Worker *owner);
  ~Conn();

  int fd() { return _fd; }
  void processInputBuffer();
  int processMultibulkBuffer();
  void _readQueryFromClient();
  void _sendReplyToClient();
  void reset();
  void addReply(const char *reply, int size);
  void replyString(std::string &str);
  void replyBulkString(std::string &str);
  void replyInterger(int32_t i);
  void replyError(std::string &err);
  static void readQueryFromClient(EV *ev, int fd, int mask, void *clientData);
  static void sendReplyToClient(EV *ev, int fd, int mask, void *clientData);


private:
  int _fd;
  int _reqType;
  int _multiBulkLen;
  int _bulkLen;
  char *_rBuf;
  uint32_t _rBufLen;
  uint32_t _rBufPos;
  char *_wBuf;
  uint32_t _wBufLen;
  uint32_t _wBufPos;
  int32_t _wBufSent;
  int _lastInteraction;
  std::vector<std::string> _argv;

  Worker *_owner;
  EV *_ev;
};


#endif //KVROCKS_CONN_H