#include "conn.h"
#include "worker.h"
#include "util.h"
#include <iostream>
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>
#include <glog/logging.h>

// TODO: shink the r/wbuf
Conn::Conn(EV *ev, int fd, Worker *owner): _ev(ev), _fd(fd), _owner(owner) {
  _rBuf = (char *)malloc(16 * 1024);
  _rBufLen = sizeof(_rBuf);
  _rBufPos = 0;
  _wBuf = (char *)malloc(1024);
  _wBufLen = 1024;
  _wBufPos = 0;
  _wBufSent = 0;
  _multiBulkLen = 0;
  _bulkLen = -1;
  _reqType = 0;
  _argv.clear();
}

Conn::~Conn() {
  close(_fd);
  free(_rBuf);
  free(_wBuf);
  _argv.clear();
}

int Conn::processMultibulkBuffer() {
  int pos;
  long long ll;
  char *newline;

  if (_multiBulkLen == 0) {
    // multi bulk len can't be read without a \r\n
    newline = strchr(_rBuf, '\r');
    if (!newline) {
      // TODO: reply error to client here.
      return KVROCKS_ERR;
    }
    if (newline-_rBuf > _rBufPos-2) {
      return KVROCKS_ERR;
    }
    // *1\r\n
    if (string2ll(_rBuf+1, newline-(_rBuf+1), &ll) == 0 || ll > 1024 *1024) {

      return KVROCKS_ERR;
    }
     pos = newline-_rBuf+2;
    if (ll <= 0) {
      // TODO: len == -1
      return KVROCKS_OK;
    }
    _multiBulkLen = ll;
    _argv.clear();
  }
  while(_multiBulkLen > 0) {
    if (_bulkLen == -1) {
      newline = strchr(_rBuf+pos, '\r');
      if (!newline) {
        break;
      }
    }
    if (newline - _rBuf > _rBufPos-2) {
      break;
    }
    if (_rBuf[pos] != '$') {
      // TODO: reply error
      return KVROCKS_ERR;
    }
    int ok = string2ll(_rBuf+pos+1, newline-(_rBuf+pos+1), &ll);
    if (!ok || ll < 0 || ll > 512 *1024 * 1024) {
      // TODO: reply error
      return KVROCKS_ERR;
    }
    pos = newline-_rBuf+2;
    _bulkLen = ll;
    if (_rBufPos-pos<_bulkLen+2) {
      /* Not enough data (+2 == trailing \r\n) */
      break;
    } else {
      std::string arg;
      arg.assign(_rBuf+pos, _bulkLen);
      _argv.push_back(arg);
      pos += _bulkLen+2;
      _bulkLen = -1;
      _multiBulkLen--;
    }
  }
  // trim to pos
  if (pos > 0) {
    memmove(_rBuf, _rBuf+pos, _rBufPos-pos);
    _rBufPos -= pos;
  }
  if (_multiBulkLen == 0) return KVROCKS_OK;
  return KVROCKS_ERR;
}

void Conn::reset() {
  _argv.clear();
  _multiBulkLen = 0;
  _bulkLen = -1;
  _reqType = 0;
}

void Conn::processInputBuffer() {
  while(_rBufPos > 0) {
    if (_reqType == 0) {
      _reqType = _rBuf[0] == '*' ? REQ_MULTIBULK : REQ_INLINE;
    }
    if (_reqType == REQ_MULTIBULK) {
      if (processMultibulkBuffer() == KVROCKS_ERR) break;
    } else {
    }
    if (_argv.size() > 0) {
      std::string reply = "OBJK";
      replyError(reply);
      reset();
    }
  }
}

void Conn::_readQueryFromClient() {
  int n, readLen = 16 * 1024, remain;
  remain = _rBufLen - _rBufPos + 1;
  if (remain < readLen) {
    _rBuf = (char *)realloc(_rBuf, _rBufLen+readLen);
  }
  n = read(_fd, _rBuf, readLen);
  if (n == -1) {
    if (errno == EAGAIN) {
      n = 0;
    } else {
     // TODO: Something wrong with conn, close it.
      LOG(INFO) << "Close the connection, " << strerror(errno);
      _owner->removeConn(this);
      return;
    }
  } else if (n == 0) {
    _owner->removeConn(this);
    LOG(INFO) << "Close the connection while closed by peer";
    return;
  }
  _rBufPos += n;
  // TODO: close the connection if exceed the max memory
  processInputBuffer();
}

void Conn::readQueryFromClient(EV *ev, int fd, int mask, void *clientData) {
  Conn *conn = reinterpret_cast<Conn *>(clientData);
  conn->_readQueryFromClient();
}

void Conn::_sendReplyToClient() {
  int n;
    while(_wBufPos > 0) {
      n = write(_fd, _wBuf+_wBufSent, _wBufPos-_wBufSent);
      if (n < 0) break;
      _wBufSent += n;
      if (_wBufSent == _wBufPos) {
        _wBufSent = 0;
        _wBufPos = 0;
      }
    }
    if (n == -1) {
      if (errno == EAGAIN) {
        n = 0;
      } else {
        _owner->removeConn(this);
        return;
      }
    }
    if (n > 0) {
      // update the laste interaction
    }
   if (_wBufPos == 0) {
      _wBufSent = 0;
      _ev->deleteFileEvent(_fd, EV_WRITEABLE);
    }
}

void Conn::sendReplyToClient(EV *ev, int fd, int mask, void *clientData) {
  Conn *conn = reinterpret_cast<Conn *>(clientData);
  conn->_sendReplyToClient();
}

void Conn::replyBulkString(std::string &str) {
  char buf[32];
  int len = str.size();
  ll2string(buf, sizeof(buf), len);
  addReply("$", 1);
  addReply(buf, sizeof(buf)-1);
  addReply("\r\n", 2);
  addReply(str.c_str(), str.size());
  addReply("\r\n", 2);
}

void Conn::replyString(std::string &str) {
  addReply("+", 1);
  addReply(str.c_str(), str.size());
  addReply("\r\n", 2);
}

void Conn::replyError(std::string &err) {
  addReply("-", 1);
  addReply(err.c_str(), err.size());
  addReply("\r\n", 2);
}

void Conn::addReply(const char *reply, int size) {
  int avail;

  if (_wBufPos == 0)  {
    if (_ev->createFileEvent(_fd, EV_WRITEABLE, sendReplyToClient, this) == EV_ERR) {
      return;
    }
  }
  avail = _wBufLen - _wBufPos;
  if (avail < size) {
    _wBuf = (char *)realloc(_wBuf, _wBufLen+size);
  }
  memcpy(_wBuf+_wBufPos, reply, size);
  _wBufPos += size;
}
