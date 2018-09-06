#include "ev.h"
#include <stdlib.h>
#include <errno.h>
#define HAVE_KQUEUE

#ifdef HAVE_EPOLL
#include "ev_epoll.cpp"
#else
#ifdef HAVE_KQUEUE
#include "ev_kqueue.cpp"
#endif
#endif

EV::EV(int setsize)
{
  _maxfd = 0;
  _setsize = setsize;
  _stop = false;
  _events = (evFileEvent*)malloc(_setsize * sizeof(evFileEvent));
  for (int i = 0; i < _setsize; i++)	{
    _events[i].mask = EV_NONE;
  }
  _fired = (evFileEvent*)malloc(_setsize * sizeof(evFileEvent));
  _apiState = new EVApiState(this);
}

EV::~EV() {
  free(_events);
  free(_fired);
}

int EV::resizeSetSize(int setsize) {
  if (setsize == _setsize) return EV_OK;
  if (_maxfd >= setsize) return EV_ERR;
  _apiState->resizeSetSize(setsize);
  _events = (evFileEvent *) realloc(_events, _setsize * sizeof(evFileEvent));
  _fired = (evFileEvent *) realloc(_fired, _setsize * sizeof(evFileEvent));
  _setsize = setsize;
  return EV_OK;
}

int EV::createFileEvent(int fd, int mask, evFileProc *proc, void *clientData) {
  if (fd >= _setsize)	{
    errno = ERANGE;
    return EV_ERR;
  }
  evFileEvent *fe = &_events[fd];
  if (_apiState->addEvent(fd, mask) == EV_ERR) {
    return EV_ERR;
  }
  fe->fd = fd;
  fe->mask |= mask;
  if (mask & EV_READABLE) fe->rfileProc = proc;
  if (mask & EV_WRITEABLE) fe->wfileProc = proc;
  fe->clientData = clientData;
  if (fd > _maxfd) {
    _maxfd = fd;
  }
  return EV_OK;
}

void EV::deleteFileEvent(int fd, int mask) {
  if (fd > _maxfd) return;
  evFileEvent *fe = &_events[fd];
  if (fe->mask == EV_NONE) return;
  _apiState->delEvent(fd, mask);
  fe->mask = fe->mask & (~mask);
  if (fd == _maxfd && fe->mask == EV_NONE) {
      int i;
      for (i = _maxfd-1; i >= 0; i--)	{
        if (_events[i].mask != EV_NONE)	break;
      }
      _maxfd = i;
  }
}

int EV::getFileEvent(int fd) {
  if (fd > _setsize) return EV_NONE;
  return _events[fd].mask;
}

int EV::processEvents() {
  int numEvents, processed = 0;

  numEvents = _apiState->poll(NULL);
  for (int i = 0; i < numEvents; i++) {
    int fd = _fired[i].fd;
    int mask = _fired[i].mask;
    bool rfired = false;
    evFileEvent *fe = &_events[fd];
    if (fe->mask & mask & EV_READABLE) {
      rfired = true;
      fe->rfileProc(this, fd, mask, fe->clientData);
    }
    if (fe->mask & mask & EV_WRITEABLE) {
      if (!rfired || fe->wfileProc != fe->rfileProc)	{
        fe->wfileProc(this, fd, mask, fe->clientData);
      }
    }
    processed++;
  }
  return processed;
}

void EV::dispatch() {
  _stop = false;
  while (!_stop) {
    processEvents();
  }
}

void EV::stop() {
  _stop = true;
}

std::string EV::getApiName() {
  return _apiState->getApiName();
}
