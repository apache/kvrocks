#include "ev.h"
#include <string>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <sys/epoll.h>

class EVApiState {
public:
	EVApiState(EV *base) : _base(base) {
		_events = (struct epoll_event*)malloc(sizeof(struct epoll_event) *_base->_setsize);
		_epfd = epoll_create(1024);
	}

	~EVApiState() {
	  free(_events);
		if (_epfd != -1) close(_epfd);
	}

	int resizeSetSize(int setsize) {
		_events = (struct epoll_event*) realloc(_events, sizeof(struct epoll_event) *_base->_setsize);
		return EV_OK;
	}

	int addEvent(int fd, int mask) {
		struct epoll_event ee;
		int op = _base->_events[fd].mask == EV_NONE ? EPOLL_CTL_ADD:EPOLL_CTL_MOD;
		ee.events = 0;
		mask |= _base->_events[fd].mask;
		if (mask & EV_READABLE) ee.events |= EPOLLIN;
		if (mask & EV_WRITEABLE) ee.events |= EPOLLOUT;
		ee.data.u64 = 0;
		ee.data.fd = fd;
		if (epoll_ctl(_epfd, op, fd, &ee) == -1) {
			return EV_ERR;
		}
		return EV_OK;
	}

	void delEvent(int fd, int delmask) {
		struct epoll_event ee;
		int mask = _base->_events[fd].mask & (~delmask);
		ee.events = 0;
		ee.data.fd = fd;
		if (mask & EV_READABLE) ee.events |= EPOLLIN;
		if (mask & EV_WRITEABLE) ee.events |= EPOLLOUT;
		if (mask != EV_NONE) {
			epoll_ctl(_epfd, EPOLL_CTL_MOD, fd, &ee);
		} else {
			/* Note, Kernel < 2.6.9 requires a non null event pointer even for
 			 * EPOLL_CTL_DEL. */
			epoll_ctl(_epfd, EPOLL_CTL_DEL, fd, &ee);
		}
	}

	int poll(struct timeval *tvp) {
	  int retval, numEvents = 0;

	  retval = epoll_wait(_epfd, _events, _base->_setsize,
	  				tvp ? (tvp->tv_sec*1000+tvp->tv_usec/1000):-1);
	  if (retval > 0) {
	  	numEvents = retval;
	 		for (int i = 0; i < numEvents; i++) {
	 			int mask = 0;
	 			struct epoll_event *ee = &_events[i];
	 			if (ee->events & EPOLLIN) mask |= EV_READABLE;
				if (ee->events & EPOLLOUT) mask |= EV_WRITEABLE;
				if (ee->events & EPOLLERR) mask |= EV_WRITEABLE;
				if (ee->events & EPOLLHUP) mask |= EV_WRITEABLE;
				_base->_fired[i].fd = ee->data.fd;
        _base->_fired[i].mask = mask;
	 		}
	  }
		return numEvents;
	}

	std::string getApiName() {
		return "epoll";
	}

private:
	int _epfd;
	struct epoll_event *_events;
	EV *_base;
};
