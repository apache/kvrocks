#include "ev.h"
#include <string>
#include <unistd.h>
#include <sys/event.h>
#include <sys/time.h>

class EVApiState {
public:
	EVApiState(EV *base) : _base(base) {
		_events = (struct kevent *)malloc(sizeof(struct kevent) *_base->_setsize);
		_kqfd = kqueue();
	}

	~EVApiState() {
	  free(_events);
		if (_kqfd != -1) close(_kqfd);
	}

	int resizeSetSize(int setsize) {
		_events = (struct kevent *) realloc(_events, sizeof(struct kevent) *_base->_setsize);
		return EV_OK;
	}

	int addEvent(int fd, int mask) {
		struct kevent ke;
		if (mask & EV_READABLE) {
			EV_SET(&ke, fd, EVFILT_READ, EV_ADD, 0, 0, nullptr);
			if (kevent(_kqfd, &ke, 1, nullptr, 0, nullptr) == -1) {
				return EV_ERR;
			}
		}
		if (mask & EV_WRITEABLE) {
			EV_SET(&ke, fd, EVFILT_WRITE, EV_ADD, 0, 0, nullptr);
			if (kevent(_kqfd, &ke, 1, nullptr, 0, nullptr) == -1) {
				return EV_ERR;
			}
		}
		return EV_OK;
	}

	void delEvent(int fd, int mask) {
		struct kevent ke;
		if (mask & EV_READABLE) {
			EV_SET(&ke, fd, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
			kevent(_kqfd, &ke, 1, nullptr, 0, nullptr);
		}
		if (mask & EV_READABLE) {
			EV_SET(&ke, fd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
			kevent(_kqfd, &ke, 1, nullptr, 0, nullptr);
		}
	}

	int poll(struct timeval *tvp) {
		int ret, numEvents = 0;
		if (tvp) {
			struct timespec timeout;
			timeout.tv_sec = tvp->tv_sec;
			timeout.tv_nsec = tvp->tv_usec*1000;
			ret = kevent(_kqfd, nullptr, 0, _events, _base->_setsize, &timeout);
		} else {
			ret = kevent(_kqfd, nullptr, 0, _events, _base->_setsize, nullptr);
		}
		if (ret > 0) {
			numEvents = ret;
			for(int i = 0; i < numEvents; i++) {
				int mask = EV_NONE;
				struct kevent *ke = &_events[i];
				if (ke->filter == EVFILT_READ) mask |= EV_READABLE;
				if (ke->filter == EVFILT_WRITE) mask |= EV_WRITEABLE;
				_base->_fired[i].fd = ke->ident;
				_base->_fired[i].mask = mask;
			}
		}
		return numEvents;
	}

	std::string getApiName() {
		return "kqueue";
	}

private:
	int _kqfd;
	struct kevent *_events;
	EV *_base;
};
