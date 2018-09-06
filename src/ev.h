#ifndef KVROCKS_EV_H
#define KVROCKS_EV_H


#include <string>

#define EV_OK 0
#define EV_ERR -1

#define EV_NONE 0
#define EV_READABLE 1
#define EV_WRITEABLE 2

class  EV;
class EVApiState;

typedef void evFileProc(EV *ev, int fd, int mask, void *clientData);

typedef struct evFileEvent {
	int fd;
	int mask;
	evFileProc *rfileProc;
	evFileProc *wfileProc;
	void *clientData;
} evFileEvent;

class EV {
public:
	EV(int setsize);
	~EV();
	int resizeSetSize(int setsize);
	int createFileEvent(int fd, int mask, evFileProc *proc, void *clientData);
	void deleteFileEvent(int fd, int mask);
	int getFileEvent(int fd);
	int processEvents();
	void dispatch();
	void stop();
	std::string getApiName();

private:
	int _setsize;
	int _maxfd;
	bool _stop;
	evFileEvent *_events; // Registered events
	evFileEvent *_fired; // Fired events
	EVApiState *_apiState;

	friend class EVApiState;
};

#endif //KVROCKS_EV_H
