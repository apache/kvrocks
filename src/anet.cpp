#include "anet.h"
#include <fcntl.h>
#include <errno.h>
#include <stdio.h>
#include <stdarg.h>
#include <unistd.h>
#include <netdb.h>
#include <string.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <string>

static void anetSetError(char *err, const char *fmt, ...)
{
  va_list ap;

  if (!err) return;
  va_start(ap, fmt);
  vsnprintf(err, ANET_ERR_LEN, fmt, ap);
  va_end(ap);
}

static int anetSetBlock(int fd, bool nonBlock, char *err) {
  int flags;

  if ((flags = fcntl(fd, F_GETFL)) == -1) {
    anetSetError(err, "fcntl(F_GETFL): %s", strerror(errno));
    return ANET_ERR;
  }
  if (nonBlock) {
    flags |= O_NONBLOCK;
  } else {
    flags &= ~O_NONBLOCK;
  }
  if (fcntl(fd, F_SETFL, flags) == -1) {
    anetSetError(err, "fcntl(F_SETFL): %s", strerror(errno));
    return ANET_ERR;
  }
  return ANET_OK;
}

int anetNonBlock(int fd, char *err) {
  return anetSetBlock(fd, true, err);
}

int anetBlock(int fd, char *err) {
  return anetSetBlock(fd, false, err);
}

static int anetListen(int fd, struct sockaddr *sa, socklen_t len, int backlog, char *err) {
  if (bind(fd, sa, len) == -1) {
    anetSetError(err, "bind: %s", strerror(errno));
    close(fd);
    return ANET_ERR;
  }
  if (listen(fd, backlog) == -1) {
    anetSetError(err, "listen: %s", strerror(errno));
    close(fd);
    return ANET_ERR;
  }
  return ANET_OK;
}

static int _anetTcpServer(int port, char *bindaddr, int af, int backlog, char *err) {
  int s, rv;
  char _port[6];  /* strlen("65535") */
  struct addrinfo hints, *servinfo, *p = NULL;

  snprintf(_port,6,"%d",port);
  memset(&hints,0,sizeof(hints));
  hints.ai_family = af;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = AI_PASSIVE;    /* No effect if bindaddr != NULL */

  if ((rv = getaddrinfo(bindaddr,_port,&hints,&servinfo)) != 0) {
    anetSetError(err, "%s", gai_strerror(rv));
    return ANET_ERR;
  }
  for (p = servinfo; p != NULL; p = p->ai_next) {
    if ((s = socket(p->ai_family,p->ai_socktype,p->ai_protocol)) == -1)
      continue;
    if (anetListen(s,p->ai_addr,p->ai_addrlen,backlog, err) == ANET_ERR) {
      freeaddrinfo(servinfo);
      return ANET_ERR;
    }
    freeaddrinfo(servinfo);
    return s;
  }
  if (!p) {
    anetSetError(err, "unable to bind socket");
  }
  freeaddrinfo(servinfo);
  return ANET_ERR;
}

int anetTcpServer(int port, char *bindaddr, int backlog, char *err) {
 return _anetTcpServer(port, bindaddr, AF_INET, backlog, err);
}

static int anetGenericAccept(int s, struct sockaddr *sa, socklen_t *len, char *err) {
  int fd;
  while(1) {
   fd = accept(s, sa, len);
   if (fd == -1) {
     if (errno == EINTR) {
      continue;
     } else {
      anetSetError(err, "accept: %s", strerror(errno));
     }
   }
   break;
  }
  return fd;
}

int anetTcpAccept(int s, char *ip, int ipLen, int *port, char *err) {
  int fd;
  struct sockaddr_storage sa;
  socklen_t salen = sizeof(sa);
  if ((fd = anetGenericAccept(s,(struct sockaddr*)&sa,&salen, err)) == -1) {
   return ANET_ERR;
  }
  if (sa.ss_family == AF_INET) {
    struct sockaddr_in *s = (struct sockaddr_in *)&sa;
    if (ip) inet_ntop(AF_INET,(void*)&(s->sin_addr),ip,ipLen);
    if (port) *port = ntohs(s->sin_port);
  } else {
    struct sockaddr_in6 *s = (struct sockaddr_in6 *)&sa;
    if (ip) inet_ntop(AF_INET6,(void*)&(s->sin6_addr),ip,ipLen);
    if (port) *port = ntohs(s->sin6_port);
  }
  return fd;
}
