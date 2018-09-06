#ifndef KVROCKS_ANET_H
#define KVROCKS_ANET_H
#define ANET_OK 0
#define ANET_ERR -1
#define ANET_ERR_LEN 256

int anetBlock(int fd, char *err);
int anetNonBlock(int fd, char *err);
int anetTcpServer(int port, char *bindaddr, int backlog, char *err);
int anetTcpAccept(int s, char *ip, int ipLen, int *port, char *err);
#endif //KVROCKS_ANET_H
