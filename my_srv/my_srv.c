#include "dbg.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <stdio.h>

int init_srv(unsigned short *port){
	int httpd = 0;
	struct sockaddr_in srv_addr;

	/*建立 socket */
	httpd = socket(PF_INET, SOCK_STREAM, 0);
	check(httpd != -1, "socket failed");

	memset(&srv_addr, 0, sizeof(srv_addr));
	srv_addr.sin_family = AF_INET;
	srv_addr.sin_port = htons(*port);
	// srv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    srv_addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    
    int i = 1;
    check(setsockopt( httpd, SOL_SOCKET, SO_REUSEADDR, (void*) &i, sizeof(i) ) >= 0, "setsockopot faild" )

    check(bind(httpd, (struct sockaddr *)&srv_addr, sizeof(srv_addr)) >= 0, "bind failed");

    /*开始监听*/  
    check(listen(httpd, 5) >= 0, "listen failed"); 
    /*返回 socket id */  
    return httpd;

error:
	if(httpd>0){
        close(httpd);
    }
    return -1;
}

/**********************************************************************/
/* Get a line from a socket, whether the line ends in a newline,
 * carriage return, or a CRLF combination.  Terminates the string read
 * with a null character.  If no newline indicator is found before the
 * end of the buffer, the string is terminated with a null.  If any of
 * the above three line terminators is read, the last character of the
 * string will be a linefeed and the string will be terminated with a
 * null character.
 * Parameters: the socket descriptor
 *             the buffer to save the data in
 *             the size of the buffer
 * Returns: the number of bytes stored (excluding null) */
/**********************************************************************/
int get_line(int sock, char *buf, int size)
{
 int i = 0;
 char c = '\0';
 int n;

 while ((i < size - 1) && (c != '\n'))
 {
  n = recv(sock, &c, 1, 0);
  /* DEBUG printf("%02X\n", c); */
  if (n > 0)
  {
   if (c == '\r')
   {
    n = recv(sock, &c, 1, MSG_PEEK);
    /* DEBUG printf("%02X\n", c); */
    if ((n > 0) && (c == '\n'))
     recv(sock, &c, 1, 0);
    else
     c = '\n';
   }
   buf[i] = c;
   i++;
  }
  else
   c = '\n';
 }
 buf[i] = '\0';
 
 return(i);
}

int main(int argc, char* argv[]){
    int srv_socket_fd = -1;
    unsigned short port = 80;
    srv_socket_fd = init_srv(&port);
    check(srv_socket_fd > 0, "start up failed");
    printf("svr started\n");

    int income_socket_fd = -1;
    struct sockaddr_in client_addr;  
    unsigned int client_addr_len = sizeof(client_addr);
    while (1)  
    {  
        /*套接字收到客户端连接请求*/  
        income_socket_fd = accept(srv_socket_fd,(struct sockaddr *)&client_addr,&client_addr_len);
        check(income_socket_fd > 0, "accept failed");
        printf("accepted srv:%d client:%d\n", srv_socket_fd, income_socket_fd);

        // char* ack = "HTTP/1.0 501 Method Not Implemented\r\nContent-Type: text/html\r\n\r\nwaliwala";
        // send(income_socket_fd, ack, strlen(ack)+1, 0);
        // printf("sent back\n");

        // close(income_socket_fd);

        // int nRecvBuf=-1;
        // int nOptBuf=-1;
        // getsockopt(income_socket_fd,SOL_SOCKET,SO_RCVBUF,&nRecvBuf,&nOptBuf);
        // printf("buf:%d opt:%d", nRecvBuf, nOptBuf);
        // nRecvBuf = 1;
        // setsockopt(srv_socket_fd,SOL_SOCKET,SO_RCVBUF,(const char*)&nRecvBuf,sizeof(int));
        // getsockopt(income_socket_fd,SOL_SOCKET,SO_RCVBUF,&nRecvBuf,&nOptBuf);
        // printf("buf:%d opt:%d", nRecvBuf, nOptBuf);

        char c = '\0';
        char buf[1000];
        // memset(buf, sizeof(buf), 0);

        int readed = 1;
        int i = 0;

        // while(readed > 0 && c != '\n'){
        while(readed > 0){ //buggy here. even the printf in loop wont show
            // printf("start readed:%d", readed); //
            readed = recv(income_socket_fd, &c, 1, 0);
            printf("readed:%d, %c\n", readed, c);
            for(int j=0;j<readed;j++){
                buf[i] = c;
                i++;
            }
        }
        buf[i] = '\0';

        printf("\nread:%s\n", buf);

        send(income_socket_fd, buf, strlen(buf), 0);

        printf("done request\n");

        close(income_socket_fd);
        
    }  
  
    printf("close srv\n");
    close(srv_socket_fd);
    return 0;

error:
    if(srv_socket_fd>0){
        printf("close svr error\n");
        close(srv_socket_fd);
    }
    return 1;

}

