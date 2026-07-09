#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <cerrno>

#define PORT 8080
#define MAX_EVENTS 1024
#define BUFFER_SIZE 1024

void make_sockets_nonblocking(int socket_fd){
	int flags = fcntl(socket_fd , F_GETFL , 0) ;
	if(flags == -1){
		perror("fcntl cant get flags") ;
		return ;
	}
	if(fcntl(socket_fd , F_SETFL , flags | O_NONBLOCK) == -1){
		perror("fcntl failed") ;
		return ;
	}
}

int main(){
	int server_fd = socket(AF_INET , SOCK_STREAM , 0) ;
	if(server_fd == -1){
		perror("socket creation failed") ;
		return 1 ;
	}

	int opt = 1 ;
	if(setsockopt(server_fd , SOL_SOCKET , SO_REUSEADDR , &opt , sizeof(opt)) < 0){
		perror("setsocket failed") ;
		return 1 ;
	}

	struct sockaddr_in address ;
	address.sin_family = AF_INET ;
	address.sin_addr.s_addr = INADDR_ANY ;
	address.sin_port = htons(PORT) ;

	if(bind(server_fd, (struct sockaddr*)&address , sizeof(address)) < 0){
		perror("bind error") ;
		return 1 ;
	}

	if(listen(server_fd,SOMAXCONN) < 0){
		perror("listten failed") ;
		return 1 ;
	}

	make_sockets_nonblocking(server_fd) ;

	int epoll_fd = epoll_create1(0) ;
	if(epoll_fd == -1){
		perror("epollcreate1 failed") ;
		return 1; 
	}

	struct epoll_event event , events[MAX_EVENTS] ;
	event.events = EPOLLIN ;
	event.data.fd = server_fd ;

	if(epoll_ctl(epoll_fd , EPOLL_CTL_ADD , server_fd , &event) == -1){
		perror("epoll ctl : server_fd") ;
		return 1; 
	}

	std :: cout << "Booted succesfully , listening on PORT " << PORT ;

	while (true){
		int num_events = epoll_wait(epoll_fd , events , MAX_EVENTS , -1) ;
		if(num_events == -1){
			if(errno == EINTR){
				continue;
			}else{
				perror("fatal epoll_wait error") ;
				break ;
			}
		}

		for(int i=0 ; i<num_events ; i++){

			if(events[i].data.fd == server_fd){
				struct sockaddr_in client_addr ;
				socklen_t client_len = sizeof(client_addr) ;
				int client_fd = accept(server_fd , (struct sockaddr*)& client_addr , &client_len) ;
				if(client_fd == -1){
					if(errno != EAGAIN && errno != EWOULDBLOCK){
						perror("accept failed") ;
					}
					continue;
				}

				make_sockets_nonblocking(client_fd) ;

				struct epoll_event client_event ;
				client_event.events = EPOLLIN | EPOLLET ;
				client_event.data.fd = client_fd ;

				if(epoll_ctl(epoll_fd,EPOLL_CTL_ADD,client_fd,&client_event) == -1){
					perror("epoll_ctl : client_fd") ;
					close(client_fd) ;
				}else{
					std::cout << "[Broker] New Worker Connected , FD : " << client_fd << std::endl ;
				}
			}
			else{
				int client_fd = events[i].data.fd ;
				bool keep_reading = true ;

				while(keep_reading){
					char buffer[BUFFER_SIZE] = {0} ;
					ssize_t bytes_read = read(client_fd,buffer,sizeof(buffer) - 1) ;
					
					if(bytes_read > 0){
						write(client_fd , buffer , bytes_read) ;
					}
					else if(bytes_read == 0){
						std :: cout << "[Broker] Worker disconnected. FD: " << client_fd << std :: endl ;
						close(client_fd) ;
						epoll_ctl(epoll_fd , EPOLL_CTL_DEL , client_fd , nullptr) ;
						keep_reading = false ;
					}else{
						if(errno == EAGAIN || errno == EWOULDBLOCK){
							keep_reading = false ;
						}else{
							perror("read failed") ;
							close(client_fd) ;
							epoll_ctl(epoll_fd , EPOLL_CTL_DEL , client_fd , nullptr) ;
							keep_reading = false ;
						}
					}
				}
			}
		}

	}
	close(server_fd) ;
	return 0 ;
}


