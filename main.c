#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

void* handle_client(void* arg) {
    int client_fd = *((int *)arg);
    free(arg);

    //chat protocol lives in here
    printf("Thread started for FD %d\n", client_fd);

    char *welcome = "1|MSG|18|#all|#all|Welcome!|";
    write(client_fd, welcome, strlen(welcome));

    char buffer[1024]; //stores incoming data from user chats
    int bytes_read;

    //makes the thread block until the client hits enter in the terminal
    //essentially if read returns 0, the other person hung up and we can close the connection/thread
    while ((bytes_read = read(client_fd, buffer, sizeof(buffer) - 1)) > 0) {
        buffer[bytes_read] = '\0'; //ends each set of values from stream with a null terminator so it can turn them into strings

        printf("Received from FD %d: %s", client_fd, buffer);

        //will put Protocol parser here later
        write(client_fd, "Server received: ", 17);
        write(client_fd, buffer, bytes_read); 

    }

    printf("Client on FD %d disconnected.\n", client_fd);
    close(client_fd);
    return NULL;

}

int main (int argc, char* argv[]) {

    if(argc != 2) {
        fprintf(stderr, "Usage: %s <port>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

   struct sockaddr_in address; //_in because it's IPv4-specific
   int port = atoi(argv[1]);

   memset(&address, 0, sizeof(address)); //zeroes out address to remove garbage values

   address.sin_family = AF_INET; //sets to IPV4
   address.sin_addr.s_addr = INADDR_ANY; //binds to all available interfaces like Wifi, Ethernet, etc
   address.sin_port = htons(port); //sets host byte to big-endian

   //creates socket
   int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
   if (listen_fd < 0) {
    perror("Socket creation has failed F");
    exit(EXIT_FAILURE);
   }

   //binding socket to port or address (whichever floats your boat)
   if(bind(listen_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
    perror("Binding has failed F");
    exit(EXIT_FAILURE);
   }

   //listens for connections with a possible backlog of 10 connections for overflowing reason yk
   if(listen(listen_fd, 10) < 0) {
    perror("Listening has failed");
    exit(EXIT_FAILURE);
   } 

   printf("Server is listening on port %d...\n", port);

   //accept loop which waits for someone to connect to move forward
   struct sockaddr_in client_addr; //empty until someone connects where it copies their ip information
   socklen_t addr_len = sizeof(client_addr);

   //runs loop perpetually for the chat server
   //sits looping until someone connects and completes the TCP Three-Way Handshake for cool people
   while(1) {
    //assigns a number to each client for distinction
    int client_fd = accept(listen_fd, (struct sockaddr *)&client_addr, &addr_len);

    if(client_fd < 0) {
        perror("Accept failed");
        continue; //doesn't end it, just keeps going to the next one since we are running a chat server here
    }

    printf("New connection accepted!\n");

    int *new_sock = malloc(sizeof(int)); //Allocates dynamically so all of the threads don't overwrite one another
    *new_sock = client_fd;

    pthread_t thread_id;
    if (pthread_create(&thread_id, NULL, handle_client, (void*)new_sock) < 0) {
        perror("Could not create the thread, L");
        free(new_sock);
    }

    pthread_detach(thread_id); //cleans up resources automatically thank god
   }


}
