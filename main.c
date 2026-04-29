#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#define MAX_CLIENTS 100

typedef struct {
    int fd;
    char name[33];
    char status[65];
    int active; //1 if slot is being used, 0 if empty
} User;

//sets max amount of people that can access at a time
User clients[MAX_CLIENTS]; //In other words, the room basically

//Mutex allows only one thread to access the code at a given time, basically locking and unlocking it
pthread_mutex_t clients_mutex = PTHREAD_MUTEX_INITIALIZER;

//reads from fd into a buffer until it hits the '|'
int read_until_bar(int fd, char *dest, int max_len) {
    int total = 0;
    char c;
    while(total < max_len -1) {
        int n = read(fd, &c, 1); //reads stream character by character into c to find the '|'
        if(n <= 0) {
            return -1; //conection was lost
        }
        if (c == '|') {
            dest[total] = '\0';
            return total;
        }
        dest[total++] = c;
    }
    return -1; //There was a buffer overflow
}

void broadcast(const char *message, int sender_fd) {
    pthread_mutex_lock(&clients_mutex);
    for(int i = 0; i < MAX_CLIENTS; i++) {
        if(clients[i].fd != -1 && clients[i].fd != sender_fd) {
            write(clients[i].fd, message, strlen(message));
        }

    }
    pthread_mutex_unlock(&clients_mutex);
}

void* handle_client(void* arg) {
    int client_fd = *((int *)arg);
    free(arg);

    char version[4];
    char type[4];
    char length_str[10];
    char name_buffer[33];

    //Phase 1: Le identification

    //1. Read Version (Should read 1)
    if (read_until_bar(client_fd, version, sizeof(version)) < 0) {
        close(client_fd); 
        return NULL;
    }

    //2. Read type (Should read NAM) bc user is trying to log in
    if(read_until_bar(client_fd, type, sizeof(type)) < 0) {
        close(client_fd);
        return NULL;
    }

    //3. reads the length of the following string / remaining bytes in message
    if(read_until_bar(client_fd, length_str, sizeof(length_str)) < 0) {
        close(client_fd);
        return NULL;
    }

    int body_len = atoi(length_str);

    //4. read the actual name
    int n = read(client_fd, name_buffer, body_len);
    if (n <= 0) {
        close(client_fd);
        return NULL;
    }
    name_buffer[n-1] = '\0'; //removes le final trailing '|' and replaces it with a null terminator

    //Phase 2: Le Name Validation

    pthread_mutex_lock(&clients_mutex); //locks other threads while this one runs so duplicate names don't happen
    int name_taken = 0;
    int my_index = -1;

    for(int i = 0; i < MAX_CLIENTS; i++) {
        if(clients[i].active && strcmp(clients[i].name, name_buffer) == 0) {
            name_taken = 1; //makes it so no one else can have that username
            break;
        }
        //if username is not occupied
        if (my_index == -1 && !clients[i].active) {
            my_index = i;
        }
    }

    //runs if name is taken
    if (name_taken || my_index == -1) {
        pthread_mutex_unlock(&clients_mutex); //unlocks mutex so other threads can run
        char *err = "1|ERR|11|1|Name in use|";
        write(client_fd, err, strlen(err));
        close(client_fd);
        return NULL;
    }

    //If succeeds and name is not taken, occupies seat
    clients[my_index].fd = client_fd;
    clients[my_index].active = 1;
    strncpy(clients[my_index].name, name_buffer, 32);
    pthread_mutex_unlock(&clients_mutex);


    //Sends welcome

    char welcome[100];
    sprintf(welcome, "1|MSG|%ld|#all|%s|Welcome!|", 13 + strlen(name_buffer), name_buffer);
    write(client_fd, welcome, strlen(welcome));

    char buffer[1024]; //stores incoming data from user chats
    int bytes_read;

    //Phase 3: Le protocol Loop

    //makes the thread block until the client hits enter in the terminal
    //essentially if it breaks, the other person hung up and we can close the connection/thread
    while (1) {

        //reads the header piece
        if(read_until_bar(client_fd, version, sizeof(version)) < 0) break;
        if(read_until_bar(client_fd, type, sizeof(type)) < 0) break;
        if(read_until_bar(client_fd, length_str, sizeof(length_str)) < 0) break;

        int msg_len = atoi (length_str);
        char *msg_body = malloc(msg_len + 1);

        //reads msg_len bytes for body
        int total_received = 0;
        while (total_received < msg_len) {
            int n = read(client_fd, msg_body + total_received, msg_len - total_received);
            if(n <= 0) {
                break;
            }
            total_received += n;
        }

        msg_body[msg_len] = '\0'; //adds safety null terminator

        if(strcmp(type, "MSG") == 0 ) {

            char final_broadcast[2048];

            snprintf(final_broadcast, sizeof(final_broadcast), "1|MSG|%d|%s|#all|%s",
                    (int)(strlen(clients[my_index].name) + 6 + strlen(msg_body)), 
                    clients[my_index].name, msg_body);

                    broadcast(final_broadcast, client_fd);
        }

        else if (strcmp(type, "WHO") == 0) {
            //Loop through clients[] and send names back to this client_fd
            //RYAN
        }
        else if (strcmp(type, "SET") == 0) {
            //update clients[my_index].status
            //RYAN
        }

        free(msg_body);

    }

    printf("Client on FD %d disconnected.\n", clients[my_index].name);
    pthread_mutex_lock(&clients_mutex);
    clients[my_index].active = 0; //emptying the seat
    clients[my_index].fd = -1;
    pthread_mutex_unlock(&clients_mutex);
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

   pthread_mutex_lock(&clients_mutex);
   for(int i = 0; i < MAX_CLIENTS; i++) {
    clients[i].fd = -1;
    clients[i].active = 0;
    memset(clients[i].name, 0, sizeof(clients[i].name));
   }
   pthread_mutex_unlock(&clients_mutex);


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
