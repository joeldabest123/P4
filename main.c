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

        //skips leading newlines
        if (c == '\n' || c == '\r') continue;
        
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
        if(clients[i].active && clients[i].fd != sender_fd) {
            write(clients[i].fd, message, strlen(message));
        }

    }
    pthread_mutex_unlock(&clients_mutex);
}

void* handle_client(void* arg) {
    int client_fd = *((int *)arg);
    free(arg);

    char version[16];
    char type[16];
    char length_str[16];
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

    if(body_len<1||body_len>33) {
        char *err = "1|ERR|9|0|Bad Name|";
        write(client_fd, err, strlen(err));
        close(client_fd);
        return NULL;
    }

    int n = 0;
    int total = 0;

    while (total<body_len) {
        n = read(client_fd, name_buffer+total, body_len-total);
        if (n<=0) {
            close(client_fd);
            return NULL;
        }
        total+=n;
    }

    //strips trailing |
    if (total > 0 && name_buffer[total-1] == '|') {
        name_buffer[total-1] = '\0';
    } else {
        name_buffer[total] = '\0';
    }

    printf("User logged in: %s\n", name_buffer);

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

    char welcome[256];
    int welc_body = (int) (strlen("#all")+1+strlen(name_buffer)
        +1+strlen("Welcome to the chat!")+1);
    
    snprintf(welcome, sizeof(welcome), "1|MSG|%d|#all|%s|Welcome to the chat!|", 
        welc_body, name_buffer);

    write(client_fd, welcome, strlen(welcome));


    //Phase 3: Le protocol Loop

    //makes the thread block until the client hits enter in the terminal
    //essentially if it breaks, the other person hung up and we can close the connection/thread
    while (1) {

        //reads the header piece
        if(read_until_bar(client_fd, version, sizeof(version)) < 0) break;
        if(read_until_bar(client_fd, type, sizeof(type)) < 0) break;
        if(read_until_bar(client_fd, length_str, sizeof(length_str)) < 0) break;

        int msg_len = atoi (length_str);
        char *msg_body = malloc(msg_len + 2); //added another +1 for safety or missing trail bar
        if (!msg_body) {
            break;
        }

        //reads msg_len bytes for body
        int total_received = 0;
        int conn_lost = 0;
        while (total_received < msg_len) {
            int n = read(client_fd, msg_body + total_received, msg_len - total_received);
            if(n <= 0) {
                conn_lost=1;
                break;
            }
            total_received += n;
        }

        msg_body[total_received] = '\0'; //adds safety null terminator

        if(conn_lost){
            free(msg_body);
            break;
        }

        if(strcmp(type, "MSG") == 0 ) {

            char recipient[34] = {0};
            char text[82] = {0};
            char *ptr = msg_body;
            if (*ptr == '|') ptr++;

            //find up to first |
            char *bar1=strchr(ptr, '|');
            if(!bar1) { 
                free(msg_body);
                continue;
            }

            int rlen=(int)(bar1-ptr); //recipient length
            if(rlen>32) {
                rlen=32;
            }
            strncpy(recipient, ptr, rlen);
            recipient[rlen]='\0';

            //text after bar1 but before last |
            char *txt_strt = bar1+1;
            char *last_bar = strrchr(txt_strt,'|');

            //text length
            int tlen= last_bar ? (int)(last_bar - txt_strt) : (int)strlen(txt_strt);

            if(tlen>80){
                tlen=80;
            }

            strncpy(text, txt_strt, tlen);
            text[tlen]='\0';

            int text_len = (int)strlen(text);
            while (text_len>0 && (text[text_len-1]=='\n' || text[text_len-1]=='\r')) {
                text[--text_len] = '\0';
            }

            char final_broadcast[2048];
            
            int body_size = (int)(strlen(clients[my_index].name)+1 + strlen(recipient) + 1 + strlen(text)+1);
            

            snprintf(final_broadcast, sizeof(final_broadcast), "1|MSG|%d|%s|%s|%s|",
                    body_size, clients[my_index].name, recipient, text);

            //ALL for public message, else private message
            if (strcmp(recipient, "#all")==0) {
                broadcast(final_broadcast, client_fd);
            }
            else {
                pthread_mutex_lock(&clients_mutex);
                int found = 0;
                for(int i = 0; i<MAX_CLIENTS; i++) {
                    if(clients[i].active && strcmp(clients[i].name, recipient)==0) {
                        write(clients[i].fd, final_broadcast, strlen(final_broadcast));
                        found=1;
                        break;
                    }
                }
                pthread_mutex_unlock(&clients_mutex);

                //unknown recipient error
                if(!found) {
                    char errbuff[128];
                    snprintf(errbuff,sizeof(errbuff),
                        "1|ERR|%d|2|Unknown Recipient|",
                        (int)strlen("2|Unknown Recipient|"));
                    write(client_fd, errbuff, strlen(errbuff));
                }
            }

            //broadcast(final_broadcast, client_fd);
        }

        else if (strcmp(type, "WHO") == 0) {
            //Loop through clients[] and send names back to this client_fd
            //RYAN

            //strip trailing |
            char query[34] = {0};
            char *qlast = strrchr(msg_body, '|');
            int qlen = qlast ? (int)(qlast - msg_body) : (int)strlen(msg_body);

            if(qlen>32) {
                qlen=32;
            }

            strncpy(query, msg_body, qlen);
            query[qlen]='\0';

            int qslen = (int)strlen(query);
            while (qslen>0 && (query[qslen-1]=='\n' || query[qslen-1]=='\r')) {
                query[--qslen] = '\0';
            }

            char response_txt[999999]; //100 users with 100 chars idk

            response_txt[0]='\0';

            pthread_mutex_lock(&clients_mutex);


            //if all is query, every active user is listed
            if(strcmp(query, "#all")==0){
                int first=1;
                for(int i = 0; i<MAX_CLIENTS; i++) {
                    if(!clients[i].active) { continue; }
                    if(!first) strcat(response_txt, "\n");
                    first = 0;

                    if(clients[i].status[0]!='\0') {
                        //if status, listed in form of name: status
                        strcat(response_txt, clients[i].name);
                        strcat(response_txt, ": ");
                        strcat(response_txt, clients[i].status);
                    }
                    else {
                        //else, listed in form of just name
                        strcat(response_txt, clients[i].name);
                    }
                }
            } 
            else {
                //look up user
                int found = 0;
                for (int i = 0; i<MAX_CLIENTS; i++) {
                    if(clients[i].active && strcmp(clients[i].name, query)==0) {
                        found=1;
                        if(clients[i].status[0]!='\0') {
                            snprintf(response_txt, sizeof(response_txt),
                                "%s: %s", clients[i].name, clients[i].status);
                        }
                        else{
                            snprintf(response_txt, sizeof(response_txt), "No status");
                        }
                        break; //?
                    }
                }

                if(!found) {
                    pthread_mutex_unlock(&clients_mutex);
                    char errbuf[128];
                    snprintf(errbuf, sizeof(errbuf), "1|ERR|%d|2|Unknown Recipient|", (int)strlen("2|Unknown Recipient|"));
                    write(client_fd, errbuf, strlen(errbuf));
                    free(msg_body);
                    continue;
                }
            }

            pthread_mutex_unlock(&clients_mutex);

            //build/send MSG response
            pthread_mutex_lock(&clients_mutex);
            char name_2[33];
            strncpy(name_2, clients[my_index].name, 32);
            name_2[32]='\0';
            pthread_mutex_unlock(&clients_mutex);

            int rbody = (int)(strlen("#all")+1+strlen(name_2)+1+strlen(response_txt)+1);
            char *rbuf = malloc(rbody+64);

            if(rbuf) {

                snprintf(rbuf,rbody+64, "1|MSG|%d|#all|%s|%s|", rbody,name_2,response_txt);
                write(client_fd,rbuf,strlen(rbuf));
                free(rbuf);
            }

        }
        else if (strcmp(type, "SET") == 0) {
            //update clients[my_index].status
            //RYAN

            char newstat[66]={0};

            //strip trailing |
            char *slast = strrchr(msg_body, '|');
            int slen=slast ? (int)(slast - msg_body):(int)strlen(msg_body);
            
            //if its too long
            if(slen>64){
                char errbuf[64];
                snprintf(errbuf, sizeof(errbuf), "1|ERR|%d|4|Too Long|",
                    (int)strlen("4|Too Long|"));
                write(client_fd, errbuf, strlen(errbuf));
                free(msg_body);
                continue;
            }

            strncpy(newstat, msg_body, slen);
            newstat[slen]='\0';

            //update status

            int nslen = (int)strlen(newstat);
            
            while(nslen>0 && (newstat[nslen-1]=='\n' || newstat[nslen-1]=='\r')) {
                newstat[--nslen] = '\0';
            }

            pthread_mutex_lock(&clients_mutex);
            strncpy(clients[my_index].status, newstat, 64);
            clients[my_index].status[64]='\0';
            char name_2[33];
            strncpy(name_2, clients[my_index].name, 32);
            name_2[32]='\0';
            pthread_mutex_unlock(&clients_mutex);

            //broadcast change
            if (newstat[0]!='\0') {
                char announce[256];
                snprintf(announce, sizeof(announce), "%s is now \"%s\"",
                    name_2, newstat);
                int abody=(int) (strlen("#all")+1+strlen("#all")+1+strlen(announce)+1);
                char *abuf = malloc(abody+32);

                if(abuf){
                    snprintf(abuf,abody+32, "1|MSG|%d|#all|#all|%s|", abody, announce);

                    broadcast(abuf,-1);
                    free(abuf);
                }
            }
        }

        else{
            char *err = "1|ERR|12|0|Unknown Type|";
            write(client_fd, err, strlen(err));
            free(msg_body);
            break;
        }

        free(msg_body);

    }

    printf("Client %s disconnected.\n", clients[my_index].name);
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

   int opt=1;
   setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));


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
    memset(clients[i].status, 0, sizeof(clients[i].status));
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
