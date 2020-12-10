#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include "./comm.h"
#include "./db.h"

/*
 * Use the variables in this struct to synchronize your main thread with client
 * threads. Note that all client threads must have terminated before you clean
 * up the database.
 */
typedef struct server_control {
    pthread_mutex_t server_mutex;
    pthread_cond_t server_cond;
    int num_client_threads;
} server_control_t;

/*
 * Controls when the clients in the client thread list should be stopped and
 * let go.
 */
typedef struct client_control {
    pthread_mutex_t go_mutex;
    pthread_cond_t go;
    int stopped;
} client_control_t;

/*
 * The encapsulation of a client thread, i.e., the thread that handles
 * commands from clients.
 */
typedef struct client {
    pthread_t thread;
    FILE *cxstr;  // File stream for input and output

    // For client list
    struct client *prev;
    struct client *next;
} client_t;

/*
 * The encapsulation of a thread that handles signals sent to the server.
 * When SIGINT is sent to the server all client threads should be destroyed.
 */
typedef struct sig_handler {
    sigset_t set;
    pthread_t thread;
} sig_handler_t;

server_control_t server = {PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER,
                           0};

client_control_t client_control = {PTHREAD_MUTEX_INITIALIZER,
                                   PTHREAD_COND_INITIALIZER, 0};
int server_active = 0;
client_t *thread_list_head = NULL;
pthread_mutex_t thread_list_mutex = PTHREAD_MUTEX_INITIALIZER;

void *run_client(void *arg);
void *monitor_signal(void *arg);
void thread_cleanup(void *arg);

/*
This function unlocks a mutex and used in a cleanup handler to make sure
the current thread is unlock
*/
void clean_up_pthread_mutex(void *arg) {
    // calls pthread_mutex_unlock on the given mutex in arg
    pthread_mutex_unlock((pthread_mutex_t *)arg);
}
// Called by client threads to wait until progress is permitted
void client_control_wait() {
    // error variable
    int err;
    // lock client control mutex
    if ((err = pthread_mutex_lock(&client_control.go_mutex)) != 0) {
        handle_error_en(err, "pthread_mutex_lock");
    }
    // push clean up thread mutex handler
    pthread_cleanup_push(&clean_up_pthread_mutex, &client_control.go_mutex);
    // wait for client control stopped to == 1
    while (client_control.stopped == 1) {
        // waits for the condition variable to change
        if ((err = pthread_cond_wait(&client_control.go,
                                     &client_control.go_mutex)) != 0) {
            handle_error_en(err, "pthread_cond_wait");
        }
    }
    // pop the cleanup handler
    pthread_cleanup_pop(1);
}

// Called by main thread to stop client threads
void client_control_stop() {
    // error variable
    int err;
    // lock client control mutex
    if ((err = pthread_mutex_lock(&client_control.go_mutex)) != 0) {
        handle_error_en(err, "pthread_mutex_lock");
    }
    // change stopped to 1
    client_control.stopped = 1;
    // unlock client control mutex
    if ((err = pthread_mutex_unlock(&client_control.go_mutex)) != 0) {
        handle_error_en(err, "pthread_mutex_unlock");
    }
}

// Called by main thread to resume client threads
void client_control_release() {
    // error variable
    int err;
    // lock client control mutex
    if ((err = pthread_mutex_lock(&client_control.go_mutex)) != 0) {
        handle_error_en(err, "pthread_mutex_lock");
    }
    // makes client_control.stopped 0
    client_control.stopped = 0;
    // broadcast the signal
    if ((err = pthread_cond_broadcast(&client_control.go)) != 0) {
        handle_error_en(err, "pthread_cond_broadcast");
    }
    // unlock client control mutex
    if ((err = pthread_mutex_unlock(&client_control.go_mutex)) != 0) {
        handle_error_en(err, "pthread_mutex_unlock");
    }
}

// Called by listener (in comm.c) to create a new client thread
void client_constructor(FILE *cxstr) {
    // malloc new memory for a client
    client_t *client = (client_t *)malloc(sizeof(client_t));
    // error check malloc
    if ((client == NULL) && (sizeof(client_t) != 0)) {
        fprintf(stderr, "malloc failed\n");
        exit(1);
    }
    // initalize all the fields of the client struct
    client->cxstr = cxstr;
    client->prev = NULL;
    client->next = NULL;
    // error variable
    int error;
    // call pthread_create to create a new thread that can run a client
    if ((error = pthread_create(&client->thread, NULL, run_client, client)) !=
        0) {
        handle_error_en(error, "pthread_create");
    }
    // detach the newly created client thread
    if ((error = pthread_detach(client->thread)) != 0) {
        handle_error_en(error, "pthread_detach");
    }
}

void client_destructor(client_t *client) {
    // call comm_shutdown on the client to close the file
    comm_shutdown(client->cxstr);
    // frees the clients memory
    free(client);
    // sets the client to null
    client = NULL;
}

// Code executed by a client thread
void *run_client(void *arg) {
    // cast the input
    client_t *client = (client_t *)arg;
    // error variable
    int err;
    // buffers for the command and response
    char response[BUFLEN];
    char command[BUFLEN];
    // memset buffers to 0
    memset(response, 0, BUFLEN * sizeof(char));
    memset(command, 0, BUFLEN * sizeof(char));

    // locks the thread_list_mutex before checking if the server is active
    if ((err = pthread_mutex_lock(&thread_list_mutex)) != 0) {
        handle_error_en(err, "pthread_mutex_lock");
    }
    // checks if the server is active
    if (server_active == 0) {
        if ((err = pthread_mutex_unlock(&thread_list_mutex)) != 0) {
            handle_error_en(err, "pthread_mutex_unlock");
        }
        // if the server is not active call client_destructor on the client
        client_destructor(client);

    } else {
        // adds client to the client list
        if (thread_list_head == NULL) {
            thread_list_head = client;
        } else {
            client->next = thread_list_head;
            thread_list_head->prev = client;
            thread_list_head = client;
        }
        // push thread cleanup if thread has been canceled
        pthread_cleanup_push(thread_cleanup, client);
        // locks the server mutex
        if ((err = pthread_mutex_lock(&server.server_mutex)) != 0) {
            handle_error_en(err, "pthread_mutex_lock");
        }
        // increment the number of client thread using the server
        server.num_client_threads++;
        // unlock the server mutex
        if ((err = pthread_mutex_unlock(&server.server_mutex)) != 0) {
            handle_error_en(err, "pthread_mutex_unlock");
        }
        // unlock the thread_list_mutex
        if ((err = pthread_mutex_unlock(&thread_list_mutex)) != 0) {
            handle_error_en(err, "pthread_mutex_unlock");
        }

        // loop through comm_serve()
        while (comm_serve(client->cxstr, response, command) == 0) {
            // memset the response buffer
            memset(response, 0, BUFLEN);
            // wait on stopped database
            client_control_wait();
            // call interpret command
            interpret_command(command, response, BUFLEN);
            // memset the command buffer
            memset(command, 0, BUFLEN);
        }
        // call pthread_cleanup_pop to get ride of terminated threads
        pthread_cleanup_pop(1);
    }
    return NULL;
}

void delete_all() {
    // variable for error
    int err;
    // lock the thread mutex
    if ((err = pthread_mutex_lock(&thread_list_mutex)) != 0) {
        handle_error_en(err, "pthread_mutex_lock");
    }
    // change the server_active int boolean to 0 because we are getting rid of
    // all clients
    server_active = 0;
    // unlock the thread_list_mutex
    if ((err = pthread_mutex_unlock(&thread_list_mutex)) != 0) {
        handle_error_en(err, "pthread_mutex_unlock");
    }
    // variable
    client_t *next;
    // lock the thread list mutex
    if ((err = pthread_mutex_lock(&thread_list_mutex)) != 0) {
        handle_error_en(err, "pthread_mutex_lock");
    }
    // set the variable cur to the thread_list_head
    client_t *cur = thread_list_head;
    // loop through the thread list
    while (cur != NULL) {
        next = cur->next;
        // cancel all the threads
        if ((err = pthread_cancel(cur->thread)) != 0) {
            handle_error_en(err, "pthread_cancel: hello");
        }
        cur = next;
    }
    // unlock the thread list mutex
    if ((err = pthread_mutex_unlock(&thread_list_mutex)) != 0) {
        handle_error_en(err, "pthread_mutex_unlock");
    }
}

// Cleanup routine for client threads, called on cancels and exit.
void thread_cleanup(void *arg) {
    // set up variables
    client_t *client = (client_t *)arg;
    int err;

    // lock the thread list mutex
    if ((err = pthread_mutex_lock(&thread_list_mutex)) != 0) {
        handle_error_en(err, "pthread_mutex_lock");
    }
    // if there is nothing in the threadlist make the thread_list_head null
    if ((client->prev == NULL) && (client->next == NULL)) {
        thread_list_head = NULL;
    } else {
        // gets the prev and next of the client
        client_t *prev = client->prev;
        client_t *next = client->next;
        // if the client is somewhere in the middle of the list
        if ((prev != NULL) && (next != NULL)) {
            // set the pointers accordingly
            prev->next = next;
            next->prev = prev;
        }
        // if the client is the start of the list
        else if ((prev == NULL) && (next != NULL)) {
            // set pointers accordingly
            thread_list_head = next;
            next->prev = NULL;
        }
        // if the client is the end of the list
        else if ((prev != NULL) && (next == NULL)) {
            // set the pointers accordingly
            prev->next = NULL;
        }
    }
    // unlock the server mutex
    if ((err = pthread_mutex_unlock(&thread_list_mutex)) != 0) {
        handle_error_en(err, "pthread_mutex_unlock");
    }
    // lock the server mutex
    if ((err = pthread_mutex_lock(&server.server_mutex)) != 0) {
        handle_error_en(err, "pthread_mutex_lock");
    }
    // decrease the number of threads
    server.num_client_threads--;
    // if there are no more clients in the list
    if (server.num_client_threads == 0) {
        // broadcast the server condition variable
        if ((err = pthread_cond_broadcast(&server.server_cond)) != 0) {
            handle_error_en(err, "pthread_cond_broadcast");
        }
    }
    // lock the server mutex
    if ((err = pthread_mutex_unlock(&server.server_mutex)) != 0) {
        handle_error_en(err, "pthread_mutex_unlock");
    }
    // destroy the client
    client_destructor(client);
}

// Code executed by the signal handler thread. For the purpose of this
// assignment, there are two reasonable ways to implement this.
// The one you choose will depend on logic in sig_handler_constructor.
// 'man 7 signal' and 'man sigwait' are both helpful for making this
// decision. One way or another, all of the server's client threads
// should terminate on SIGINT. The server (this includes the listener
// thread) should not, however, terminate on SIGINT!
void *monitor_signal(void *arg) {
    // set variables
    sigset_t *sig_set = (sigset_t *)arg;
    int sig_num;
    int err;
    // wait for signal
    while (1) {
        // using sigwait to wait for the SIGINT signal
        if ((err = sigwait(sig_set, &sig_num)) > 0) {
            perror("sigwait");
            exit(1);
        }
        // if the signal is SIGINT
        if (sig_num == SIGINT) {
            // delete all clients
            if((err = printf("SIGINT recieved, cancelling all clients\n")) < 0){
                fprintf(stderr, "printf failed");
                exit(1);
            }
            delete_all();
            // lock the thread list mutex
            if ((err = pthread_mutex_lock(&thread_list_mutex)) != 0) {
                handle_error_en(err, "pthread_mutex_lock");
            }
            // change the server back to active so the signal can be used again
            server_active = 1;
            // unlock the thread_list_mutex
            if ((err = pthread_mutex_unlock(&thread_list_mutex)) != 0) {
                handle_error_en(err, "pthread_mutex_unlock");
            }
        }
    }

    return 0;
}

sig_handler_t *sig_handler_constructor() {
    // malloc memory for the signal handler
    sig_handler_t *sig_handle = (sig_handler_t *)malloc(sizeof(sig_handler_t));
    // error check malloc
    if ((sig_handle == NULL) && (sizeof(sig_handler_t) != 0)) {
        fprintf(stderr, "malloc failed\n");
        exit(1);
    }
    // error check
    int err;
    // empty the set
    if ((err = sigemptyset(&sig_handle->set)) == -1) {
        perror("sigemptyset");
        exit(1);
    }
    // add SIGINT to the set
    if ((err = sigaddset(&sig_handle->set, SIGINT)) == -1) {
        perror("sigaddset");
        exit(1);
    }
    // block SIGINT using pthread_sigmask
    if ((err = pthread_sigmask(SIG_BLOCK, &sig_handle->set, 0)) != 0) {
        handle_error_en(err, "pthread_sigmask");
    }
    // create a thread to handle SIGINT
    if ((err = pthread_create(&sig_handle->thread, NULL, monitor_signal,
                              (void *)&sig_handle->set)) != 0) {
        handle_error_en(err, "pthread_create");
    }

    return sig_handle;
}

void sig_handler_destructor(sig_handler_t *sighandler) {
    int err;
    // cancel thread
    if ((err = pthread_cancel(sighandler->thread)) != 0) {
        handle_error_en(err, "pthread_cancel bye");
        exit(1);
    }
    // join the thread
    if ((err = pthread_join(sighandler->thread, 0)) != 0) {
        handle_error_en(err, "pthread_join ");
        exit(1);
    }
    // free memory
    free(sighandler);
    // set sighandler to null
    sighandler = NULL;
}

// The arguments to the server should be the port number.
int main(int argc, char *argv[]) {
    // variables
    int err;
    char buf[BUFLEN];
    char *tokens[BUFLEN];
    char *token;
    int i = 0;
    ssize_t response;

    if ((err = pthread_mutex_lock(&thread_list_mutex)) != 0) {
        handle_error_en(err, "pthread_mutex_lock");
    }
    // set the server active
    server_active = 1;
    if ((err = pthread_mutex_unlock(&thread_list_mutex)) != 0) {
        handle_error_en(err, "pthread_mutex_unlock");
    }
    // ignore SIGPIPE
    signal(SIGPIPE, SIG_IGN);
    // sighandler
    sig_handler_t *sig_handle = sig_handler_constructor();
    // call start_listener
    pthread_t listen = start_listener(atoi(argv[1]), client_constructor);

    char *s;
    // loop command line until EOF
    while (1) {
        // memset buffers
        memset(buf, 0, BUFLEN * sizeof(char));
        memset(tokens, 0, BUFLEN * sizeof(char *));
        i = 0;
        // read the input into buf
        response = read(0, buf, BUFLEN);
        s = buf;

        // if response is less than 0 throw an error
        if (response < 0) {
            perror("read");
            exit(1);
        }
        // if response is greater than 0 execute the functions inside
        if (response > 0) {
            // null terminate the buffer
            buf[response] = '\0';
            // make tokens and add them to the tokens array
            while ((token = strtok(s, " \t\n")) != NULL) {
                tokens[i] = token;
                s = NULL;
                i++;
            }
            if (i > 0) {
                // if the command is a s
                if (strcmp(tokens[0], "s") == 0) {
                    printf("stopping all clients\n");
                    client_control_stop();

                }
                // if the command is a g
                else if (strcmp(tokens[0], "g") == 0) {
                    printf("releasing all clients\n");
                    client_control_release();

                }
                // if the command is a p
                else if (strcmp(tokens[0], "p") == 0) {
                    // print the tree given teh file name
                    if ((err = db_print(tokens[1])) == -1) {
                        fprintf(stderr, "db_print error");
                    }
                }
            }
        }
        // if response is 0 that means EOF has happened and return
        if (response == 0) {
            // delete all clients
            delete_all();
            // lock the server mutex
            if ((err = pthread_mutex_lock(&server.server_mutex)) != 0) {
                handle_error_en(err, "pthread_mutex_lock");
            }
            // push a clean up handler to clean up the mutex
            pthread_cleanup_push(clean_up_pthread_mutex, &server.server_mutex);
            // wait for the number of threads to be 0
            while (server.num_client_threads != 0) {
                pthread_cond_wait(&server.server_cond, &server.server_mutex);
            }
            // cleanup pop
            pthread_cleanup_pop(1);
            // destroy the sighandler
            sig_handler_destructor(sig_handle);
            // call db_cleanup
            db_cleanup();
            // cancel the lisenter thread
            if ((err = pthread_cancel(listen)) != 0) {
                handle_error_en(err, "pthread_cancel hi");
                exit(1);
            }
            // join the listener thread
            if ((err = pthread_join(listen, NULL)) != 0) {
                handle_error_en(err, "pthread_join");
                exit(1);
            }
            if((err = printf("exiting database\n")) < 0){
                fprintf(stderr, "printf failed");
                exit(1);
            }
            return 0;
        }
    }
    return 0;
}
