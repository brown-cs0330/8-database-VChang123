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

void clean_up_pthread_mutex(void *arg) {
    // error check
    pthread_mutex_unlock((pthread_mutex_t *)arg);
}
// Called by client threads to wait until progress is permitted
void client_control_wait() {
    // TODO: Block the calling thread until the main thread calls
    // client_control_release(). See the client_control_t struct.
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
    // TODO: Ensure that the next time client threads call client_control_wait()
    // at the top of the event loop in run_client, they will block.

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
    // TODO: Allow clients that are blocked within client_control_wait()
    // to continue. See the client_control_t struct.
    // lock client control mutex
    int err;
    if ((err = pthread_mutex_lock(&client_control.go_mutex)) != 0) {
        handle_error_en(err, "pthread_mutex_lock");
    }
    client_control.stopped = 0;
    // broadcast signal
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
    // You should create a new client_t struct here and initialize ALL
    // of its fields. Remember that these initializations should be
    // error-checked.
    //
    // TODO:
    // Step 1: Allocate memory for a new client and set its connection stream
    // to the input argument.
    // Step 2: Create the new client thread running the run_client routine.
    // Step 3: Detach the new client thread

    client_t *client = (client_t *)malloc(sizeof(client_t));
    if ((client == NULL) && (sizeof(client_t) != 0)) {
        fprintf(stderr, "malloc failed\n");
        exit(1);
    }
    client->cxstr = cxstr;
    client->prev = NULL;
    client->next = NULL;
    int error;
    if ((error = pthread_create(&client->thread, NULL, run_client, client)) !=
        0) {
        handle_error_en(error, "pthread_create");
    }
    // handle error
    if ((error = pthread_detach(client->thread)) != 0) {
        handle_error_en(error, "pthread_detach");
    }
}

void client_destructor(client_t *client) {
    // TODO: Free and close all resources associated with a client.
    // Whatever was malloc'd in client_constructor should
    // be freed here!
    comm_shutdown(client->cxstr);
    free(client);
    client = NULL;
}

// Code executed by a client thread
void *run_client(void *arg) {
    // TODO:
    // Step 1: Make sure that the server is still accepting clients.
    // Step 2: Add client to the client list and push thread_cleanup to remove
    //       it if the thread is canceled.
    // Step 3: Loop comm_serve (in comm.c) to receive commands and output
    //       responses. Execute commands using interpret_command (in db.c)
    // Step 4: When the client is done sending commands, exit the thread
    //       cleanly.
    //
    // Keep stop and go in mind when writing this function!

    // server does not accept clients when recieved stopped signal
    client_t *client = (client_t *)arg;
    // client_t *cur = thread_list_head;
    int err;
    char response[BUFLEN];
    char command[BUFLEN];
    // memset buffers to 0
    memset(response, 0, BUFLEN * sizeof(char));
    memset(command, 0, BUFLEN * sizeof(char));

    // checks if the server is accpeting new clients
    if (server_active == 0) {
        client_destructor(client);
    } else {
        // adds client to the client list
        if ((err = pthread_mutex_lock(&thread_list_mutex)) != 0) {
            handle_error_en(err, "pthread_mutex_lock");
        }
        if (thread_list_head == NULL) {
            thread_list_head = client;
            thread_list_head->prev = NULL;
            thread_list_head->next = NULL;

        } else {
            client->next = thread_list_head;
            client->prev = NULL;
            thread_list_head->prev = client;
            thread_list_head = client;
        }
        // push thread cleanup if thread has been canceled
        pthread_cleanup_push(thread_cleanup, client);

        if ((err = pthread_mutex_lock(&server.server_mutex)) != 0) {
            handle_error_en(err, "pthread_mutex_lock");
        }
        server.num_client_threads++;

        if ((err = pthread_mutex_unlock(&server.server_mutex)) != 0) {
            handle_error_en(err, "pthread_mutex_unlock");
        }
        if ((err = pthread_mutex_unlock(&thread_list_mutex)) != 0) {
            handle_error_en(err, "pthread_mutex_unlock");
        }

        // loop through comm_serve()
        while (comm_serve(client->cxstr, response, command) == 0) {
            // wait on stopped database
            memset(response, 0, BUFLEN);
            client_control_wait();
            interpret_command(command, response, BUFLEN);
            memset(command, 0, BUFLEN);
        }

        pthread_cleanup_pop(1);
    }
    return NULL;
}

void delete_all() {
    // TODO: Cancel every thread in the client thread list with the
    // pthread_cancel function.
    client_t *cur = thread_list_head;
    client_t *next;
    server_active = 0;
    int err;
    pthread_mutex_lock(&thread_list_mutex);
    while (cur != NULL) {
        next = cur->next;

        if ((err = pthread_cancel(cur->thread)) != 0) {
            handle_error_en(err, "pthread_cancel: hello");
        }
        cur = next;
    }
    pthread_mutex_unlock(&thread_list_mutex);
    if (server.num_client_threads == 0) {
        pthread_cond_broadcast(&server.server_cond);
    }
}

// Cleanup routine for client threads, called on cancels and exit.
void thread_cleanup(void *arg) {
    // TODO: Remove the client object from thread list and call
    // client_destructor. This function must be thread safe! The client must
    // be in the list before this routine is ever run.
    client_t *client = (client_t *)arg;
    int err;

    if ((client->prev == NULL) && (client->next == NULL)) {
        thread_list_head = NULL;
    }

    if ((err = pthread_mutex_lock(&thread_list_mutex)) != 0) {
        handle_error_en(err, "pthread_mutex_lock");
    }
    client_t *prev = client->prev;
    client_t *next = client->next;
    if ((prev != NULL) && (next != NULL)) {
        prev->next = next;
        next->prev = prev;
    } else if ((prev == NULL) && (next != NULL)) {
        thread_list_head = next;
        next->prev = NULL;
    } else if ((prev != NULL) && (next == NULL)) {
        prev->next = NULL;
    }
    if ((err = pthread_mutex_unlock(&thread_list_mutex)) != 0) {
        handle_error_en(err, "pthread_mutex_unlock");
    }

    if ((err = pthread_mutex_lock(&server.server_mutex)) != 0) {
        handle_error_en(err, "pthread_mutex_lock");
    }
    server.num_client_threads--;

    if ((err = pthread_mutex_unlock(&server.server_mutex)) != 0) {
        handle_error_en(err, "pthread_mutex_unlock");
    }
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
    // TODO: Wait for a SIGINT to be sent to the server process and cancel
    // all client threads when one arrives.
    sigset_t *sig_set = (sigset_t *)arg;
    int sig_num;
    int err;
    while (1) {
        if ((err = sigwait(sig_set, &sig_num)) > 0) {
            fprintf(stderr, "sigwait");
            exit(1);
        }
        if (sig_num == SIGINT) {
            printf("SIGINT recieved, cancelling all clients\n");
            delete_all();

            server_active = 1;
        }
    }

    return 0;
}

sig_handler_t *sig_handler_constructor() {
    // TODO: Create a thread to handle SIGINT. The thread that this function
    // creates should be the ONLY thread that ever responds to SIGINT.
    sig_handler_t *sig_handle = (sig_handler_t *)malloc(sizeof(sig_handler_t));

    if ((sig_handle == NULL) && (sizeof(sig_handler_t) != 0)) {
        fprintf(stderr, "malloc failed\n");
        exit(1);
    }
    // error check
    sigemptyset(&sig_handle->set);
    sigaddset(&sig_handle->set, SIGINT);
    int err;
    if ((err = pthread_create(&sig_handle->thread, NULL, monitor_signal,
                              (void *)&sig_handle->set)) != 0) {
        handle_error_en(err, "pthread_create");
    }

    return sig_handle;
}

void sig_handler_destructor(sig_handler_t *sighandler) {
    // TODO: Free any resources allocated in sig_handler_constructor.
    // Cancel and join with the signal handler's thread.
    int err;
    if ((err = pthread_cancel(sighandler->thread)) != 0) {
        handle_error_en(err, "pthread_cancel bye");
        exit(1);
    }
    if ((err = pthread_join(sighandler->thread, 0)) != 0) {
        handle_error_en(err, "pthread_join ");
        exit(1);
    }
    free(sighandler);
    sighandler = NULL;
}

// The arguments to the server should be the port number.
int main(int argc, char *argv[]) {
    // TODO:
    // Step 1: Set up the signal handler.
    // Step 2: block SIGPIPE so that the server does not abort when a client
    // disocnnects
    // Step 3: Start a listener thread for clients (see
    // start_listener in
    //       comm.c).
    // Step 4: Loop for command line input and handle accordingly until EOF.
    // Step 5: Destroy the signal handler, delete all clients, cleanup the
    //       database, cancel and join with the listener thread
    //
    // You should ensure that the thread list is empty before cleaning up the
    // database and canceling the listener thread. Think carefully about what
    // happens in a call to delete_all() and ensure that there is no way for a
    // thread to add itself to the thread list after the server's final
    // delete_all().

    int err;
    server_active = 1;
    char buf[BUFLEN];
    char *tokens[BUFLEN];
    char *token;
    int i = 0;

    // char *file;
    ssize_t response;

    // sig mask list
    sigset_t list;
    // error check
    sigemptyset(&list);

    sigaddset(&list, SIGINT);

    // block SIGPIPE and SIGINT
    signal(SIGPIPE, SIG_BLOCK);
    if ((err = pthread_sigmask(SIG_BLOCK, &list, 0)) != 0) {
        handle_error_en(err, "pthread_sigmask");
    }
    // sighandler
    sig_handler_t *sig_handle = sig_handler_constructor();

    // call start_listener
    pthread_t listen = start_listener(atoi(argv[1]), client_constructor);

    // loop command line until EOF
    while (1) {
        memset(buf, 0, BUFLEN * sizeof(char));
        memset(tokens, 0, BUFLEN * sizeof(char *));
        i = 0;
        char *s = buf;
        // read the input into buf
        response = read(0, buf, BUFLEN);

        // if response is less than 0 throw an error
        if (response < 0) {
            perror("read");
            exit(1);
        }
        // if response is greater than 0 execute the functions inside
        if (response > 0) {
            // null terminate the buffer
            buf[response] = '\0';

            while ((token = strtok(s, " \t\n")) != NULL) {
                tokens[i] = token;
                s = NULL;
                i++;
            }
            if (strcmp(tokens[0], "s") == 0) {
                printf("stopping all clients\n");
                client_control_stop();

            } else if (strcmp(tokens[0], "g") == 0) {
                printf("releasing all clients\n");
                client_control_release();

            } else if (strcmp(tokens[0], "p") == 0) {
                if ((err = db_print(tokens[1])) == -1) {
                    fprintf(stderr, "db_print error");
                    exit(1);
                }
            }
        }
        // if response is 0 that means EOF has happened and return
        if (response == 0) {
            printf("exiting database\n");
            server_active = 0;
            sig_handler_destructor(sig_handle);
            delete_all();
            if ((err = pthread_mutex_lock(&server.server_mutex)) != 0) {
                handle_error_en(err, "pthread_mutex_lock");
            }
            pthread_cleanup_push(clean_up_pthread_mutex, &server.server_mutex);
            while (server.num_client_threads != 0) {
                pthread_cond_wait(&server.server_cond, &server.server_mutex);
            }
            pthread_cleanup_pop(1);

            db_cleanup();
            if ((err = pthread_cancel(listen)) != 0) {
                handle_error_en(err, "pthread_cancel hi");
                exit(1);
            }
            if ((err = pthread_join(listen, NULL)) != 0) {
                handle_error_en(err, "pthread_join");
                exit(1);
            }
            return 0;
        }
    }
    return 0;
}
