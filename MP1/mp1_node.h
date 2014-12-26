/**********************
*
* Progam Name: MP1. Membership Protocol.
* 
* Code authors: <Prakyath J>
*
* Current file: mp2_node.h
* About this file: Header file.
* 
***********************/

#ifndef _NODE_H_
#define _NODE_H_

#include "stdincludes.h"
#include "params.h"
#include "queue.h"
#include "requests.h"
#include "emulnet.h"

/* Configuration Parameters */
char JOINADDR[30];                    /* address for introduction into the group. */
extern char *DEF_SERVADDR;            /* server address. */
extern short PORTNUM;                /* standard portnum of server to contact. */

/* Miscellaneous Parameters */
extern char *STDSTRING;
extern int MAX_NNB;

#define MAX_MEMBERS 50                  // maximum number of members in a membership list
#define T_FAIL 25                /* Time out to declare a node as failed*/
#define T_CLEANUP 25            /* Time out to delete the node from the entry*/

typedef struct memberlist{
        struct address addr;            // Address of the member
        int heartbeatcounter;           // heartbeat counter to be incremented
        int timestamp;                  // timestamp of the member
        int isfailed;                   // flag to check if the member has failed
}memberlist;

typedef struct member{            
        struct address addr;            // my address
        int inited;                     // boolean indicating if this member is up
        int ingroup;                    // boolean indiciating if this member is in the group

        queue inmsgq;                   // queue for incoming messages

        int bfailed;                    // boolean indicating if this member has failed

        struct memberlist membershiplist[MAX_MEMBERS];  // membership list maintained by the member

        int countofmembers;             // count of members present in the membership list
} member;



/* Message types */
/* Meaning of different message types
  JOINREQ - request to join the group
  JOINREP - replyto JOINREQ
*/
enum Msgtypes{
		JOINREQ,			
		JOINREP,
        PROCESSGOSSIP,
		DUMMYLASTMSGTYPE
};

/* Generic message template. */
typedef struct messagehdr{ 	
	enum Msgtypes msgtype;
} messagehdr;


/* Functions in mp2_node.c */

/* Message processing routines. */
STDCLLBKRET Process_joinreq STDCLLBKARGS;
STDCLLBKRET Process_joinrep STDCLLBKARGS;
STDCLLBKRET Process_gossip STDCLLBKARGS;

/*
int recv_callback(void *env, char *data, int size);
int init_thisnode(member *thisnode, address *joinaddr);
*/

/*
Other routines.
*/

void nodestart(member *node, char *servaddrstr, short servport);
void nodeloop(member *node);
int recvloop(member *node);
int finishup_thisnode(member *node);

#endif /* _NODE_H_ */

