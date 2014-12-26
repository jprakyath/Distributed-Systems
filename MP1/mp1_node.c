/**********************
*
* Progam Name: MP1. Membership Protocol
* 
* Code authors: <Prakyath J>
*
* Current file: mp1_node.c
* About this file: Member Node Implementation
* 
***********************/

#include "mp1_node.h"
#include "emulnet.h"
#include "MPtemplate.h"
#include "log.h"


/*
 *
 * Routines for introducer and current time.
 *
 */

char NULLADDR[] = {0,0,0,0,0,0};
int isnulladdr( address *addr){
    return (memcmp(addr, NULLADDR, 6)==0?1:0);
}

/* 
Return the address of the introducer member. 
*/
address getjoinaddr(void){

    address joinaddr;

    memset(&joinaddr, 0, sizeof(address));
    *(int *)(&joinaddr.addr)=1;
    *(short *)(&joinaddr.addr[4])=0;

    return joinaddr;
}

/*
Return the address of the introducer member.
*/
address getgossipaddr(int value){

    address gossipaddr;

    memset(&gossipaddr, 0, sizeof(address));
    *(int *)(&gossipaddr.addr)=value;
    *(short *)(&gossipaddr.addr[4])=0;

    return gossipaddr;
}

/*
 *
 * Message Processing routines.
 *
 */

/* 
Received a JOINREQ (joinrequest) message.
*/
void Process_joinreq(void *env, char *data, int size)
{
    /* <your code goes in here> */

    // env = the node who has received the msg. Introducer
    member *node = (member *) env;

    /* address of the member who sent a join request to the introducer.*/
    address *addr = (address *)data;

    size_t msgsize = sizeof(messagehdr) + sizeof(memberlist)*MAX_NNB;
    messagehdr *msg=malloc(msgsize);

    printf("\nIn Process_joinreq function, Sending reply from the introducer to ");
    printf("%d.%d.%d.%d:%d \n", addr->addr[0], addr->addr[1],
            addr->addr[2], addr->addr[3], *(short *)&addr->addr[4]);

    msg->msgtype=JOINREP;

    // add the new nodes row in the membership list and call the logNodeAdd function
    //memcpy(&node->membershiplist[node->countofmembers].addr,&node->addr,sizeof(char)*6);
    memcpy(&node->membershiplist[node->countofmembers].addr,addr,sizeof(char)*6);
    node->membershiplist[node->countofmembers].heartbeatcounter += 1;
    node->membershiplist[node->countofmembers].timestamp = getcurrtime();
    node->membershiplist[node->countofmembers].isfailed = 0;
#ifdef DEBUGLOG
    logNodeAdd(&node->addr,&node->membershiplist[node->countofmembers].addr);
#endif
    node->countofmembers++;

    printf("\nThe number of members in the list of introducer is %d\n",node->countofmembers);

    //msg should be the membership list.
    memcpy((char *)(msg+1), &node->membershiplist, msgsize-sizeof(messagehdr));
    /* send JOINREQ message to introducer member. */
    MPp2psend(&node->addr, addr, (char *)(msg), msgsize);

    free(msg);
    return;
}

/* 
Received a JOINREP (joinreply) message. 
*/
void Process_joinrep(void *env, char *data, int size)
{
	/* <your code goes in here> */
    //static char s[1024];
    member *node = (member *) env;
    int i ;

    /* address of the member who sent a reply.*/
    //address *addr = (address *)data;

    memberlist *list = (memberlist *)(data);
    // data will be the membership list of introducer and size is the size of data type of my stucture
    // env = myself
    // add my membership list entry (call Log node add)

    node->ingroup = 1;

    for (i = 0; i < MAX_NNB; i++)
    {
        // end of the list
        if (memcmp(&list[i].addr, NULLADDR, sizeof(address)) == 0)
        {
            return;
        }
        // The entry is already present in my membership list
        else if(memcmp(&list[i].addr, &node->membershiplist[i].addr,sizeof(address)) == 0)
        {
            if(node->membershiplist[i].heartbeatcounter < list[i].heartbeatcounter)
            {
                node->membershiplist[i].timestamp = getcurrtime();
                node->membershiplist[i].heartbeatcounter = list[i].heartbeatcounter;
            }
        } 
        else
        {
            /** Adding the introducer's membership list to the membership list of the other nodes*/
            memcpy(&node->membershiplist[i].addr,&list[i].addr,sizeof(address));
            node->membershiplist[i].heartbeatcounter += 1;
            node->membershiplist[i].timestamp = getcurrtime();
            node->membershiplist[i].isfailed = 0;
#ifdef DEBUGLOG
            logNodeAdd(&node->addr,&node->membershiplist[i].addr);
#endif
            node->countofmembers++;
        }
    }

    return;
}


/* 
Array of Message handlers. 
*/
void ( ( * MsgHandler [20] ) STDCLLBKARGS )={
/* Message processing operations at the P2P layer. */
    Process_joinreq, 
    Process_joinrep,
    Process_gossip
};

/* 
Called from nodeloop() on each received packet dequeue()-ed from node->inmsgq. 
Parse the packet, extract information and process. 
env is member *node, data is 'messagehdr'. 
*/
int recv_callback(void *env, char *data, int size){

    member *node = (member *) env;
    messagehdr *msghdr = (messagehdr *)data;

    // pktdata is nothing but the data sent in this case the address.
    char *pktdata = (char *)(msghdr+1);

    if(size < sizeof(messagehdr)){
#ifdef DEBUGLOG
        LOG(&((member *)env)->addr, "Faulty packet received - ignoring");
#endif
        return -1;
    }

#ifdef DEBUGLOG
    LOG(&((member *)env)->addr, "Received msg type %d with %d B payload", msghdr->msgtype, size - sizeof(messagehdr));
#endif

    if((node->ingroup && msghdr->msgtype >= 0 && msghdr->msgtype <= DUMMYLASTMSGTYPE)
        || (!node->ingroup && msghdr->msgtype==JOINREP))            
            /* if not yet in group, accept only JOINREPs */
        MsgHandler[msghdr->msgtype](env, pktdata, size-sizeof(messagehdr));
    /* else ignore (garbled message) */
    free(data);

    return 0;
}

/*
 *
 * Initialization and cleanup routines.
 *
 */

/* 
Find out who I am, and start up. 
*/
int init_thisnode(member *thisnode, address *joinaddr){
    
    int i;
    if(MPinit(&thisnode->addr, PORTNUM, (char *)joinaddr)== NULL){ /* Calls ENInit */
#ifdef DEBUGLOG
        LOG(&thisnode->addr, "MPInit failed");
#endif
        exit(1);
    }
#ifdef DEBUGLOG
    else LOG(&thisnode->addr, "MPInit succeeded. Hello.");
#endif

    thisnode->bfailed=0;
    thisnode->inited=1;
    thisnode->ingroup=0;

    thisnode->countofmembers = 0;
    /* node is up! */

    //Initialize membership list
    // address, heartbeat counter, local timestamp, flag to check if the member is failed
    //thisnode->membershiplist = (memberlist *)malloc(sizeof(memberlist)*10);
    for(i = 0; i< EN_GPSZ ; i++)
    {
        memcpy(&thisnode->membershiplist[i].addr,NULLADDR,sizeof(address));
        thisnode->membershiplist[i].heartbeatcounter = 0;
        thisnode->membershiplist[i].timestamp = 0;
        thisnode->membershiplist[i].isfailed = 0;
    }

    return 0;
}


/* 
Clean up this node. 
*/
int finishup_thisnode(member *node){

	/* <your code goes in here> */
    //if(node != NULL)
        //free(node);
    int i;
    for(i = 0; i< MAX_MEMBERS ; i++)
    {
        memset(&node->membershiplist[i],0,sizeof(memberlist));
    }
    return 0;
}


/* 
 *
 * Main code for a node 
 *
 */

/* 
Introduce self to group. 
*/
int introduceselftogroup(member *node, address *joinaddr){
    
    messagehdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if(memcmp(&node->addr, joinaddr, 4*sizeof(char)) == 0){
        /* I am the group booter (first process to join the group). Boot up the group. */
#ifdef DEBUGLOG
        LOG(&node->addr, "Starting up group...");
#endif
        // initialize membership list of the introducer
        // add an entry of itself to its membership list
        /** Introducer adding itself to its membership list*/
        memcpy(&node->membershiplist[node->countofmembers].addr,&node->addr,sizeof(char)*6);
        node->membershiplist[node->countofmembers].heartbeatcounter = 1;
        node->membershiplist[node->countofmembers].timestamp = getcurrtime();
        node->membershiplist[node->countofmembers].isfailed = 0;
        node->ingroup = 1;
        node->countofmembers++;
        logNodeAdd(&node->addr,&node->addr);
    }
    else{

        //msgsize  = msg type + data size. In this case the data size is of address
        // implement size for gossip msg.

        size_t msgsize = sizeof(messagehdr) + sizeof(address);
        msg=malloc(msgsize);

    /* create JOINREQ message: format of data is {struct address myaddr} */
        msg->msgtype=JOINREQ;
        memcpy((char *)(msg+1), &node->addr, sizeof(address));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        LOG(&node->addr, s);
#endif

    /* send JOINREQ message to introducer member. */
        MPp2psend(&node->addr, joinaddr, (char *)msg, msgsize);
        
        free(msg);
    }

    return 1;

}

/* 
Called from nodeloop(). 
*/
void checkmsgs(member *node){
    void *data;
    int size;

    /* Dequeue waiting messages from node->inmsgq and process them. */
	
    while((data = dequeue(&node->inmsgq, &size)) != NULL) {
        recv_callback((void *)node, data, size); 
    }
    return;
}


/* 
Executed periodically for each member. 
Performs necessary periodic operations. 
Called by nodeloop(). 
*/
void nodeloopops(member *node){
    // code for
    /* 1. increment my hearbeat counter
       2. check that no node has failed Tfail
       3. do cleanup Tcleanup and call logNodeRemove
       4. Send your updated membership list to any member. Gossip use rand function
    */
    // function will be called till the global time ends

    // The entry is already present in my membership list
    char s[1024];
    int i;
    int found;
    int gossip_number;
    messagehdr *msg;
    address gossip_addr;

    for (i = 0; i < MAX_NNB; i++) {
        if (memcmp(&node->membershiplist[i].addr, NULLADDR, sizeof(address)) == 0) {
            continue;
        }
        if (memcmp(&node->addr, &node->membershiplist[i].addr, sizeof(char) * 4) == 0) {
            node->membershiplist[i].heartbeatcounter++;
            node->membershiplist[i].timestamp = getcurrtime();
        } else {
        if (!node->membershiplist[i].isfailed) {
            if ((getcurrtime() - node->membershiplist[i].timestamp) > T_FAIL) {
                node->membershiplist[i].isfailed = 1;
                node->membershiplist[i].timestamp = getcurrtime();
            }
        } else {
            if ((getcurrtime() - node->membershiplist[i].timestamp) > T_CLEANUP) {
                logNodeRemove(&node->addr, &node->membershiplist[i].addr);
                memcpy((address*)&node->membershiplist[i].addr, NULLADDR, sizeof(char)*6);
                node->membershiplist[i].heartbeatcounter = 0;
                node->membershiplist[i].isfailed = 0;
                node->membershiplist[i].timestamp = 0;

            }
        }
        }
    }

    // gossip to other processes
    size_t msgsize = sizeof(messagehdr) + sizeof(memberlist)*MAX_NNB;
    msg=malloc(msgsize);

        /* create PROCESSGOSSIP message: format of data is {struct address myaddr} */
    msg->msgtype=PROCESSGOSSIP;
    memcpy((char *)(msg+1), &node->membershiplist, msgsize - sizeof(messagehdr));

    gossip_number = (rand() % EN_GPSZ)+1;
    gossip_addr = getgossipaddr(gossip_number);

    Gossip:
    found = 0;
    for(i = 0; i < node->countofmembers ; i++) {
        if(memcmp(&node->membershiplist[i].addr, &gossip_addr,sizeof(address)) == 0){
            if(memcmp(&node->addr, &gossip_addr,sizeof(address)) != 0)
            memcpy(&gossip_addr,&node->membershiplist[i].addr,sizeof(address));
            found = 1;
        } else {
            gossip_number = (rand() % EN_GPSZ)+1;
            gossip_addr = getgossipaddr(gossip_number);
        }
    }

    if(found != 1)
        goto Gossip;

#ifdef DEBUGLOG
        sprintf(s, "Gossiping...!");
        LOG(&node->addr, s);
#endif
        /* send gossip to random member. */
        MPp2psend(&node->addr, &gossip_addr, (char *)msg, msgsize);
#ifdef DEBUGLOG
        sprintf(s, "Received gossip");
        LOG(&gossip_addr, s);
#endif

    free(msg);
    return;
}

/* Process the gossip function */
void Process_gossip(void *env, char *data, int size) {

    /* Compare and merge my membership list and the membership list of the
    received node */
    member *node = (member *) env;
    int i,j ;
    memberlist *list = (memberlist *)(data);

        #if 1
    for (i = 0; i < MAX_NNB; i++)
    {
        if (memcmp(&list[i].addr, NULLADDR, sizeof(address)) == 0) {
            continue;
        }
        int flag = 0;
        for(j = 0; j < MAX_NNB ; j++)
        {
            if(memcmp(&list[i].addr,&node->membershiplist[j].addr,sizeof(address)) == 0)
            {
                if(list[i].heartbeatcounter > node->membershiplist[j].heartbeatcounter)
                {
                    node->membershiplist[j].heartbeatcounter = list[i].heartbeatcounter;
                    node->membershiplist[j].timestamp = getcurrtime();
                }
                flag = 1;
            }
        }

        if(flag != 1  && list[i].isfailed != 1)
        {
        /** Adding the introducer's membership list to the membership list of the other nodes*/
        memcpy(&node->membershiplist[node->countofmembers].addr,&list[i].addr,sizeof(address));
        node->membershiplist[node->countofmembers].heartbeatcounter += 1;
        node->membershiplist[node->countofmembers].timestamp = getcurrtime();
        node->membershiplist[node->countofmembers].isfailed = 0;
#ifdef DEBUGLOG
        logNodeAdd(&node->addr,&node->membershiplist[node->countofmembers].addr);
#endif
        node->countofmembers++;
        }
    }
#endif
    return;
}

/* 
Executed periodically at each member. Called from app.c.
*/
void nodeloop(member *node){
    if (node->bfailed) return;

    checkmsgs(node);

    /* Wait until you're in the group... */
    if(!node->ingroup) return ;

    /* ...then jump in and share your responsibilites! */
    nodeloopops(node);
    
    return;
}

/* 
All initialization routines for a member. Called by app.c. 
*/
void nodestart(member *node, char *servaddrstr, short servport){

    address joinaddr=getjoinaddr();

    /* Self booting routines */
    if(init_thisnode(node, &joinaddr) == -1){

#ifdef DEBUGLOG
        LOG(&node->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if(!introduceselftogroup(node, &joinaddr)){
        finishup_thisnode(node);
#ifdef DEBUGLOG
        LOG(&node->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/* 
Enqueue a message (buff) onto the queue env. 
*/
int enqueue_wrppr(void *env, char *buff, int size){    return enqueue((queue *)env, buff, size);}

/* 
Called by a member to receive messages currently waiting for it. 
*/
int recvloop(member *node){
    if (node->bfailed) return -1;
    else return MPrecv(&(node->addr), enqueue_wrppr, NULL, 1, &node->inmsgq); 
    /* Fourth parameter specifies number of times to 'loop'. */
}

