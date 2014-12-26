/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
    for (size_t i = 0; i < RING_SIZE; i++) {
        ring.emplace_back(Node(Address(EMPTY_ADDRESS)));
    }
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */

void MP2Node::updateRing() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	vector<Node> curMemList;
	bool change = false;
    size_t count_of_members = 0;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());
	
    vector<Node>::iterator it;
    for (it = curMemList.begin(); it != curMemList.end(); it++) {
        Node memberNode = *it;
        size_t nodeHashCode = memberNode.getHashCode();
        Node existingNode = ring[nodeHashCode];

        if (memberNode.getAddress()->getAddress() != existingNode.getAddress()->getAddress()) {
            ring[memberNode.getHashCode()] = memberNode;
            count_of_members ++;
            change = true;
        }
    }

    if (count_of_members != curMemList.size()) {
        // some node has failed
        change = true;
    }

    /*
     * Step 3: Run the stabilization protocol IF REQUIRED
     */
    // Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
    if (change == true && !ht->isEmpty()) {
        stabilizationProtocol();
    }
    trackMsgTypeTimer();
}


// track the trackMsgType and trackKeyValue to check for timeout
void MP2Node::trackMsgTypeTimer() {
    vector<int> tempVector;
    vector<string> tempStringVector;
    int trans_id;
    int timer;
    int msg_type;
    int quorum_counter;

    map<int, vector<int> >::iterator it;
    for(it = trackMsgType.begin(); it != trackMsgType.end(); it++){
        trans_id = it->first;
        tempVector = trackMsgType[trans_id];
        msg_type = tempVector[0];
        timer = tempVector[2];
        quorum_counter = tempVector[1];
        trackMsgType[trans_id] = tempVector;

        tempStringVector = trackKeyValue[trans_id];
        string tempKey = tempStringVector[0];
        string tempValue = tempStringVector[1];

        // Check the timer if the coordinator does not get any reply then
        // log a read or an update failure
        if( (par->getcurrtime() - timer) > 15 && quorum_counter < 2){
            if(msg_type == READ) {
                log->logReadFail(&memberNode->addr, true, trans_id ,tempKey);
            } else if (msg_type == UPDATE) {
                log->logUpdateFail(&memberNode->addr, true, it->first ,tempKey,tempValue);
            }
            trackKeyValue.erase(trans_id);
            trackMsgType.erase(trans_id);
        }
    }
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
	/*
	 * Implement this
	 */
    vector<Node> replicas = findNodes(key);

    int transaction_id = ++g_transID;
    Node primary_replica = replicas.at(0);
    Node secondary_replica = replicas.at(1);
    Node tertiary_replica = replicas.at(2);

    Message *primary = new Message(transaction_id, memberNode->addr, CREATE, key, value, PRIMARY);
    Message *secondary = new Message(transaction_id, memberNode->addr, CREATE, key, value, SECONDARY);
    Message *tertiary = new Message(transaction_id, memberNode->addr, CREATE, key, value, TERTIARY);
    string pri = primary->toString();
    string sec = secondary->toString();
    string ter = tertiary->toString();

    emulNet->ENsend(&memberNode->addr, primary_replica.getAddress(), pri);
    emulNet->ENsend(&memberNode->addr, secondary_replica.getAddress(), sec);
    emulNet->ENsend(&memberNode->addr, tertiary_replica.getAddress(), ter);

    vector<int> tempVector;
    /* Variable to track the message type */
    tempVector.push_back(CREATE);
    /* Variable to track the quorum counter */
    tempVector.push_back(0);
    /* Variable to track the time */
    tempVector.push_back(par->getcurrtime());
    /* Key = transaction id : value = msg_type,quorum_counter,time */
    trackMsgType[transaction_id] = tempVector;

    vector<string> tempKeyValueVector;
    tempKeyValueVector.push_back(key);
    tempKeyValueVector.push_back(value);
    trackKeyValue[transaction_id] = tempKeyValueVector;
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){
	/*
	 * Implement this
	 */
    vector<Node> replicas = findNodes(key);

    int transaction_id = g_transID++;
    Node primary_replica = replicas.at(0);
    Node secondary_replica = replicas.at(1);
    Node tertiary_replica = replicas.at(2);

    Message *primary = new Message(transaction_id, memberNode->addr, READ, key);
    Message *secondary = new Message(transaction_id, memberNode->addr, READ, key);
    Message *tertiary = new Message(transaction_id, memberNode->addr, READ, key);

    string pri = primary->toString();
    string sec = secondary->toString();
    string ter = tertiary->toString();

    emulNet->ENsend(&memberNode->addr, primary_replica.getAddress(), pri);
    emulNet->ENsend(&memberNode->addr, secondary_replica.getAddress(), sec);
    emulNet->ENsend(&memberNode->addr, tertiary_replica.getAddress(), ter);

    vector<int> tempVector;
    /* Variable to track the message type */
    tempVector.push_back(READ);
    /* Variable to track the quorum counter */
    tempVector.push_back(0);
    /* Variable to track the time */
    tempVector.push_back(par->getcurrtime());
    /* Key = transaction id : value = msg_type,quorum_counter,time */
    trackMsgType[transaction_id] = tempVector;

    vector<string> tempKeyValueVector;
    tempKeyValueVector.push_back(key);
    trackKeyValue[transaction_id] = tempKeyValueVector;
}
/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){
	/*
	 * Implement this
	 */
    vector<Node> replicas = findNodes(key);

    int transaction_id = g_transID++;
    Node primary_replica = replicas.at(0);
    Node secondary_replica = replicas.at(1);
    Node tertiary_replica = replicas.at(2);

    Message *primary = new Message(transaction_id, memberNode->addr, UPDATE, key, value, PRIMARY);
    Message *secondary = new Message(transaction_id, memberNode->addr, UPDATE, key, value, SECONDARY);
    Message *tertiary = new Message(transaction_id, memberNode->addr, UPDATE, key, value, TERTIARY);

    string pri = primary->toString();
    string sec = secondary->toString();
    string ter = tertiary->toString();

    emulNet->ENsend(&memberNode->addr, primary_replica.getAddress(), pri);
    emulNet->ENsend(&memberNode->addr, secondary_replica.getAddress(), sec);
    emulNet->ENsend(&memberNode->addr, tertiary_replica.getAddress(), ter);

    vector<int> tempVector;
    tempVector.push_back(UPDATE);
    tempVector.push_back(0);
    tempVector.push_back(par->getcurrtime());
    trackMsgType[transaction_id] = tempVector;

    vector<string> tempKeyValueVector;
    tempKeyValueVector.push_back(key);
    tempKeyValueVector.push_back(value);
    trackKeyValue[transaction_id] = tempKeyValueVector;

}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */

void MP2Node::clientDelete(string key){
	/*
	 * Implement this
	 */
    vector<Node> replicas = findNodes(key);

    int transaction_id = g_transID++;
    Node primary_replica = replicas.at(0);
    Node secondary_replica = replicas.at(1);
    Node tertiary_replica = replicas.at(2);

    Message *primary = new Message(transaction_id, memberNode->addr, DELETE, key);
    Message *secondary = new Message(transaction_id, memberNode->addr, DELETE, key);
    Message *tertiary = new Message(transaction_id, memberNode->addr, DELETE, key);

    string pri = primary->toString();
    string sec = secondary->toString();
    string ter = tertiary->toString();

#if 0
    Address *addr = primary_replica.getAddress();
    printf("Primary : %d.%d.%d.%d:%d \n", addr->addr[0], addr->addr[1], addr->addr[2], addr->addr[3], *(short *)&addr->addr[4]);
    addr = secondary_replica.getAddress();
    printf("Secondary : %d.%d.%d.%d:%d \n", addr->addr[0], addr->addr[1], addr->addr[2], addr->addr[3], *(short *)&addr->addr[4]);
    addr = tertiary_replica.getAddress();
    printf("Tertiary : %d.%d.%d.%d:%d \n", addr->addr[0], addr->addr[1], addr->addr[2], addr->addr[3], *(short *)&addr->addr[4]);
#endif

    emulNet->ENsend(&memberNode->addr, primary_replica.getAddress(), pri);
    emulNet->ENsend(&memberNode->addr, secondary_replica.getAddress(), sec);
    emulNet->ENsend(&memberNode->addr, tertiary_replica.getAddress(), ter);

    vector<int> tempVector;
    tempVector.push_back(DELETE);
    tempVector.push_back(0);
    tempVector.push_back(par->getcurrtime());
    trackMsgType[transaction_id] = tempVector;

    vector<string> tempKeyValueVector;
    tempKeyValueVector.push_back(key);
    trackKeyValue[transaction_id] = tempKeyValueVector;

}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */
	// Insert key, value, replicaType into the hash table
    Entry entry = Entry(value, par->getcurrtime(), replica);
    bool status = ht->create(key,entry.convertToString());
    return status;
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key) {
	/*
	 * Implement this
	 */
	// Read key from local hash table and return value
    string value = ht->read(key);
    if (value != "") {
        value = Entry(value).value;
    }
    return value;
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */
	// Update key in local hash table and return true or false
    Entry entry = Entry(value, par->getcurrtime(), replica);
    bool status = ht->update(key, entry.convertToString());
    return status;
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key) {
	/*
	 * Implement this
	 */
	// Delete the key from the local hash table
    bool status = ht->deleteKey(key);
    return status;
}

// send a reply to the coordinator for msg type CREATE, UPDATE and DELETE
void MP2Node::sendReplyToCoordinator(Message *msg, bool result) {
    Message *message = new Message(msg->transID, msg->fromAddr,REPLY,result );
    string msg_str = message->toString();
    emulNet->ENsend(&(this->getMemberNode()->addr), &msg->fromAddr, msg_str);
}

// send a reply to the coordinator for msg type READ
void MP2Node::sendReadReplyToCoordinator(Message *msg) {
    Message *message = new Message(msg->transID, msg->fromAddr, msg->value);
    string msg_str = message->toString();
    emulNet->ENsend(&(this->getMemberNode()->addr), &msg->fromAddr, msg_str);
}
/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	char * data;
	int size;
	/*
	 * Declare your local variables here
	 */
    bool result;
    vector<int> tempVector;
    string read_key;
    vector<string> tempStringVector;
    string tempKey = "", tempValue = "";
    int msg_type, quorum_counter;

	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);
        Message *msg = new Message(message);

		/*
		 * Handle the message types here
		 */
        switch(msg->type) {

        case CREATE:
            result = createKeyValue(msg->key,msg->value,msg->replica);
            if(result == true && msg->transID != -1){
                log->logCreateSuccess(&(msg->fromAddr),false,
                                      msg->transID,msg->key,msg->value);
            } else {
                log->logCreateFail(&(msg->fromAddr),false,msg->transID,msg->key,msg->value);
            }
            sendReplyToCoordinator(msg,result);
            break;

        case READ:
            {
                read_key = readKey(msg->key);
                if(read_key != ""){
                    log->logReadSuccess(&(msg->fromAddr),false,msg->transID,msg->key,read_key);
                    msg->value = read_key;
                } else {
                    log->logReadFail(&(msg->fromAddr),false,msg->transID,msg->key);
                }
                sendReadReplyToCoordinator(msg);
            }
            break;

        case UPDATE:
            result = updateKeyValue(msg->key,msg->value,msg->replica);
            if(result == true){
                log->logUpdateSuccess(&(msg->fromAddr),false,
                                      msg->transID,msg->key,msg->value);
            } else {
                log->logUpdateFail(&(msg->fromAddr),false,msg->transID,msg->key,msg->value);
            }
            sendReplyToCoordinator(msg,result);
            break;

        case DELETE:
            result = deletekey(msg->key);
            if(result == true){
                log->logDeleteSuccess(&(msg->fromAddr),false,msg->transID,msg->key);
            } else {
                log->logDeleteFail(&(msg->fromAddr),false,msg->transID,msg->key);
            }
            sendReplyToCoordinator(msg,result);
            break;

        case REPLY:
            {
                if(msg->success == true && msg->transID != -1) {
                    // checking for message_type
                    tempVector = trackMsgType[msg->transID];
                    msg_type = tempVector[0];
                    quorum_counter = tempVector[1];
                    // waiting for quorum
                    quorum_counter += 1;
                    tempVector[1] = quorum_counter;
                    trackMsgType[msg->transID] = tempVector;

                    if(msg_type == CREATE){

                        // fetching the key and the value from the data structure
                        tempStringVector = trackKeyValue[msg->transID];
                        tempKey = tempStringVector[0];
                        tempValue = tempStringVector[1];

                        if(quorum_counter == 2) {
                            // got quorum
                            log->logCreateSuccess(&(this->getMemberNode()->addr),true,
                                                  msg->transID,tempKey,tempValue);
                        }
                    } else if(msg_type == DELETE){

                        tempStringVector = trackKeyValue[msg->transID];
                        tempKey = tempStringVector[0];

                        if(quorum_counter == 2) {
                            log->logDeleteSuccess(&(this->getMemberNode()->addr),true,
                                                  msg->transID,tempKey);
                        }
                    } else if(msg_type == UPDATE){

                        tempStringVector = trackKeyValue[msg->transID];
                        tempKey = tempStringVector[0];
                        tempValue = tempStringVector[1];

                        if(quorum_counter == 2) {
                            log->logUpdateSuccess(&(this->getMemberNode()->addr),true,
                                                  msg->transID,tempKey,tempValue);
                        }
                    }

                } else if(msg->transID != -1 ) {
                    // checking the message type
                    tempVector = trackMsgType[msg->transID];
                    msg_type = tempVector[0];
                    quorum_counter = tempVector[1];
                    // waiting for quorum
                    quorum_counter += 1;
                    tempVector[1] = quorum_counter;
                    trackMsgType[msg->transID] = tempVector;

                    if(msg_type == CREATE){

                        tempStringVector = trackKeyValue[msg->transID];
                        tempKey = tempStringVector[0];
                        tempValue = tempStringVector[1];
                        if(quorum_counter == 2) {
                            // got quorum
                            log->logCreateFail(&memberNode->addr,true,msg->transID,tempKey,tempValue);
                        }

                    } else if(msg_type == DELETE) {

                        tempStringVector = trackKeyValue[msg->transID];
                        tempKey = tempStringVector[0];
                        // checking quorum
                        if(quorum_counter == 2) {
                            log->logDeleteFail(&memberNode->addr,true,msg->transID,tempKey);
                        }
                    } else if(msg_type == UPDATE){

                        tempStringVector = trackKeyValue[msg->transID];
                        tempKey = tempStringVector[0];
                        tempValue = tempStringVector[1];
                        if(quorum_counter == 2) {
                            // got quorum
                            log->logUpdateFail(&memberNode->addr,true,msg->transID,tempKey,tempValue);
                            trackKeyValue.erase(msg->transID);
                        }
                    }
                }
            }
            break;
        case READREPLY:
            {
                if(msg->value != "") {
                    // checking for message_type
                    tempVector = trackMsgType[msg->transID];
                    msg_type = tempVector[0];
                    quorum_counter = tempVector[1];
                    // waiting for quorum
                    quorum_counter += 1;
                    // assign back the quorum counter value & store it
                    tempVector[1] = quorum_counter;
                    trackMsgType[msg->transID] = tempVector;

                    // fetching the key and the value from the data structure
                    tempStringVector = trackKeyValue[msg->transID];
                    tempKey = tempStringVector[0];

                    if(quorum_counter == 2) {
                        // got quorum
                        log->logReadSuccess(&(this->getMemberNode()->addr),true,
                                              msg->transID,tempKey,msg->value);
                    }
                } else {
                    tempVector = trackMsgType[msg->transID];
                    msg_type = tempVector[0];
                    quorum_counter = tempVector[1];
                    // waiting for quorum
                    quorum_counter += 1;
                    tempVector[1] = quorum_counter;
                    trackMsgType[msg->transID] = tempVector;

                    tempStringVector = trackKeyValue[msg->transID];
                    tempKey = tempStringVector[0];
                    // checking quorum
                    if(quorum_counter == 2) {
                        log->logReadFail(&memberNode->addr,true,msg->transID,tempKey);
                        //trackKeyValue.erase(msg->transID);
                    }
                }
            }
            break;

        }

	}

	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;

    size_t i = pos;
    while(addr_vec.size() != 3) {
        if (ring[i].getAddress()->getAddress() != EMPTY_ADDRESS && i != (pos - 1)%RING_SIZE) {
            addr_vec.emplace_back(ring[i]);
        }
        i = (i + 1) % RING_SIZE;
    }
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol() {

    bool present = false;
    bool failed = false;
    int count_of_members = 0;

    int i = 0;
    while(i != RING_SIZE) {
        if (ring[i].getAddress()->getAddress() != EMPTY_ADDRESS) {
            count_of_members++;
        }
        i++;
    }

    vector<Node> curMemList = getMembershipList();
    if (count_of_members != curMemList.size())
        failed = true;

    if (failed == true) {
        // if a node has failed remove it from the ring
        for (int i = 0; i < RING_SIZE; i++) {
            present = false;
            for (int j = 0; j < curMemList.size(); j++) {
                if (curMemList[j].getAddress()->getAddress() == ring[i].getAddress()->getAddress()) {
                    present = true;
                    break;
                }
            }
            if (present == false) {
                // Stabilize the ring
                ring[i] = Node(Address(EMPTY_ADDRESS));
            }
        }
        auto hashTable = ht->hashTable;
        for (auto &entry: hashTable) {
            string key = entry.first;
            string value = entry.second;
            vector<Node> replicas = findNodes(key);
            Node primary_replica = replicas.at(0);
            Node secondary_replica = replicas.at(1);
            Node tertiary_replica = replicas.at(2);

            Message *primary = new Message(-1, memberNode->addr, CREATE, key, value, PRIMARY);
            Message *secondary = new Message(-1, memberNode->addr, CREATE, key, value, SECONDARY);
            Message *tertiary = new Message(-1, memberNode->addr, CREATE, key, value, TERTIARY);
            string pri = primary->toString();
            string sec = secondary->toString();
            string ter = tertiary->toString();
            emulNet->ENsend(&memberNode->addr, primary_replica.getAddress(), pri);
            emulNet->ENsend(&memberNode->addr, secondary_replica.getAddress(), sec);
            emulNet->ENsend(&memberNode->addr, tertiary_replica.getAddress(), ter);
        }
    }
}
