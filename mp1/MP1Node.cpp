/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
    for( int i = 0; i < 6; i++ ) {
        NULLADDR[i] = 0;
    }
    this->memberNode = member;
    this->emulNet = emul;
    this->log = log;
    this->par = params;
    this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
        return false;
    }
    else {
        return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
    Queue q;
    return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
    /*
     * This function is partially implemented and may require changes
     */
    int id = *(int*)(&memberNode->addr.addr);
    int port = *(short*)(&memberNode->addr.addr[4]);

    memberNode->bFailed = false;
    memberNode->inited = true;
    memberNode->inGroup = false;
    // node is up!
    memberNode->nnb = 0;
    memberNode->heartbeat = 0;
    memberNode->pingCounter = TFAIL;
    memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
    MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
    /*
     * Your code goes here
     */
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
        return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
        return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
        ptr = memberNode->mp1q.front().elt;
        size = memberNode->mp1q.front().size;
        memberNode->mp1q.pop();
        recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
    /*
     * Your code goes here
     */
    MessageHdr* messageHdr = (MessageHdr*) data;
    MsgTypes msgType = messageHdr->msgType;


    if (msgType == JOINREQ) {
        cout << "JOINREQ -----------------" << endl;
        return joinReqHandler(env, data+sizeof(MessageHdr), size); //no need for the msgType anymore, so move address right
    } else if (msgType == JOINREP) {
        cout << "JOINREP -----------------" << endl;
        return joinRepHandler(env, data+sizeof(MessageHdr), size); //no need for the msgType anymore, so move address right
    } else {
        cout << "other ---------" << endl;
        return false;
    }

    return false;
}

/**
 * FUNCTION NAME: joinReqHandler
 *
 * DESCRIPTION: Handler for JOINREQ messages
 */
bool MP1Node::joinReqHandler(void *env, char *data, int size) {
    //Get requester information
    //message structure ---> MsgType, address, heartbeat
    Address requesterAddress;
    memcpy(requesterAddress.addr, data, sizeof(getMemberNode()->addr.addr));
    cout << "" << endl;
    cout << "requesterAddress: ";
    cout << requesterAddress.getAddress() << endl;
    long heartbeat;
    memcpy(&heartbeat, data+sizeof(getMemberNode()->addr.addr), sizeof(long));
    //cout << "heartbeat: ";
    //cout<< heartbeat << endl;

    //memberListEntries will be updated. They require an id (int), port (short), heartbeat (long), and ts (long)
    //get the id and port from the data
    //int id = *(int*)(data  + sizeof(getMemberNode()->addr) + sizeof(long)); //??
    string reqAddStr = (requesterAddress.getAddress());
    size_t pos = reqAddStr.find(":");
    int id = stoi(reqAddStr.substr(0, pos));
    short port = (short)stoi(reqAddStr.substr(pos + 1, reqAddStr.size()-pos-1));

    updateMembershipList(id, port, heartbeat);

    //send membership list
    sendMembershipList(id, port, heartbeat, &requesterAddress, JOINREP);

    return true;
}


/**
 * FUNCTION NAME: joinRepHandler
 *
 * DESCRIPTION: Handler for JOINREP messages
 */
bool MP1Node::joinRepHandler(void *env, char *data, int size) {
    //1. extract data from *data

    //get Address from *data
    Address addr;
    memcpy(&addr, (Address*)data, sizeof(Address));
    size -= sizeof(Address);
    //get heartbeat from *data
    long heartbeat;
    memcpy(&heartbeat, (long*)(data + sizeof(Address)), sizeof(long));
    size -= sizeof(long);
    //get members and put them into own list
        //what is the size?
    int numberOfEntries = size / ( sizeof(int) + sizeof(short) + sizeof(long) );
    for (int i = 0; i < numberOfEntries; i++) {
        int id;
        short port;
        long heartbeat;
        int timestamp;
        memcpy(&id, data, sizeof(int));
        memcpy(&port, data + sizeof(int), sizeof(port));
        memcpy(&heartbeat, data + sizeof(int) + sizeof(port), sizeof(long));    //<--- are these what was sent in sendMembershipList?
        memcpy(&timestamp, data + sizeof(int) + sizeof(port) + sizeof(long), sizeof(int)); //<--should this be local time?

        MemberListEntry entry(id, port, heartbeat, timestamp);
        memberNode->memberList.push_back((entry));
    }



    //2. updateMembershipList
    return true;
}

/**
 * FUNCTION NAME: updateMembershipList
 *
 * DESCRIPTION: update the membership list with the new joiner and update heartbeats
 */
void MP1Node::updateMembershipList(int id, short port, long heartbeat) {
    cout << "updating memberlist" << endl;
    int sizeOfMemberList = this->memberNode->memberList.size();
    cout << "size of membership list: "; cout << sizeOfMemberList << endl;
    for (int i = 0; i < sizeOfMemberList; i++) {
        if (heartbeat > memberNode->memberList[i].getheartbeat()) {
            cout << "setting [ id: ";
            cout << memberNode->memberList[i].getid();
            cout << " port: ";
            cout << memberNode->memberList[i].getport();
            cout << " ] to timestamp: ";
            cout << par->getcurrtime() << endl;
            memberNode->memberList[i].setheartbeat(heartbeat);
            memberNode->memberList[i].settimestamp(par->getcurrtime());
        } else {
            cout << "no heartbeat updates." << endl;
        }
    }
    MemberListEntry entry(id, port, heartbeat, par->getcurrtime());
    memberNode->memberList.push_back(entry);
}

/**
 * FUNCTION NAME: sendMembershipList
 *
 * DESCRIPTION: send a membership list to a node
 */
void MP1Node::sendMembershipList(int id, short port, long heartbeat, Address *to, enum MsgTypes msgType) {
    /*Memberlist entry structure:
     * int id, short port, long heartbeat, long timestamp
     */


    /* this is how messages are created and sent:
     *
     *  size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);
     *
     */
    //message size
    long numberOfMembers = memberNode->memberList.size();
    size_t msgsize = sizeof(MessageHdr) + sizeof(getMemberNode()->addr.addr) + sizeof(long)
                     + numberOfMembers * ( sizeof(int) + sizeof(short) + sizeof(long));

    //allocate MessageHdr with message size
    MessageHdr* msg = (MessageHdr*) malloc(msgsize);

    //allocate data
    char* data = (char*) (msg + 1);
    msg->msgType = msgType;
    memcpy(data, getMemberNode()->addr.addr, sizeof(getMemberNode()->addr.addr));            //address
    memcpy(data+sizeof(getMemberNode()->addr.addr), &memberNode->heartbeat, sizeof(long));   //heartbeat
    char* dataIndex= data+sizeof(getMemberNode()->addr.addr) + sizeof(long);

    //use iterator to 1. erase failed entries and 2. populate data to be sent
    for (vector<MemberListEntry>::iterator iterator = memberNode->memberList.begin(); iterator != memberNode->memberList.end(); iterator++) {
        if ((par->getcurrtime() - iterator->gettimestamp()) > TREMOVE) { //failed node must be removed
            iterator = memberNode->memberList.erase(iterator);
            numberOfMembers--;
            continue;
        }
        if ((par->getcurrtime() - iterator->gettimestamp()) > TFAIL) { //mark node as failed. ???
            continue;
        }

        memcpy(dataIndex, &iterator->id, sizeof(int));
        memcpy(dataIndex + sizeof(int), &iterator->port, sizeof(short));
        memcpy(dataIndex + sizeof(int) + sizeof(short), &iterator->heartbeat, sizeof(long));
        dataIndex = dataIndex + sizeof(int) + sizeof(short) + sizeof(long);
    }
    cout << "end loop" << endl;

    emulNet->ENsend(&memberNode->addr, to, (char *)msg, msgsize);
    free(msg);
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

    /*
     * Your code goes here
     */

    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
    return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
    memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
           addr->addr[3], *(short*)&addr->addr[4]) ;
}
