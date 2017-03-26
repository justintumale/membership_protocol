/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 *
 * 	Justin Tumale
 * 	Coursera Cloud Computing 1
 * 	Membership Protocol Assignment
 * 	3/25/2017
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
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long);
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

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
    return 0;
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

int MP1Node::getIdFromAddress(string address) {
    size_t pos = address.find(":");
    int id = stoi(address.substr(0, pos));
    return id;
}

short MP1Node::getPortFromAddress(string address) {
    size_t pos = address.find(":");
    short port = (short)stoi(address.substr(pos + 1, address.size()-pos-1));
    return port;
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
    if (size < (int)sizeof(MessageHdr)) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Message received with size less than MessageHdr. Ignored.");
#endif
        return false;
    }

    MessageHdr* messageHdr = (MessageHdr*) data;
    MsgTypes msgType = messageHdr->msgType;

    if (msgType == JOINREQ) {
        return joinReqHandler(env, data+sizeof(MessageHdr), size - sizeof(MessageHdr)); //no need for the msgType anymore, so move address right
    } else if (msgType == JOINREP) {
        return joinRepHandler(env, data + sizeof(MessageHdr), size - sizeof(MessageHdr)); //no need for the msgType anymore, so move address right
    } else if (msgType == HEARTBEATREQ) {
        return heartbeatReqHandler(env, data + sizeof(MessageHdr), size - sizeof(MessageHdr));
    } else if (msgType == HEARTBEATREP){
        return heartbeatRepHandler(env, data + sizeof(MessageHdr), size - sizeof(MessageHdr));
    } else {
        return false;
    }
}

/**
 * FUNCTION NAME: joinReqHandler
 *
 * DESCRIPTION: Handler for JOINREQ messages
 */
bool MP1Node::joinReqHandler(void *env, char *data, int size) {
    cout << "start joinReqHandler..." << endl;

    if (size < (int)(sizeof(memberNode->addr.addr) + sizeof(long))) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Message JOINREQ received with size wrong. Ignored.");
#endif
        return false;
    }

    //Get requester information
    //message structure ---> MsgType, address, number of members, member data (id, port, heartbeat)
    Address requesterAddress;
    memcpy(requesterAddress.addr, data, sizeof(memberNode->addr.addr));     //extract requester's Address from data
    data += sizeof(memberNode->addr.addr);
    size -= sizeof(memberNode->addr.addr);

    long heartbeat;
    memcpy(&heartbeat, data, sizeof(long));   //extract heartbeat from data
    //data += sizeof(long);
    //size -= sizeof(long);

    //Get id and port
    string reqAddStr = (requesterAddress.getAddress());
    int id = getIdFromAddress(reqAddStr);
    short port = getPortFromAddress(reqAddStr);

    //update the member in the membership list
    updateMembershipList(id, port, heartbeat);

    //send membership list to requester
    sendMembershipList(&requesterAddress, JOINREP);

    cout << "...end joinReqHandler." << endl;
    return true;
}


/**
 * FUNCTION NAME: joinRepHandler
 *
 * DESCRIPTION: Handler for JOINREP messages
 */
bool MP1Node::joinRepHandler(void *env, char *data, int size) {
    cout << "start joinRepHandler..." << endl;

    if (size < (int)(sizeof(memberNode->addr.addr))) {
        return false;
    }

    Address replierAddr;
    memcpy(replierAddr.addr, data, sizeof(memberNode->addr.addr));          //extract replier's Address from data
    data += sizeof(memberNode->addr.addr);
    size -= sizeof(memberNode->addr.addr);

    if (!recvMembershipList(env, data, size, "JOINREP")) {
        return false;
    }
    this->memberNode->inGroup = true;

    cout << "...end joinRepHandler." << endl;
    return true;
}

/**
 * FUNCTION NAME: heartbeatReqHandler
 *
 * DESCRIPTION: Handler for HEARTBEATREQ messages. When a HEARTBEATREQ is received by a node,
 * that node also receives a copy of the requester node's membership list. Merge this membership list
 * with own. Next, reply to the requester node with a message containing own address and a HEARTBEATREP
 * msgType. When the requester node receives this node's message, this node's heartbeat number
 * is increased in the requester node's membership list.
 */
bool MP1Node::heartbeatReqHandler(void *env, char *data, int size) {
    cout << "start heartbeatReqHandler..." << endl;

    if (size < (int)(sizeof(memberNode->addr.addr))) {
        return false;
    }

    Address requesterAddr;
    memcpy(requesterAddr.addr, data, sizeof(memberNode->addr.addr));    //extract Address from data
    data += sizeof(memberNode->addr.addr);
    size -= sizeof(memberNode->addr.addr);

    if (!recvMembershipList(env, data, size, "HEARTBEATREQ")) {         //extract membership list from the message
        return false;                                                   //and push the data into own membership list
    }

    //construct message of structure [msgType, address]
    size_t msgSize = sizeof(MessageHdr) + sizeof(memberNode->addr.addr);
    MessageHdr *msg = (MessageHdr*) malloc (msgSize);
    msg->msgType = HEARTBEATREP;
    memcpy((char*)(msg + 1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));

    emulNet->ENsend(&memberNode->addr, &requesterAddr,(char*)msg, msgSize);
    free(msg);

    cout << "...end heartbeatReqHandler." << endl;
    return true;
}

/**
 * FUNCTION NAME: heartbeatRepHandler
 *
 * DESCRIPTION: Handler for HEARTBEATREP messages. When a HEARTBEATREP message is received from a replier,
 * increase the replier's heartbeat number in own membership list.
 */
bool MP1Node::heartbeatRepHandler(void *env, char *data, int size) {
    cout << "start heartbeatRepHandler..." << endl;

    if (size < (int)(sizeof(memberNode->addr.addr))) {
        return false;
    }

    Address replierAddr;
    memcpy(&replierAddr.addr, data, sizeof(memberNode->addr.addr));

    int id = getIdFromAddress(replierAddr.getAddress());
    short port = getPortFromAddress(replierAddr.getAddress());

    for (vector<MemberListEntry>::iterator entry = memberNode->memberList.begin(); entry != memberNode->memberList.end(); entry++) {
        if (entry->id == id && entry->port == port) {
            entry->settimestamp( par->getcurrtime() );
            entry->heartbeat = entry->heartbeat + 1;
            return true;
        }
    }

    cout << "...end heartbeatRepHandler." << endl;
    return false;
}

/**
 * FUNCTION NAME: updateMembershipList
 *
 * DESCRIPTION: update the member passed into the argument in own member list. If the
 * member list does not contain the entry, create a new MemberListEntry and push it into
 * the list.
 */
void MP1Node::updateMembershipList(int id, short port, long heartbeat) {
    cout << "updating membership list ..." << endl;

    int sizeOfMemberList = this->memberNode->memberList.size();
    for (int i = 0; i < sizeOfMemberList; i++) {
        if (memberNode->memberList[i].id == id && memberNode->memberList[i].port == port) {
            if (heartbeat > memberNode->memberList[i].getheartbeat()) {
                memberNode->memberList[i].heartbeat = heartbeat;
                memberNode->memberList[i].settimestamp(par->getcurrtime());
            }
            return;
        }
    }

    //if the memberlist does not contain the entry, create a new one and push it into the list
    MemberListEntry entry(id, port, heartbeat, par->getcurrtime());
    memberNode->memberList.push_back(entry);

#ifdef DEBUGLOG
    Address logAddr;
    memcpy(&logAddr.addr[0], &entry.id, sizeof(int));
    memcpy(&logAddr.addr[4], &entry.port, sizeof(short));
    log->logNodeAdd(&memberNode->addr, &logAddr);
#endif

    cout << "...end updateMembershipList." << endl;
}

/**
 * FUNCTION NAME: updateMembershipList
 *
 * DESCRIPTION: update the membership list and update heartbeats
 */
void MP1Node::updateMembershipList(MemberListEntry& entry) {
    updateMembershipList(entry.getid(), entry.getport(), entry.getheartbeat());
}


/**
 * FUNCTION NAME: sendMembershipList
 *
 * DESCRIPTION: send a membership list to a node
 */
void MP1Node::sendMembershipList(Address *to, enum MsgTypes msgType) {
    cout << "start sendMembershipList ..." << endl;

    long numberOfMembers = memberNode->memberList.size();
    //message structure: [MessageHdr] [Address] [Number of members] [Members...]
    size_t msgsize = sizeof(MessageHdr) + sizeof(memberNode->addr.addr) + sizeof(long) + (numberOfMembers * (sizeof(int) + sizeof(short) + sizeof(log)));

    //malloc memory space for the header
    MessageHdr* msg = (MessageHdr *) malloc(msgsize * sizeof(char));

    //msg will be a pointer to an address
    char* data = (char*) (msg + 1);

    //set the message type
    msg->msgType = msgType;

    //set this node's address into the message
    memcpy(data, &memberNode->addr.addr, sizeof(memberNode->addr.addr));
    data += sizeof(memberNode->addr.addr);

    //set the number of members (for now)
    char* numberOfMembersPtr = data;
    memcpy(numberOfMembersPtr, &numberOfMembers, sizeof(long));
    data += sizeof(long);

    //set the members in list and delete members that have failed
    for (vector<MemberListEntry>::iterator entry = memberNode->memberList.begin(); entry != memberNode->memberList.end();) {
        if (entry != memberNode->memberList.begin()) {
            if (par->getcurrtime() - entry->timestamp > TREMOVE) {
#ifdef DEBUGLOG
                Address toAddr;
                memcpy(&toAddr.addr[0], &entry->id, sizeof(int));
                memcpy(&toAddr.addr[4], &entry->port, sizeof(short));
                log->logNodeRemove(&memberNode->addr, &toAddr);
#endif
                entry = memberNode->memberList.erase(entry);
                numberOfMembers--;
                continue;
            }
            //dont copy the failed not into the data
            if (par->getcurrtime() - entry->timestamp > TFAIL) {
                numberOfMembers--;
                ++entry;
                continue;
            }
        }

        memcpy(data, &entry->id, sizeof(int));
        data += sizeof(int);
        memcpy(data, &entry->port, sizeof(short));
        data += sizeof(short);
        memcpy(data, &entry->heartbeat, sizeof(long));
        data += sizeof(long);

        ++entry;
    }

    //set new number of members members were deleted
    memcpy(numberOfMembersPtr, &numberOfMembers, sizeof(long));

    //read new message size in case members were deleted
    msgsize = sizeof(MessageHdr) + sizeof(memberNode->addr.addr) + sizeof(long) + (numberOfMembers * (sizeof(int) + sizeof(short) + sizeof(log)));

    //send membership list through network
    emulNet->ENsend(&memberNode->addr, to, (char *)msg, msgsize);   //send the membership list to network
    free(msg);

    cout << "...end sendMembershipList." << endl;
}

/**
 * FUNCTION NAME: recvMembershipList
 *
 * DESCRIPTION: receive a membership list from another node. Make necessary updates to own membership list.
 */
bool MP1Node::recvMembershipList(void *env, char *data, int size, const char * label) {

    //get the number of members
    long numberOfMembers;
    memcpy(&numberOfMembers, data, sizeof(long));
    data += sizeof(long);
    size -= sizeof(long);

    if (size < (int)(numberOfMembers * (sizeof(int) + sizeof(short) + sizeof(log)))) {
        return false;
    }

    //extract each member from data and update own membership list
    MemberListEntry entry;
    for (int i = 0; i < numberOfMembers; i++) {

        memcpy(&entry.id, data, sizeof(int));
        data += sizeof(int);
        memcpy(&entry.port, data, sizeof(short));
        data += sizeof(short);
        memcpy(&entry.heartbeat, data, sizeof(long));
        data += sizeof(long);
        entry.timestamp = par->getcurrtime();

        int id = entry.id;
        short port = entry.port;
        long heartbeat = entry.heartbeat;

        updateMembershipList(id, port, heartbeat);
    }

    return true;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Update own entry in membership list. Pick a random node to propogate your membership list to
 * (GOSSIP PROTOCOL).
 */
void MP1Node::nodeLoopOps() {
    cout << "start nodeLoopOps..." << endl;

    //completess and accuracy tests fail when this executed of par->getcurrtime() <= 3
    if (par->getcurrtime() > 3 && memberNode->memberList.size() > 1) {

        //update own node's heartbeat and timestamp
        int id = getIdFromAddress(memberNode->addr.getAddress());
        short port = getPortFromAddress(memberNode->addr.getAddress());
        for (vector<MemberListEntry>::iterator entry = memberNode->memberList.begin();
             entry != memberNode->memberList.end(); entry++) {
            if (entry->id == id && entry->port == port) {
                entry->settimestamp(par->getcurrtime());
                entry->heartbeat = entry->heartbeat + 1;
                break;
            }
        }

        //GOSSIP PROTOCOL: pick a random member to send the member list to
        int randomIndex = rand() % (memberNode->memberList.size() - 1) + 1;
        MemberListEntry &entry = memberNode->memberList[randomIndex];

        //check if that node has failed before sending member list to it
        if (par->getcurrtime() - entry.timestamp > TFAIL) {
            return;
        }

        //send member list
        Address toAddr;
        memcpy(&toAddr.addr[0], &entry.id, sizeof(int));
        memcpy(&toAddr.addr[4], &entry.port, sizeof(short));
        this->sendMembershipList(&toAddr, HEARTBEATREQ);
    }

    cout << "...end nodeLoopOps." << endl;
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

    //The very first entry will be the own node
    int id = *(int*)(&memberNode->addr.addr);
    int port = *(short*)(&memberNode->addr.addr[4]);
    MemberListEntry memberEntry(id, port, 0, par->getcurrtime());
    memberNode->memberList.push_back(memberEntry);
    memberNode->myPos = memberNode->memberList.begin();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2], addr->addr[3], *(short*)&addr->addr[4]) ;
}
