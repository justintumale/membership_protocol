/**********************************;
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
    MessageHdr* messageHdr = (MessageHdr*) data;
    MsgTypes msgType = messageHdr->msgType;

    if (msgType == JOINREQ) {
        cout << "JOINREQ -----------------" << endl;
        return joinReqHandler(env, data+sizeof(MessageHdr), size - sizeof(MessageHdr)); //no need for the msgType anymore, so move address right
    } else if (msgType == JOINREP) {
        cout << "JOINREP -----------------" << endl;
        return joinRepHandler(env, data + sizeof(MessageHdr),
                              size - sizeof(MessageHdr)); //no need for the msgType anymore, so move address right
    } else if (msgType == HEARTBEAT) {
        cout << "HEARTBEAT * * * * * ** * **" << endl;
        return heartbeatHandler(env, data + sizeof(MessageHdr), size - sizeof(MessageHdr));
    } else {
        cout << "other ---------" << endl;
        return false;
    }
}

/**
 * FUNCTION NAME: joinReqHandler
 *
 * DESCRIPTION: Handler for JOINREQ messages
 */
bool MP1Node::joinReqHandler(void *env, char *data, int size) {


    if (size < (int)(sizeof(memberNode->addr.addr) + sizeof(long))) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Message JOINREQ received with size wrong. Ignored.");
#endif
        return false;
    }

    //Get requester information
    //message structure ---> MsgType, address, heartbeat
    Address requesterAddress;
    memcpy(requesterAddress.addr, data, sizeof(getMemberNode()->addr.addr));
    long heartbeat;
    memcpy(&heartbeat, data+sizeof(getMemberNode()->addr.addr), sizeof(long));

    //memberListEntries will be updated. They require an id (int), port (short), heartbeat (long), and ts (long)
    //get the id and port from the data
    //int id = *(int*)(data  + sizeof(getMemberNode()->addr) + sizeof(long)); //??
    string reqAddStr = (requesterAddress.getAddress());
    int id = getIdFromAddress(reqAddStr);
    short port = getPortFromAddress(reqAddStr);

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
    cout << "start joinRepHandler..." << endl;
    recvMembershipList(env, data, size);
    this->memberNode->inGroup = true;
    cout << "...end joinRepHandler." << endl;
    return true;
}

/**
 * FUNCTION NAME: heartbeatHandler
 *
 * DESCRIPTION: Handler for HEARTBEAT messages
 */
bool MP1Node::heartbeatHandler(void *env, char *data, int size) {
    if (!memberNode->inGroup) {
        return false;
    }
    cout << "start heartbeatHandler..." << endl;
    recvMembershipList(env, data, size);
    cout << "...end heartbeatHandler." << endl;
    return true;
}

/**
 * FUNCTION NAME: updateMembershipList
 *
 * DESCRIPTION: update the membership list with the new joiner and update heartbeats
 */
void MP1Node::updateMembershipList(int id, short port, long heartbeat) {
    int sizeOfMemberList = this->memberNode->memberList.size();
    for (int i = 0; i < sizeOfMemberList; i++) {

        if (memberNode->memberList[i].id == id && memberNode->memberList[i].port == port) {
            if (heartbeat > memberNode->memberList[i].getheartbeat()) {
                memberNode->memberList[i].setheartbeat(heartbeat);
                memberNode->memberList[i].settimestamp(par->getcurrtime());
            }
            return;
        }
    }
    //if the memberlist does not contain the entry, create a new one and push it into the list
    MemberListEntry entry(id, port, heartbeat, par->getcurrtime());
    memberNode->memberList.push_back(entry);
    //Log node add
#ifdef DEBUGLOG
    Address logAddr;
    memcpy(&logAddr.addr[0], &entry.id, sizeof(int));
    memcpy(&logAddr.addr[4], &entry.port, sizeof(short));
    log->logNodeAdd(&memberNode->addr, &logAddr);
#endif
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
void MP1Node::sendMembershipList(int id, short port, long heartbeat, Address *to, enum MsgTypes msgType) {
    cout << "start sendMembershipList ..." << endl;

    long numberOfMembers = memberNode->memberList.size();
    //message structure: [MessageHdr] [Address] [Number of members] [Members...]
    size_t msgsize = sizeof(MessageHdr) + sizeof(Address) + sizeof(long) + (numberOfMembers * (sizeof(int) + sizeof(short) + sizeof(long)));

    //malloc memory space for the header
    MessageHdr* msg = (MessageHdr *) malloc(msgsize);

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
    for (vector<MemberListEntry>::iterator entry = memberNode->memberList.begin(); entry != memberNode->memberList.end(); entry++) {
        if (entry != memberNode->memberList.begin()) {
            if (par->getcurrtime() - entry->timestamp > TREMOVE) {
#ifdef DEBUGLOG
                Address toAddr;
                memcpy(&toAddr.addr[0], &entry->id, sizeof(int));
                memcpy(&toAddr.addr[4], &entry->port, sizeof(short));
                log->logNodeRemove(&memberNode->addr, &toAddr);
                cout << "REMOVING          ";
                cout << toAddr.getAddress() << endl;
#endif
                entry = memberNode->memberList.erase(entry);
                numberOfMembers--;
                continue;
            } else if (par->getcurrtime() - entry->timestamp > TFAIL) {
                numberOfMembers--;
                continue;
            }
        }
        memcpy(data, &entry->id, sizeof(int));
        data += sizeof(int);
        memcpy(data, &entry->port, sizeof(short));
        data += sizeof(short);
        memcpy(data, &entry->heartbeat, sizeof(long));
        data += sizeof(long);
    }

    //set new number of members members were deleted
    memcpy(numberOfMembersPtr, &numberOfMembers, sizeof(long));
    //readjust message size if members were deleted
    msgsize = sizeof(MessageHdr) + sizeof(Address) + sizeof(long) + (numberOfMembers * (sizeof(int) + sizeof(short) + sizeof(long)));


    cout << "...end sendMembershipList." << endl;

    emulNet->ENsend(&memberNode->addr, to, (char *)msg, msgsize);
    free(msg);
}

/**
 * FUNCTION NAME: recvMembershipList
 *
 * DESCRIPTION: receive a membership list and update this node's own membership list
 */
void MP1Node::recvMembershipList(void *env, char *data, int size) {
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

    //clear vector if not empty, since nodes being introduced/reintroduced should be empty
    if (memberNode->memberList.size() > 0) {
        memberNode->memberList.clear();
    }

    MemberListEntry entry;

    for (int i = 0; i < numberOfEntries; i++) {
        int id;
        short port;
        long heartbeat;
        int timestamp;

        memcpy(&entry.id, data, sizeof(int));
        memcpy(&entry.port, data + sizeof(int), sizeof(port));
        memcpy(&entry.heartbeat, data + sizeof(int) + sizeof(short), sizeof(long));
        entry.timestamp = par->getcurrtime();

        updateMembershipList(entry);

        if (i != numberOfEntries - 1) {
            data = data + sizeof(int) + sizeof(short) + sizeof(long);
        }
    }
}
/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
    cout << "begin nodeLoopOps..." << endl;
    /*
     * Your code goes here
     */

    int id = getIdFromAddress(memberNode->addr.getAddress());
    short port = getPortFromAddress(memberNode->addr.getAddress());


    if (memberNode->memberList.size() > 1 && par->getcurrtime() > 3) {

        vector<MemberListEntry>::iterator itr = memberNode->memberList.begin();
        while (itr != memberNode->memberList.end()) {
            if (itr->getid() == id && itr->getport == port) {
                itr->heartbeat = itr->heartbeat + 1;
                itr->timestamp = par->getcurrtime();
            } else if ((par->getcurrtime() - itr->gettimestamp()) > TREMOVE) { //failed node must be removed
                //Log node removal
                Address toAddr;
                memcpy(&toAddr.addr[0], &itr->id, sizeof(int));
                memcpy(&toAddr.addr[4], &itr->port, sizeof(short));
                log->logNodeRemove(&memberNode->addr, &toAddr);

                //remove node from list
                itr = memberNode->memberList.erase(itr);
                numberOfMembers--;
            }
            itr++;
        }

        //GOSSIP PROTOCOL: Pick random node to deliver the heartbeat to
        //srand(time(NULL));
        int randomIndex = rand() % (memberNode->memberList.size() - 1) + 1;
        MemberListEntry &entry = memberNode->memberList[randomIndex];

        //RETURN if the one chosen has failed
        if (par->getcurrtime() - entry.timestamp > TFAIL) {
            return;
        }

        //Get address of the node to send to
        Address toAddr;
        memcpy(&toAddr.addr[0], &entry.id, sizeof(int));
        memcpy(&toAddr.addr[4], &entry.port, sizeof(short));


        //Send the heartbeat
        sendMembershipList(entry.id, entry.port, entry.heartbeat, &toAddr, HEARTBEAT);
    }
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
