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

    memberNode->myPos = addNewMember(id, port, memberNode->heartbeat, memberNode->heartbeat);

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

   return 1;
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
	if (size < sizeof(MessageHdr)) return false;

    MessageHdr *msg = (MessageHdr *) data;
    switch (msg->msgType)
    {
    case JOINREQ:
        return handleJoinRequestMessage(data, size);
    case JOINREP:
        return handleJoinReplyMessage(data, size);
    
    default:
        return false;
    }
}

bool MP1Node::handleJoinRequestMessage(char *data, int size) {
    char addr[6];
    long heartbeat = 0;
    MessageHdr *msg = (MessageHdr *)data;

    memcpy(&addr, (char *) (msg+1), sizeof(addr));
    memcpy(&heartbeat, (char *)(msg+1) + 1 + sizeof(addr), sizeof(long));

    Address toaddr;
    *(int*)(toaddr.addr) = *(int*)addr;
	*(short *)(&toaddr.addr[4]) = *(short*)(&addr[4]);
    
    // Send the membership list as JOINREP message
    sendMembershipListTo(&toaddr, JOINREP);

    int id = *(int *)(&addr[0]);
    short port = *(short *)(&addr[4]);
    addNewMember(id, port, heartbeat, memberNode->heartbeat);

    return true;
}

bool MP1Node::handleJoinReplyMessage(char *data, int size) {
    MessageHdr *hdr = (MessageHdr *) data;
    int expected_size = sizeof(MessageHdr) + sizeof(int);
    if (size < expected_size) return false;

    int members_count;
    memcpy(&members_count, (char *) (hdr+1), sizeof(int));
    expected_size += members_count * (sizeof(int) + sizeof(short) + sizeof(long));
    if (size < expected_size) return false;
    
#ifdef DEBUGLOG
    log->LOG(&memberNode->addr, "Received member list %d", members_count);
#endif

    char *msg = ((char *) (hdr+1) + sizeof(int));
    for (int i = 0; i < members_count; i++) {
        int id;
        short port;
        long heartbeat;

        memcpy(&id, msg, sizeof(int));
        memcpy(&port, msg + sizeof(int), sizeof(short));
        memcpy(&heartbeat, msg + sizeof(int) + sizeof(short), sizeof(long));
        msg += sizeof(short) + sizeof(int) + sizeof(long);

        addNewMember(id, port, heartbeat, memberNode->heartbeat);
    }

    memberNode->nnb = members_count;

    return true;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
    memberNode->heartbeat++;

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

vector<MemberListEntry>::iterator MP1Node::addNewMember(int id, short port, long heartbeat, long timestamp) {
#ifdef DEBUGLOG
    log->LOG(&memberNode->addr, "Added new member %d:%d", id, port);
#endif
    memberNode->nnb++;
    return memberNode->memberList.insert(memberNode->memberList.end(), MemberListEntry(id, port, heartbeat, timestamp));
}

void MP1Node::sendMembershipListTo(Address *toaddr, MsgTypes type) {
    const int members_count = memberNode->nnb;
    const int member_size = sizeof(short) + sizeof(int) + sizeof(long);
    const size_t msgsize = 1 + sizeof(MessageHdr) + sizeof(int) + members_count * (member_size);

    char *msg = (char *) malloc(msgsize * sizeof(char));

    MessageHdr *hdr = (MessageHdr *)msg;
    hdr->msgType = type;

    memcpy((char *)(hdr+1), &members_count, sizeof(int));
    char *data = ((char*)(hdr+1) + sizeof(int));

    for (MemberListEntry& entry : memberNode->memberList) {
        //char addr[6];
        //memcpy(&addr[0], &entry.id, sizeof(int));
        //memcpy(&addr[4], &entry.port, sizeof(short));
        memcpy(data, &entry.id, sizeof(int));
        memcpy(data + sizeof(int), &entry.port, sizeof(short));
        // memcpy(data, &addr[0], sizeof(addr));
        memcpy(data + sizeof(int) + sizeof(short), &entry.heartbeat, sizeof(long));
        data += sizeof(short) + sizeof(int) + sizeof(long);
    }

    emulNet->ENsend(&memberNode->addr, toaddr, msg, msgsize);

    free(msg);    
}

string MP1Node::debugMessage(char *msg, int size) {
    string message = "Message=";
    for (int i = 0; i < size; i++) {
        message.push_back((char) (msg[i] + '0'));
    }
    return message;
}
