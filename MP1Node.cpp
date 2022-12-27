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

        for (int i = 0; i < par->EN_GPSZ; i++)
        {     
            int memberId = i+1;

            // dhsawhne: ID should be consistent with the address of nodes created
            // since node0 has ID of 1, need `i+1` defintion of the IDs
            MemberListEntry memberEntry(memberId, 0, 0, 0);
            memberNode->memberList.push_back(memberEntry);

            Address addr;
            this->PopulateAddress(&addr, memberId);

            log->logNodeAdd(&memberNode->addr, &addr);
        }
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
	memberNode->bFailed = false;
	memberNode->inited = false;
	memberNode->inGroup = false;
    
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;

    memberNode->memberList.clear();
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
    // dhsawhne: this is the main line that blocks further operations
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
    
    if (size < (int)sizeof(MessageHdr)) {
        #ifdef DEBUGLOG
                log->LOG(&memberNode->addr, "Message received with size less than MessageHdr. Ignored.");
        #endif
        return false;
    }

    // get the data back in the required format
    // the incoming message type
    MessageHdr incomingMsg;
    Address joinaddr;

    memcpy(&incomingMsg, data, sizeof(MessageHdr));
    memcpy(joinaddr.addr, data + sizeof(MessageHdr), sizeof(memberNode->addr.addr));

    // it will point to the data after both the message header and the `joinAddr`
    char* incomingNextPtr = data + sizeof(MessageHdr) + sizeof(memberNode->addr.addr);
    if (incomingMsg.msgType == JOINREQ)
    {
        int members = memberNode->memberList.size();

        // format: message type, sender (i.e. current node), # members, (# members * address of each member)
        size_t msgsize = sizeof(MessageHdr) +  sizeof(memberNode->addr.addr) + sizeof(int) + (members * sizeof(memberNode->addr.addr));
        MessageHdr* sendingMsg =  (MessageHdr*) malloc(msgsize * sizeof(char));

        sendingMsg->msgType = JOINREP;

        char* next = (char*)(sendingMsg+1);
        memcpy(next, &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        next += sizeof(memberNode->addr.addr);

        memcpy(next, &members, sizeof(int));
        next += sizeof(int);

        for(auto i : memberNode->memberList)
        {
            int id = i.getid();

            Address sendingAddr;
            *(int *)(&(sendingAddr.addr))=id;
            *(short *)(&(sendingAddr.addr[4]))=0;

            memcpy(next, &sendingAddr.addr, sizeof(sendingAddr.addr));

            next += sizeof(sendingAddr.addr);
        }

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, &joinaddr, (char *)sendingMsg, msgsize);

        free(sendingMsg);
    }

    if (incomingMsg.msgType == JOINREP)
    {
        int members = 0;
        memcpy(&members, incomingNextPtr, sizeof(int));
        incomingNextPtr += sizeof(int);

        // dhsawhne: TODO we included self in the membership list here and in the seed node
        // we shouldn't include self
        for (int i = 0; i < members; i++)
        {
            Address addr;
            memcpy(&addr, incomingNextPtr, sizeof(addr.addr));

            int id = 0;
            memcpy(&id, &(addr.addr[0]), sizeof(int));

            MemberListEntry memberEntry(id, 0, 0, 0);
            memberNode->memberList.push_back(memberEntry);

            log->logNodeAdd(&memberNode->addr, &addr);

            // increment the pointer
            incomingNextPtr += sizeof(addr.addr);
        }

        memberNode->inGroup = true;

        #ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "In the group.");
        #endif
    }

    if (incomingMsg.msgType == GOSSIP)
    {
        this->ReconcileGossipMembershipList(data);
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

    this->IncrementMetadataForSelf();
    this->TryRemoveExpiredMembers();
    this->GossipMembershipList();
    return;
}

void MP1Node::ReconcileGossipMembershipList(char* data)
{
    char* nextPtr = data + sizeof(MessageHdr) + sizeof(memberNode->addr.addr);

    int incomingMembers = 0;
    memcpy(&incomingMembers, nextPtr, sizeof(int));

    nextPtr += sizeof(int);

    for (int i = 0; i < incomingMembers; i++)
    {
        MemberListEntry incomingMemberEntry;
        memset(&incomingMemberEntry, '\0', sizeof(MemberListEntry));

        memcpy(&incomingMemberEntry, nextPtr, sizeof(MemberListEntry));
        nextPtr += sizeof(MemberListEntry);

        if (incomingMemberEntry.getid() == this->GetMemberNodeId())
        {
            // can't reconcile self, only increment counters for self
            continue;
        }

        bool found = false;
        auto internalMemberEntry = this->memberNode->memberList.begin();
        for (; internalMemberEntry < this->memberNode->memberList.end(); internalMemberEntry++)
        {
            if (internalMemberEntry->getid() == incomingMemberEntry.getid())
            {
                found = true;
                break;
            }
        }

        if (found)
        {
            if (incomingMemberEntry.getheartbeat() > internalMemberEntry->getheartbeat())
            {
                internalMemberEntry->heartbeat = incomingMemberEntry.getheartbeat();
                internalMemberEntry->timestamp = par->getcurrtime();
            }
        }
        else
        {
            // only add members if they're recently active gossip messages
            if (par->getcurrtime() - incomingMemberEntry.gettimestamp() < TFAIL)
            {
                MemberListEntry newMemberEntry(incomingMemberEntry);
                newMemberEntry.timestamp = par->getcurrtime();

                this->memberNode->memberList.push_back(newMemberEntry);

                Address addedAddress;
                this->PopulateAddress(&addedAddress, newMemberEntry.getid());
            }
        }
    }
}

void MP1Node::GossipMembershipList()
{
    // get all active members (including self)
    int activeMembers = 0;
    for (auto ptr = this->memberNode->memberList.begin(); ptr < this->memberNode->memberList.end(); ptr++)
    {
        // dhsawhne: > or >=?
        if ((par->getcurrtime()-ptr->gettimestamp()) >= TFAIL)
        {
            continue;
        }

        activeMembers++;
    }

    // MessageFormat: MessageHeader, source address, # members, (#members * struct MembershipListEntry)
    int msgSize = sizeof(MessageHdr) + sizeof(this->memberNode->addr.addr) + sizeof(int) +  (activeMembers*sizeof(MemberListEntry));
    MessageHdr* sendingMsg = (MessageHdr*) malloc(msgSize);
    sendingMsg->msgType = GOSSIP;

    char* nextPtr = (char*)(sendingMsg + 1);
    memcpy(nextPtr, this->memberNode->addr.addr, sizeof(this->memberNode->addr.addr));
    nextPtr += sizeof(this->memberNode->addr.addr);

    memcpy(nextPtr, &activeMembers, sizeof(int));
    nextPtr += sizeof(int);

    for (auto ptr = this->memberNode->memberList.begin(); ptr < this->memberNode->memberList.end(); ptr++)
    {
        // dhsawhne: > or >=?
        if ((par->getcurrtime()-ptr->gettimestamp()) >= TFAIL)
        {
            continue;
        }

        // cout << "SERIALIZE: " << "ID: " << ptr->getid() << " Port: " << ptr->getport() << " HB: " << ptr->getheartbeat() << " TS: " << ptr->gettimestamp() << endl;
        memcpy(nextPtr, &(*ptr), sizeof(MemberListEntry));
        nextPtr += sizeof(MemberListEntry);
    }

    // finally, send the message to each member
    // dhsawhne: optimize by sending to only few members
    for (auto ptr = this->memberNode->memberList.begin(); ptr < this->memberNode->memberList.end(); ptr++)
    {
        if (ptr->getid() == this->GetMemberNodeId())
        {
            // don't send message to self
            continue;
        }

        Address sendingAddress;
        this->PopulateAddress(&sendingAddress, ptr->getid());
       
        emulNet->ENsend(&memberNode->addr, &sendingAddress, (char *)sendingMsg, msgSize);
    }

    free(sendingMsg);
}

bool MP1Node::TryRemoveExpiredMembers()
{
    bool isAnyMemberRemoved = false;
    vector<MemberListEntry>::iterator ptr = this->memberNode->memberList.begin();
    for (;ptr < this->memberNode->memberList.end(); ptr++)
    {
        if (ptr->getid() == this->GetMemberNodeId())
        {
            // can't remove self
            continue;
        }

        // dhsawhne: > or >=?
        if ((par->getcurrtime()-ptr->gettimestamp()) >= TREMOVE)
        {
            Address removedAddress;
            this->PopulateAddress(&removedAddress, ptr->getid());

            this->memberNode->memberList.erase(ptr);
            isAnyMemberRemoved = true;

            log->logNodeRemove(&this->memberNode->addr, &removedAddress);
        }
    }

    return isAnyMemberRemoved;
}

void MP1Node::IncrementMetadataForSelf()
{
    vector<MemberListEntry>::iterator ptr = this->memberNode->memberList.begin();
    for (;ptr < this->memberNode->memberList.end(); ptr++)
    {
        if (ptr->getid() == this->GetMemberNodeId())
        {
            ptr->heartbeat++;
            ptr->timestamp++;
        }
    }
}

int MP1Node::GetMemberNodeId()
{
    int id = 0;
    memcpy(&id, this->memberNode->addr.addr, sizeof(int));

    return id;
}

int MP1Node::GetMemberNodePort()
{
    return 0;
}

void MP1Node::PopulateAddress(Address* address, int id)
{
    memset(address, '\0', sizeof(address->addr));

    address->addr[0] = id;

    short port = 0;
    address->addr[4] = port;
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
