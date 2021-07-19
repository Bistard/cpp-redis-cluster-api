#include "clusterpool.h"

namespace RedisClusterAPI
{

template<typename CONTEXT>
ClusterPool<CONTEXT>::ClusterPool(int connect_timeout, 
                                  int command_timeout,
                                  ConnectFn *connectFn,
                                  FreeConnectFn *freeConnectFn)
    : _connectFn(connectFn), 
      _freeConnectFn(freeConnectFn),
      _connect_timeout(connect_timeout),
      _command_timeout(command_timeout)
{
    if (connectFn == NULL || freeConnectFn == NULL) {
        exit(EXIT_FAILURE);
    }

    _mapPool = new MapPool();
}

template<typename CONTEXT>
ClusterPool<CONTEXT>::~ClusterPool() 
{   
    ClearPool(_mapPool);
    delete _mapPool;
}

template<typename CONTEXT>
UpdatePoolType ClusterPool<CONTEXT>::InitPool(const char *ip, int port)
{
    redisContext *context = NULL;
    redisReply *reply = NULL;

    if (_connect_timeout > 0) {
        context = redisConnectWithTimeout(ip, port, {_connect_timeout, 0});
    } else {
        context = redisConnect(ip, port);
    }
    
    if (context == NULL || context->err) {
        if (context) {
            redisFree(context);
        }
        return UPDATE_FALSE;
    }

    if (_command_timeout > 0) {
        redisSetTimeout(context, {_command_timeout, 0});
    }

    reply = (redisReply *)(redisCommand(context, REDIS_COMMAND_CLUSTER_SLOTS));
    if (reply == NULL) {
        redisFree(context);
        return UPDATE_FALSE;
    }

    if (reply->type == REDIS_REPLY_ERROR || reply->type != REDIS_REPLY_ARRAY) {
        redisFree(context);
        freeReplyObject(reply);
        return UPDATE_FALSE;
    }

    bool isSame = IsSamePool(reply);
    if (isSame) {
        redisFree(context);
        freeReplyObject(reply);
        return UPDATE_UNCHANGED;
    }

    MapPool *newMapPool = new MapPool();
    size_t master_cnt = reply->elements;
    bool err = false;
    
    for (int i = 0; i < master_cnt; i++) {
        if (reply->element[i]->type == REDIS_REPLY_ARRAY &&
            reply->element[i]->elements >= 3 &&
            reply->element[i]->element[0]->type == REDIS_REPLY_INTEGER &&
            reply->element[i]->element[1]->type == REDIS_REPLY_INTEGER &&
            reply->element[i]->element[2]->type == REDIS_REPLY_ARRAY &&
            reply->element[i]->element[2]->elements >= 2 &&
            reply->element[i]->element[2]->element[0]->type == REDIS_REPLY_STRING &&
            reply->element[i]->element[2]->element[1]->type == REDIS_REPLY_INTEGER) 
        {
            Slot slot_start = reply->element[i]->element[0]->integer;
            Slot   slot_end = reply->element[i]->element[1]->integer;
            SlotRange slots = { slot_start, slot_end };
            char *       ip = reply->element[i]->element[2]->element[0]->str;
            int        port = reply->element[i]->element[2]->element[1]->integer;
            char *       id = reply->element[i]->element[2]->element[2]->str;
            
            ClusterNodeData node;
            if (InitNode(node, ip, port, id) == false) {
                err = true;
                break;
            }
            if (InsertNode(newMapPool, slots, node) == false) {
                err = true;
                break;
            }
        } else {
            err = true;
            ClearPool(newMapPool);
            delete newMapPool;
            newMapPool = NULL;
            break;
        }
    }

    if (!err) {

        MapPool *oldMapPool = _mapPool;
        _mapPool = newMapPool;
        
        ClearPool(oldMapPool);
        delete oldMapPool;
        oldMapPool = NULL;
    } else {
    }
    
    redisFree(context);
    freeReplyObject(reply);
    
    if (err) {
        return UPDATE_FALSE;
    } else {
        return UPDATE_TRUE;
    }
}

template<typename CONTEXT>
bool ClusterPool<CONTEXT>::InitNode(ClusterNodeData &node, 
                                    const char *ip, 
                                    int port, 
                                    const char *id)
{
    Context *context = _connectFn(ip, port);
    if (context == NULL) {
        return false;
    }
    if (context->err) {
        _freeConnectFn(context);
        return false;
    }

    node = ClusterNodeData(false, ip, port, id, context);
    return true;
}

template<typename CONTEXT>
bool ClusterPool<CONTEXT>::InsertNode(MapPool *mapPool, 
                                      SlotRange slots, 
                                      ClusterNodeData node)
{
    mapPool->insert(typename MapPool::value_type(slots, node));
    return true;
}

template<typename CONTEXT>
auto ClusterPool<CONTEXT>::GetNodeBySlot(Slot index) -> ClusterNode *
{
    SlotRange range = {index + 1, 0};

    typename MapPool::iterator it = _mapPool->lower_bound(range);
    if (it != _mapPool->begin()) {
        it--;
    }

    if (it != _mapPool->end()) {
        SlotRange range = it->first;
        if (range.first <= index && index <= range.second) {
            return (ClusterNode *) &(*it);
        } else {
            return NULL;
        }
    }
    return NULL;
}

template<typename CONTEXT>
auto ClusterPool<CONTEXT>::GetNodeByKey(const std::string *key) -> ClusterNode *
{
    Slot index = SlotHash::slotByKey(key->c_str(), key->length());
    return GetNodeBySlot(index);
}

template<typename CONTEXT>
auto ClusterPool<CONTEXT>::GetNodeByCtx(const Context *context) -> ClusterNode *
{
    typename MapPool::iterator it;
    for (it = _mapPool->begin(); it != _mapPool->end(); it++) {
        if (context == it->second.context) {
            return (ClusterNode *) &(*it);
        }
    }
    return NULL;
}

template<typename CONTEXT>
auto ClusterPool<CONTEXT>::GetNodeByID(const char *id) -> ClusterNode *
{
    typename MapPool::iterator it;
    for (it = _mapPool->begin(); it != _mapPool->end(); it++) {
        if (strncmp(it->second.id, id, 41) == 0) {
            return (ClusterNode *) &(*it);
        }
    }
    return NULL;
}

template<typename CONTEXT>
UpdatePoolType ClusterPool<CONTEXT>::UpdatePool()
{
    int res;
    ClusterNodeData *nodeData;

    typename MapPool::iterator it;
    for (it = _mapPool->begin(); it != _mapPool->end(); it++) {
        nodeData = &(it->second);

        res = InitPool(nodeData->ip, nodeData->port);
        if (res == UPDATE_FALSE) {
            continue;
        }
        return UPDATE_TRUE;
    }

    return UPDATE_FALSE;
}

template<typename CONTEXT>
bool ClusterPool<CONTEXT>::IsSamePool(const redisReply *reply)
{
    if (reply == NULL) {
        return false;
    }

    size_t master_cnt = reply->elements;
    if (master_cnt != _mapPool->size()) {
        return false;
    }

    for (int i = 0; i < master_cnt; i++) {
        char *ip = reply->element[i]->element[2]->element[0]->str;
        int port = reply->element[i]->element[2]->element[1]->integer;
        char *id = reply->element[i]->element[2]->element[2]->str;

        ClusterNode *node = GetNodeByID(id);
        if (node == NULL) {
            return false;
        }
        ClusterNodeData &nodeData = node->second;
        
        if (strncmp(nodeData.id, id, 41) || strncmp(nodeData.ip, ip, 16) || nodeData.port != port) {
            return false;
        }
    }
    return true;
}

template<typename CONTEXT>
void ClusterPool<CONTEXT>::ClearPool(MapPool *mapPool)
{
    ClusterNodeData *nodeData = NULL;
    
    int count = 1;
    typename MapPool::iterator it;
    for (it = mapPool->begin(); it != mapPool->end(); it++) {
        nodeData = &(it->second);
        if (nodeData && nodeData->context) {
            try {
                _freeConnectFn(nodeData->context);
            }
            catch(const std::exception& e) {
                std::cerr << "[ClearPool() | free() | " << e.what() << "]\n";
            }
            
            nodeData->context = NULL;
        }
        count++;
    }
    mapPool->clear();
}

template<typename CONTEXT>
void ClusterPool<CONTEXT>::PrintPool()
{
    typename MapPool::iterator it;
    ClusterNodeData *nodeData;

    std::cout << "\n[pool | at " << &_mapPool << "]" << std::endl;
    for (it = _mapPool->begin(); it != _mapPool->end(); it++) {
        nodeData = &(it->second);
        std::cout << "\t[ID | " << nodeData->id << " | "
                  << "port | " << nodeData->port << " | "
                  << "ip | " << nodeData->ip << " | "
                  << "context | " << nodeData->context << " | "
                  << "connection | " << nodeData->connected << " | "
                  << "slot | " << it->first.first << " " << it->first.second 
                  << "]\n";
    }
    std::cout << std::endl;
}

template<typename CONTEXT>
void ClusterPool<CONTEXT>::PrintNode(const ClusterNode *node, bool frontTab)
{
    const ClusterNodeData *nodeData = &(node->second);
    if (frontTab) {
        std::cout << "\t";
    }
    std::cout << "\t[ID | " << nodeData->id << "|"
              << " port | " << nodeData->port << "|"
              << " ip | " << nodeData->ip << "|"
              << " context | " << nodeData->context
              << "]\n";
}

// Explicitly instantiate the template
template class ClusterPool<redisContext>;
template class ClusterPool<redisAsyncContext>;

} // RedisClusterAPI