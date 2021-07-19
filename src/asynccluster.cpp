#include "asynccluster.h"

namespace RedisClusterAPI
{

// int64_t GetCurrUsec();

////////////////////////////////// DATA ////////////////////////////////////////

CommandData::CommandData() : cmd(NULL), key(NULL), index(-1), 
                             cmdlen(0), retryCount(0) {}

CommandData::CommandData(char *c, std::string k, uint32_t idx, uint32_t len) 
    : cmd(c), index(idx), cmdlen(len) 
{
    key.assign(k);
}

CommandData::~CommandData()
{
    if (cmd) {
        free(cmd);
        cmd = NULL;
    }
}

AsyncClusterData::AsyncClusterData() : cmdData(NULL), privdata(NULL), err(0) {}

AsyncClusterData::AsyncClusterData(CommandData *commandData, void *data)
    : cmdData(commandData), privdata(data), err(0) {}

AsyncClusterData::~AsyncClusterData() 
{
    delete cmdData; 
    cmdData = NULL;
}

void AsyncClusterData::SetError(int type, const char *str)
{
    err = type;

    if (str == NULL) {
        return;
    }

    size_t len = strlen(str);
    len = len < (sizeof(msg) - 1) ? len : (sizeof(msg) - 1);
    memcpy(msg, str, len);
    msg[len] = '\0';
}

void AsyncClusterData::CleanError() 
{
    err = 0;
    msg[0] = '\0';
}

///////////////////////////// ASYNC CLUSTER ////////////////////////////////////

AsyncCluster::AsyncCluster(const char *ip, 
                           int port, 
                           int connect_timeout, 
                           int command_timeout, 
                           struct event_base *ev_base, 
                           AsyncClusterCallback *callback, 
                           bool debug)
    : _ev_base(ev_base), _callback(callback), _port(port), 
      _debug(debug), _running(false)
{
    memset(_ip, 0, sizeof(_ip));
	strncpy(_ip, ip, strlen(ip));
    _pool = new AsyncClusterPool(connect_timeout, command_timeout, 
                                 (ConnectFn *)redisAsyncConnect, 
                                 (FreeConnectFn *)redisAsyncFree);
    _failedCommandQueue = new std::queue<AsyncClusterData *>;
}

AsyncCluster::~AsyncCluster()
{
    DisConnect();

    delete _callback;
    _callback = NULL;
    
    while (_failedCommandQueue->empty() == false) {
        AsyncClusterData *acData = _failedCommandQueue->front();
        delete acData;
        _failedCommandQueue->pop();
    }

    delete _failedCommandQueue;
    _failedCommandQueue = NULL;

}

bool AsyncCluster::Connect()
{
    _pool->InitPool(_ip, _port);
    
    MapPool *mapPool = _pool->GetMapPool();
    MapPool::iterator it;
    for (it = mapPool->begin(); it != mapPool->end(); it++) {
        redisAsyncContext *context = it->second.context;
        context->data = (void *)this;
        redisLibeventAttach(context, _ev_base);
        redisAsyncSetConnectCallback(context, OnConnect);
        redisAsyncSetDisconnectCallback(context, OnDisconnect);
    }
    
    _running = true;
    return true;
}

bool AsyncCluster::DisConnect()
{
    _running = false;
    
    if (_pool) {
        delete _pool;
        _pool = NULL;
    }
    
    return true;
}

bool AsyncCluster::PingALL(void *privdata)
{
    MapPool *mapPool = _pool->GetMapPool();
    MapPool::iterator it;
    for (it = mapPool->begin(); it != mapPool->end(); it++) {
        redisAsyncContext *context = it->second.context;
        int flag = redisAsyncCommand(context, OnCommand, privdata, "PING");
        if (flag) {
            return flag;
        }
    }
    return true;
}

bool AsyncCluster::Set(const char *key, const char *val, void *privdata)
{
    return Command(key, privdata, "SET %s %s", key, val);
}

bool AsyncCluster::Get(const char *key, void *privdata)
{
    return Command(key, privdata, "GET %s", key);
}

bool AsyncCluster::Command(std::string key, 
                           void *privdata, 
                           const char *format, 
                           ...)
{
    va_list ap;
    va_start(ap, format);
    
    char *cmd;
    int cmdlen = redisvFormatCommand(&cmd, format, ap);

    Slot index = SlotHash::slotByKey(key.c_str(), key.length());
    ClusterNode *node = _pool->GetNodeBySlot(index);
    if (node == NULL) {
        free(cmd);
        return false;
    }

    redisAsyncContext *context = node->second.context;
    context->data = (void *)this;
    CommandData *cmdData = new CommandData(cmd, key, index, cmdlen);
    AsyncClusterData *acData = new AsyncClusterData(cmdData, privdata);

    int res = redisAsyncFormattedCommand(context, 
                                         OnCommand, 
                                         acData, 
                                         acData->cmdData->cmd, 
                                         acData->cmdData->cmdlen);
    if (res != REDIS_OK) {
        delete acData;
        return false;
    }
    
    return true;
}

bool AsyncCluster::DoneCommand(redisReply *reply, void *acdata, bool if_free)
{
    AsyncClusterData *acData = (AsyncClusterData *)acdata;
    if (acData == NULL) {
        return false;
    }

    if (reply == NULL || acData->err) {
        _callback->OnCommand(NULL, (void *)this, acData->privdata);
    } else {
        _callback->OnCommand(reply, (void *)this, acData->privdata);
    }

    if (if_free) {
        delete acData;
    }
    return true;
}

bool AsyncCluster::RetryCommand(redisAsyncContext *retryContext, void *acdata)
{
    AsyncClusterData *acData = (AsyncClusterData *)acdata;
    if (acData == NULL) {
        return false;
    }

    if (retryContext->c.flags & (REDIS_DISCONNECTING | REDIS_FREEING)) {
        acData->SetError(REDIS_ERR, "Don't accept new commands when the "
                                    "connection is about to be closed.");
        DoneCommand(NULL, acdata, true);
        return false;
    }

    int res = redisAsyncFormattedCommand(retryContext, 
                                         OnCommand,
                                         acData,
                                         acData->cmdData->cmd, 
                                         acData->cmdData->cmdlen);
    if (res != REDIS_OK) {
        return false;
    }
    
    return true;
}

int AsyncCluster::RetryFailedCommands()
{
    AsyncClusterData *acData = NULL;
    ClusterNode *node = NULL;
    redisAsyncContext *retryContext = NULL;

    int failurePendingCommandCount = 0;
    while (_failedCommandQueue->empty() == false) {
        
        acData = PopFailedCommand();
        node = _pool->GetNodeBySlot(acData->cmdData->index);

        if (node == NULL) {
            acData->SetError(REDIS_ERR, "cluster node cannot found");
            DoneCommand(NULL, acData, true);
            continue;
        }
        
        acData->CleanError();
        retryContext = node->second.context;
        bool res = RetryCommand(retryContext, acData);
        if (!res) {
            failurePendingCommandCount++;
        }
    }

    return failurePendingCommandCount;
}

void AsyncCluster::PushFailedCommand(AsyncClusterData *acdata)
{
    _failedCommandQueue->push(acdata);
}

AsyncClusterData *AsyncCluster::PopFailedCommand()
{
    if (_failedCommandQueue->size() == 0) {
        return NULL;
    }
    
    AsyncClusterData *acData = _failedCommandQueue->front();
    _failedCommandQueue->pop();
    return acData;
}

ReplyType AsyncCluster::ProcessReply(redisReply *reply)
{
    if (reply == NULL) {
        return FAILED;
    }

    if (reply->type == REDIS_REPLY_ERROR) {
        if ((int)strlen("MOVED") < reply->len && 
                    strncmp(reply->str, "MOVED", strlen("MOVED")) == 0) {
            return MOVED;
        } else if ((int)strlen("ASK") < reply->len && 
                   strncmp(reply->str, "ASK", strlen("ASK")) == 0) {
            return ASK;
        } else if ((int)strlen("TRYAGAIN") < reply->len &&
                   strncmp(reply->str, "TRYAGAIN", strlen("TRYAGAIN")) == 0) {
            return TRYAGAIN;
        } else if ((int)strlen("CROSSSLOT") < reply->len &&
                   strncmp(reply->str, "CROSSSLOT", strlen("CROSSSLOT")) == 0) {
            return CROSSSLOT;
        } else if ((int)strlen("CLUSTERDOWN") < reply->len &&
                   strncmp(reply->str, "CLUSTERDOWN", strlen("CLUSTERDOWN")) == 0) {
            return CLUSTERDOWN;
        } else {
            return SENTINEL;
        }
    }
    return OK;
}

UpdatePoolType AsyncCluster::UpdatePool()
{
    UpdatePoolType res = _pool->UpdatePool();
    
    if (res == UPDATE_UNCHANGED || res == UPDATE_FALSE) {
        return res;
    }

    MapPool *mapPool = _pool->GetMapPool();
    MapPool::iterator it;
    for (it = mapPool->begin(); it != mapPool->end(); it++) {
        redisAsyncContext *newcontext = it->second.context;
        newcontext->data = (void *)this;
        redisLibeventAttach(newcontext, _ev_base);
        redisAsyncSetConnectCallback(newcontext, OnConnect);
        redisAsyncSetDisconnectCallback(newcontext, OnDisconnect);
    }

    return UPDATE_TRUE;
}

//////////////////////////// CALLBACK FUNCTIONS ////////////////////////////////

void AsyncCluster::OnCommand(redisAsyncContext *context, void *r, void *acdata)
{
    if (context == NULL) {
        return;
    }

    AsyncCluster *asyncCluster = (AsyncCluster *)context->data;
    redisReply *reply = (redisReply *)r;
    AsyncClusterData *acData = (AsyncClusterData *)acdata;
    AsyncClusterPool *pool = NULL;
    ClusterNode *node = NULL;
    ClusterNodeData *nodeData = NULL;

    if (acData->cmdData == NULL) {
        delete acData;
        return;
    }
    
    if (reply == NULL) {
        // note:
        //   In Nordix/hiredis-cluster library, when enough NULL reply is
        // received, it updates the whole cluster pool after the master is 
        // considered as timed out.
        //   However in this case, once one NULL reply is received, the cluster
        // will temporarily store that command as failed command. Once enough 
        // NULL reply to the same node are recieved, the master is considered as
        // timeout. The API will update the cluster pool, if it updates 
        // successed, it will resend all the failed commands to the new 
        // corresponding node.

        acData->SetError(context->err, context->errstr);
        
        pool = asyncCluster->GetPool();
        node = pool->GetNodeBySlot(acData->cmdData->index);

        if (node == NULL) {
            asyncCluster->DoneCommand(NULL, acData, true);
            return;
        }

        //   Since the cluster pool has been updated and the cluster API will 
        // clean the old cluster pool, it will eventually call redisAsyncFree(), 
        // this function handles all the pending callbacks first, then will do 
        // actual freeing. Once the old callback with the old context is reached
        // , it needs to be resended to the new one.
        if (node->second.context != context) {
            asyncCluster->RetryCommand(node->second.context, acData);
            return;
        }
        
        nodeData = &(node->second);
        
        nodeData->failureCount++;
        if (nodeData->failureCount >= AsyncClusterPool::FAILUREMAXCOUNT) {
            
            // the master is considered as timed out
            nodeData->connected = false;
            nodeData->failureCount = 0;

            UpdatePoolType res = asyncCluster->UpdatePool();
            if (res == UPDATE_FALSE || res == UPDATE_UNCHANGED) {
                acData->SetError(res, res == UPDATE_FALSE ? 
                        "pool update error" : "pool update unchanged");
                asyncCluster->DoneCommand(NULL, acData, true);
                return;
            }
            
            asyncCluster->PushFailedCommand(acData);
            asyncCluster->RetryFailedCommands();
            return;
        }

        asyncCluster->_failedCommandQueue->push(acData);
        return;
    }
    
    // reply is received
    ReplyType state = asyncCluster->ProcessReply(reply);
    if (state >= FAILED && state <= SENTINEL) {
        
        // TODO: complete
        // acData->cmdData->retryCount++;
        // if (acData->cmdData->retryCount >= CommandData::RETRYMAXCOUNT) {
        //     acData->SetError(TOOMANYRETRIES, "too many retries");
        //     asyncCluster->DoneCommand(context, reply, acData);
        //     return;
        // }
        switch (state) {
        case FAILED:
            // printf("[FAILED]\n");
            // ...
            // asyncCluster->DoneCommand(context, reply, acData);
            break;

        case MOVED:
            // printf("[MOVED]\n");
            // ...
            // asyncCluster->DoneCommand(context, reply, acData);
            break;
        case ASK:
            // printf("[ASK]\n");
            // ...
            // asyncCluster->DoneCommand(context, reply, acData);
            break;
        case TRYAGAIN:
        case CROSSSLOT:
        case CLUSTERDOWN:
            // printf("[CLUSTERDOWN]\n");
            acData->SetError(REDIS_ERR, "CLUSTERDOWN");
            asyncCluster->DoneCommand(NULL, acData, true);
            return;
        default:
            // printf("[ERROR]\n");
            break;
        }

        asyncCluster->RetryCommand(context, acData);
        return;
    } else if (state == OK) {
        acData->CleanError();
        asyncCluster->DoneCommand(reply, acData, true);
        return;
    }
    // should never be reached here
}

void AsyncCluster::OnConnect(const redisAsyncContext *context, int status)
{
    if (context == NULL || context->data == NULL) {
        return;
    }

    AsyncCluster *asyncCluster = (AsyncCluster *)context->data;
    ClusterNode *node = asyncCluster->GetPool()->GetNodeByCtx(context);
     if (node == NULL) {
         return;
    }
    
    ClusterNodeData *nodeData = &(node->second);
    nodeData->connected = true;

    if (asyncCluster->_callback) {
        asyncCluster->_callback->OnConnect(context, status);
    }
    
    if (asyncCluster->is_debug()) {
        printf("[OnConnect() | context | %p | QUIT | state: %s]\n", context, 
                status == REDIS_OK ? "REDIS_OK" : "REDIS_ERROR");
    }
}

void AsyncCluster::OnDisconnect(const redisAsyncContext *context, int status)
{
    if (context->data == NULL) {
        return;
    }

    AsyncCluster *asyncCluster = (AsyncCluster *)context->data;    
    ClusterNode *node = asyncCluster->GetPool()->GetNodeByCtx(context);
    if (node == NULL) {
        return;
    }
    
    ClusterNodeData *nodeData = &(node->second);
    nodeData->connected = false;
    nodeData->context = NULL;

    if (asyncCluster->is_running()) {
        // handle unexpected disconnection here
    }
    
    if (asyncCluster->_callback) {
        asyncCluster->_callback->OnDisconnect(context, status);
    }

    if (asyncCluster->is_debug()) {
        printf("[OnDisconnect() | context | %p | QUIT | state: %s]\n", context, 
                status == REDIS_OK ? "REDIS_OK" : "REDIS_ERROR");
    }    
}

// not used for now
#if false
int64_t GetCurrUsec()
{
    int64_t usec;
    struct timeval now;
    int status;

    status = gettimeofday(&now, NULL);
    if (status < 0) {
        return -1;
    }

    usec = (int64_t)now.tv_sec * 1000000LL + (int64_t)now.tv_usec;
    return usec;
}
#endif

} // RedisClusterAPI