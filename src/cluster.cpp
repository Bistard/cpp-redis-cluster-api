#include "cluster.h"

namespace RedisClusterAPI
{

Cluster::Cluster(const char *ip, int port, int connect_timeout, int command_timeout, bool debug)
    : _port(port), _debug(debug)
{
    memset(_ip, 0, sizeof(_ip));
	strncpy(_ip, ip, strlen(ip));
    _pool = new SyncClusterPool(connect_timeout, command_timeout,
                                (ConnectFn *)redisConnect, 
                                (FreeConnectFn *)redisFree);
}

Cluster::~Cluster() 
{
    DisConnect();
}

bool Cluster::Connect()
{
    UpdatePoolType res = _pool->InitPool(_ip, _port);
    if (res == UPDATE_FALSE || res == UPDATE_UNCHANGED) {
        return false;
    }
    return true;
}

bool Cluster::DisConnect()
{
    if (_pool) {
        delete _pool;
        _pool = NULL;
    }
    return true;
}

bool Cluster::PingALL()
{
    MapPool *pool = _pool->GetMapPool();
    for (MapPool::iterator it = pool->begin(); it != pool->end(); it++) {
        redisContext *context = it->second.context;
        redisReply *reply = (redisReply *)redisCommand(context, "PING");
        if (reply == NULL) {
            return false;
        } else if ((reply->type == REDIS_REPLY_STATUS && 
                    !strcasecmp(reply->str, "PONG")) == 0) 
        {
            return false;
        }
        freeReplyObject(reply);
    }
    return true;
}

bool Cluster::Set(const char *key, const char *val)
{
    redisReply *reply = static_cast<redisReply *>(Command(key, "SET %s %s", key, val));
    if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
        return false;
    }
    freeReplyObject(reply);
    return true;
}

bool Cluster::Get(const char *key, std::string &output)
{
    redisReply *reply = static_cast<redisReply *>(Command(key, "GET %s", key));
    if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
        return false;
    }
    output.assign(reply->str);
    freeReplyObject(reply);
    return true;
}

int Cluster::processReply(const redisReply *reply, const char *&ip, int &port)
{
    if (reply == NULL || reply->str == NULL) {
        return FAILED;
    }

    if (reply->type == REDIS_REPLY_ERROR) {
        if (strcmp(reply->str, "CLUSTERDOWN") == 0) {
            return CLUSTERDOWN;
        } else {
            char *ptr1 = reply->str, *ptr2;
            ptr2 = strchr(ptr1, ' ');
            ptr1 = strchr(ptr2 + 1, ' ');
            *ptr1 = '\0';
            ptr2 = strrchr(ptr1 + 1, ':');
            *ptr2 = '\0';
            ip = ptr1 + 1;
            port = atoi(ptr2 + 1);
            if (strncmp(reply->str, "MOVED", 5) == 0) {
                return MOVED;
            } else if (strcmp(reply->str, "ASK") == 0) {
                return ASK;
            }
        }
    }
    return OK;
}

/////////////////////// PRIVATE MEMBER FUNCTIONS ///////////////////////////////

redisReply *Cluster::Command(std::string key, const char *format, ...)
{
    UpdatePoolType flag;
    va_list ap;
    va_start(ap, format);
    
    redisReply *reply = NULL;
    DoneCommand(key, format, ap, &reply);
    if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
        exit(EXIT_FAILURE);
    }

    const char *ip;
    int port;
    int state = processReply((const redisReply *)reply, ip, port);
    
    if (state == OK) {
        
    } else if (state == FAILED) {

    } else if (state == MOVED) {
        
        if (_debug) {
            printf("%s\n", reply->str);
        }

        freeReplyObject(reply);

        flag = _pool->UpdatePool();
        if (flag == UPDATE_FALSE || flag == UPDATE_UNCHANGED) {
            exit(EXIT_FAILURE);
        }
        DoneCommand(key, format, ap, &reply); // TODO: ap is changed?
        if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
            freeReplyObject(reply);
            return NULL;
        }

    } else if (state == ASK) {

        freeReplyObject(reply);

        redisContext *newcontext = redisConnect(ip, port);
        reply = (redisReply *)redisCommand(newcontext, "ASKING");
        if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
            freeReplyObject(reply);
            return NULL;
        }
        freeReplyObject(reply);
        // DoneCommand();
        if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
            return NULL;
        }

    } else if (state == CLUSTERDOWN) {
        printf("[cluster down]\n");
    }
    
    va_end(ap);
    return reply;
}

void Cluster::DoneCommand(std::string key, 
                          const char *format, 
                          va_list ap, 
                          redisReply **reply)
{
    int flag;
    char *cmd;
    int cmdlen = redisvFormatCommand(&cmd, format, ap);
    bool updated = false;
    
    ClusterNode *node = NULL;
    while (true) {
        node = _pool->GetNodeByKey(&key);

        flag = redisAppendFormattedCommand(node->second.context, cmd, cmdlen);
        if (flag == REDIS_ERR) {
            printf("[redisAppendFormattedCommand ERROR]\n");
        }

        flag = redisGetReply(node->second.context, (void **)reply);
        if (flag == REDIS_ERR) {
            freeReplyObject(*reply);

            // if updated the pool, still fails to send command
            if (updated) {
                break;
            }

            UpdatePoolType res = _pool->UpdatePool();
            // new master hasn't been elected yet, fails to send command
            if (res == UPDATE_FALSE || res == UPDATE_UNCHANGED) {
                break;
            }

            if (_debug) {
                printf("[POOL UPDATED]\n");
            }
            
            // new master has been elected, try to send the command again
            updated = true;
            continue;
        } else {
            // only when redisGetReply() successed
            free(cmd);
            return;
        }
    }
    // fails to send command
    *reply = NULL;
    free(cmd);
    return;
}

} // RedisClusterAPI
