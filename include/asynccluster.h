#pragma once
#include <hiredis.h>
#include "../../depends/hiredis/include/async.h"
#include <event2/event.h>

#include <cstdlib>
#include "../../../depends/hiredis/include/adapters/libevent.h"

#include <string.h>
#include <string>
#include <map>
#include <queue>
#include <stdarg.h>

#include "slothash.h"
#include "clustertypelist.h"
#include "clusterpool.h"
#include "cluster.h"

namespace RedisClusterAPI
{

template<typename CONTEXT>
class ClusterPool;

template<typename CONTEXT>
class ClusterTypeList;

enum ReplyType;
enum UpdatePoolType;

class CommandData
{
public:
    CommandData();
    CommandData(char *c, std::string k, uint32_t idx, uint32_t len);
    ~CommandData();
public:
    char *cmd;
    std::string key;
    uint32_t index;
    uint32_t cmdlen;
    uint32_t retryCount; // TODO: not used for now
public:
    static const uint32_t RETRYMAXCOUNT = 5;
};

class AsyncClusterData
{
public:
    AsyncClusterData();
    AsyncClusterData(CommandData *commandData, void *data);
    ~AsyncClusterData();
    void SetError(int type, const char *str);
    void CleanError();
public:
    CommandData *cmdData;
    void *privdata;
    int err;
    char msg[128];
};

class AsyncClusterCallback : public ClusterTypeList<redisAsyncContext>
{
public:
    virtual void OnConnect(const redisAsyncContext *context, int status) = 0;
    virtual void OnDisconnect(const redisAsyncContext *context, int status) = 0;
    virtual void OnCommand(redisReply *reply, void *self, void *privdata) = 0;
};

// TODO: set a timer to constant RetryFailedCommands()
// TODO: right now, it only initializes Cluster with the given ip:port, but it should try all the possibilities in the config
class AsyncCluster : public ClusterTypeList<redisAsyncContext>
{
public:
    typedef ClusterPool<redisAsyncContext> AsyncClusterPool;
public:
    AsyncCluster(const char *ip, 
                 int port, 
                 int connect_timeout, 
                 int command_timeout, 
                 struct event_base *ev_base, 
                 AsyncClusterCallback *callback, 
                 bool debug = false);
    ~AsyncCluster();
    AsyncCluster(const AsyncCluster &) = delete;
    AsyncCluster& operator=(const AsyncCluster &) = delete;

    bool Connect();
    bool DisConnect();
    bool PingALL(void *privdata = NULL);
    bool Set(const char *key, const char *val, void *privdata = NULL);
    bool Get(const char *key, void *privdata = NULL);
public:
    bool Command(std::string key, void *privdata, const char *format, ...);
    bool DoneCommand(redisReply *reply, void *acdata, bool if_free);
    bool RetryCommand(redisAsyncContext *retryContext, void *acdata);
    int RetryFailedCommands();
    void PushFailedCommand(AsyncClusterData *acdata);
    AsyncClusterData *PopFailedCommand();
    
    static ReplyType ProcessReply(redisReply *reply); 
    UpdatePoolType UpdatePool();
    
    static void OnCommand(redisAsyncContext *context, void *reply, void *acdata);
    static void OnConnect(const redisAsyncContext *context, int status); 
    static void OnDisconnect(const redisAsyncContext *context, int status); 
public:
    bool is_debug() { return _debug; }
    bool is_running() { return _running; }
    struct event_base *GetEvBase() { return _ev_base; }
    AsyncClusterPool *GetPool() { return _pool; }
    std::queue<AsyncClusterData *> *GetFailedCommands() { return _failedCommandQueue; }
    void SetCallback(AsyncClusterCallback *callback) { _callback = callback; }
private:
    struct event_base *_ev_base;
    AsyncClusterPool *_pool;
    AsyncClusterCallback *_callback;
    std::queue<AsyncClusterData *> *_failedCommandQueue;
    char _ip[32];
	int _port;
    bool _debug;
    bool _running;
};

} // RedisClusterAPI