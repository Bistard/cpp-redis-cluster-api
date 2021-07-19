#pragma once

#include <iostream>
#include <async.h>
#include <hiredis.h>
#include <map>

namespace RedisClusterAPI
{

template<typename CONTEXT>
class ClusterTypeList
{
public: 
    typedef CONTEXT                                       Context;
    typedef unsigned int                                  Slot;
    typedef std::pair<Slot, Slot>                         SlotRange;
    struct                                                ClusterNodeData;
    typedef std::pair<SlotRange, ClusterNodeData>         ClusterNode;
    struct                                                SlotCmp;
    typedef std::map<SlotRange, ClusterNodeData, SlotCmp> MapPool;

    typedef void (CommandCallbackFn)(redisReply *reply, void *self, void *data);
    typedef void (ConnectCallbackFn)(const redisAsyncContext *context, int status);
    typedef void (DisconnectCallbackFn)(const redisAsyncContext *context, int status);
    typedef Context *(ConnectFn)(const char *ip, int port);
    typedef void (FreeConnectFn)(Context *context);

    class ClusterNodeData 
    {
    public:
        ClusterNodeData() = default;
        ClusterNodeData(bool is_connected, const char *IP, int port, const char *ID, Context *ctx)
            : connected(is_connected), port(port), context(ctx), failureCount(0)
        { 
            strncpy(ip, IP, 16);
            strncpy(id, ID, 41);
        }
    public:
        bool connected;
        char ip[16];
        int port;
        char id[41];
        Context *context;
        uint32_t failureCount;
    };

    struct SlotCmp {
        bool operator()(const SlotRange &a, const SlotRange &b) const
        {
            return a.first < b.first;
        }
    };

};

enum ReplyType {
    OK = 50,
    FAILED,
    MOVED,
    ASK,
    TRYAGAIN,
    CROSSSLOT,
    CLUSTERDOWN,
    SENTINEL
};

enum UpdatePoolType {
    UPDATE_FALSE = 100,
    UPDATE_TRUE,
    UPDATE_UNCHANGED
};

} // RedisClusterAPI