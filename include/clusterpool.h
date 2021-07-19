#pragma once
#include <hiredis.h>
#include <async.h>

#include <string.h>
#include <string>
#include <map>
#include <stdarg.h>

#include "slothash.h"
#include "clustertypelist.h"
#include "cluster.h"
#include "asynccluster.h"

namespace RedisClusterAPI
{

#define REDIS_COMMAND_CLUSTER_SLOTS "CLUSTER SLOTS"

template <typename CONTEXT = redisContext>
class ClusterPool : public ClusterTypeList<CONTEXT>
{
public:
    typedef typename ClusterTypeList<CONTEXT>::Context         Context;
    typedef typename ClusterTypeList<CONTEXT>::Slot            Slot;
    typedef typename ClusterTypeList<CONTEXT>::SlotRange       SlotRange;
    typedef typename ClusterTypeList<CONTEXT>::ClusterNodeData ClusterNodeData;
    typedef typename ClusterTypeList<CONTEXT>::ClusterNode     ClusterNode;
    typedef typename ClusterTypeList<CONTEXT>::SlotCmp         SlotCmp;
    typedef typename ClusterTypeList<CONTEXT>::MapPool         MapPool;
    typedef typename ClusterTypeList<CONTEXT>::ConnectFn       ConnectFn;
    typedef typename ClusterTypeList<CONTEXT>::FreeConnectFn   FreeConnectFn;
public:
    ClusterPool(int connect_timeout, int command_timeout, 
                ConnectFn *connectFn, FreeConnectFn *freeConnectFn);
    ~ClusterPool();

    ClusterPool(const ClusterPool &) = delete;
    ClusterPool &operator=(const ClusterPool &) = delete;

    UpdatePoolType InitPool(const char *ip, int port);
    bool InitNode(ClusterNodeData &nodeContext, const char *ip, int port, const char *id);
    static bool InsertNode(MapPool *mapPool, SlotRange slots, ClusterNodeData node);
    ClusterNode *GetNodeBySlot(Slot index);
    ClusterNode *GetNodeByKey(const std::string *key);
    ClusterNode *GetNodeByCtx(const Context *context);
    ClusterNode *GetNodeByID(const char *id);

    UpdatePoolType UpdatePool();
    bool IsSamePool(const redisReply *reply);
    void ClearPool(MapPool *mapPool);
    void PrintPool();
    static void PrintNode(const ClusterNode *node, bool frontTab = false);

    MapPool *GetMapPool() { return _mapPool; }
    int GetConnectTimeout() { return _connect_timeout; }
    int GetCommandTimeout() { return _command_timeout; }
public:
    static const uint32_t FAILUREMAXCOUNT = 1;
private:
    MapPool *_mapPool;
    ConnectFn *_connectFn;
    FreeConnectFn *_freeConnectFn;
    int _connect_timeout;
    int _command_timeout;
};

} // RedisClusterAPI