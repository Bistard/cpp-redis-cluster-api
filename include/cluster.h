#pragma once
#include <hiredis.h>

#include <string.h>
#include <string>
#include <map>
#include <stdarg.h>

#include "slothash.h"
#include "clustertypelist.h"
#include "clusterpool.h"

namespace RedisClusterAPI
{

template<typename CONTEXT>
class ClusterPool;

template<typename CONTEXT>
class ClusterTypeList;

enum ReplyType;
enum UpdatePoolType;

// TODO: right now, it only initialized with the given ip:port, but it should try all the possibilities in the config
class Cluster : public ClusterTypeList<redisContext>
{
public:
    typedef ClusterPool<redisContext> SyncClusterPool;
public:
    Cluster(const char *ip, 
            int port, 
            int connect_timeout, 
            int command_timeout, 
            bool debug = false);
    ~Cluster();
    Cluster(const Cluster &) = delete;
    Cluster& operator=(const Cluster &) = delete;

    bool Connect();
    bool DisConnect();
    bool PingALL();
    bool Set(const char *key, const char *val);
    bool Get(const char *key, std::string &output);
public:
    SyncClusterPool *GetPool() { return _pool; }
    static int processReply(const redisReply *reply, const char *&ip, int &port);
private:
    redisReply *Command(std::string key, const char *format, ...);
    void DoneCommand(std::string key, const char *format, va_list ap, redisReply **reply);
private:
    SyncClusterPool *_pool;
    char _ip[32];
	int _port;
    bool _debug;
};

} // RedisClusterAPI