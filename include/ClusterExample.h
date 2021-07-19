#pragma once

#include <iostream>
#include <hiredis.h>

#include "cluster.h"
#include "asynccluster.h"

#include <eventhandler.h>
#include "event2/event.h"

#include <thread>
#include <time.h>
#include <cmath>

#define TEST_CASE_temp 333333
#define TEST_CASE_0 100
#define TEST_CASE_1 1000
#define TEST_CASE_2 10000
#define TEST_CASE_3 100000
#define TEST_CASE_4 1000000

#define IP "192.168.211.218"
#define PORT1 6379
#define PORT2 6380
#define PORT3 6381
#define TEST_PORT 8000
#define TIMEOUT 15000
#define DEBUG_MODE 1

namespace RedisClusterAPI
{

struct TestAsyncClusterData {
    char *cmd;
    int cnt;
    void *privdata;
    TestAsyncClusterData(char *command, int count, void *data) 
    {
        cmd = command;
        cnt = count;
        privdata = data;
    }
    ~TestAsyncClusterData() = default;
};

class TestAsyncClusterCallback : public AsyncClusterCallback
{
public:
    virtual void OnDisconnect(const redisAsyncContext *context, int status);
    virtual void OnConnect(const redisAsyncContext *context, int status);
    virtual void OnCommand(redisReply *reply, void *self, void *data);
};

class TestStressAsyncClusterCallback : public AsyncClusterCallback
{
public:
    virtual void OnDisconnect(const redisAsyncContext *context, int status);
    virtual void OnConnect(const redisAsyncContext *context, int status);
    virtual void OnCommand(redisReply *reply, void *self, void *data);
};

class ClusterExample
{
public:
    ClusterExample(long int testcases);
    ~ClusterExample();
    
    // sync
    void cluster_test();
    void cluster_ask_moved_test();
    
    // async
    void async_cluster_test();

    // stress test
    void stress_cluster_test();
    void stress_async_cluster_test();
public:
    static void cluster_set_test(Cluster *cluster, const char *key, const char *val);
    static void cluster_get_test(Cluster *cluster,const char *key, std::string &buff);

    static void async_cluster_set_test(AsyncCluster *asyncCluster, const char *key, const char *val);
    static void async_cluster_get_test(AsyncCluster *asyncCluster, const char *key, std::string *buff);
public:
    struct event_base *_ev_base;
    AsyncCluster *_asyncCluster;
    Cluster *_cluster;
};

} // RedisClusterAPI