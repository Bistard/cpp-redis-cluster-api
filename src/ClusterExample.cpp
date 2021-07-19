#include "ClusterExample.h"

namespace RedisClusterAPI
{

static timeval _start;
static timeval _end;

static long int _TESTCASES;

ClusterExample::ClusterExample(long int testcases)
    : _ev_base(NULL),
      _asyncCluster(NULL),
      _cluster(NULL)
{ 
    _TESTCASES = testcases;
}

ClusterExample::~ClusterExample()
{
    if (_asyncCluster) {
        delete _asyncCluster;
        _asyncCluster = NULL;
    }
    if (_cluster) {
        delete _cluster;
        _cluster = NULL;
    }
    if (_ev_base) {
        event_base_free(_ev_base);
        _ev_base = NULL;
    }
}

/////////////////////////// REDIS CLUSTER API TESTS ////////////////////////////

void ClusterExample::cluster_test()
{
    if (_cluster == NULL) {
        _cluster = new Cluster(IP, PORT3, TIMEOUT, DEBUG_MODE);
    }

    if (_cluster->Connect()) {
        std::cout << "Connection Successed." << std::endl;
    }
    int a;
    std::cout << "continue? [Y/N]: ";
    std::cin >> a;
    cluster_set_test(_cluster, "GeForce_RTX_3060", "$900");
    cluster_set_test(_cluster, "GeForce_RTX_3060Ti", "$1400");
    cluster_set_test(_cluster, "GeForce_RTX_3070", "$1400");
    cluster_set_test(_cluster, "GeForce_RTX_3080", "$2300");
    cluster_set_test(_cluster, "GeForce_RTX_3090", "$3100");
    cluster_set_test(_cluster, "Radeon_RX_6700_XT", "$970");
    cluster_set_test(_cluster, "Radeon_RX_6800", "$1300");
    cluster_set_test(_cluster, "Radeon_RX_6800_XT", "$1600");
    cluster_set_test(_cluster, "Radeon_RX_6900_XT", "$1800");
    std::cout << "====================================" << std::endl;
    std::string buff;
    cluster_get_test(_cluster, "GeForce_RTX_3060", buff);
    cluster_get_test(_cluster, "GeForce_RTX_3060Ti", buff);
    cluster_get_test(_cluster, "GeForce_RTX_3070", buff);
    cluster_get_test(_cluster, "GeForce_RTX_3080", buff);
    cluster_get_test(_cluster, "GeForce_RTX_3090", buff);
    cluster_get_test(_cluster, "Radeon_RX_6700_XT", buff);
    cluster_get_test(_cluster, "Radeon_RX_6800", buff);
    cluster_get_test(_cluster, "Radeon_RX_6800_XT", buff);
    cluster_get_test(_cluster, "Radeon_RX_6900_XT", buff);

    if (_cluster->DisConnect()) {
        std::cout << "Disconnected." << std::endl;
    }
}

void ClusterExample::cluster_ask_moved_test()
{
    if (_cluster == NULL) {
        _cluster = new Cluster(IP, PORT3, TIMEOUT, DEBUG_MODE);
    }

    if (_cluster->Connect()) {
        std::cout << "Connection Successed." << std::endl;
    }
    int a;
    std::cout << "continue? [Y/N]: ";
    std::cin >> a;
}

void ClusterExample::cluster_set_test(Cluster *cluster, 
                                      const char *key, 
                                      const char *val)
{
    static int count = 1;
    printf("[SET case #%d]: ", count);
    if (cluster->Set(key, val)) {
        std::cout << "OK." << std::endl;
    } else {
        std::cout << "FAILED." << std::endl;
    }
    count++;
}

void ClusterExample::cluster_get_test(Cluster *cluster, 
                                      const char *key, 
                                      std::string &output)
{
    static int count = 1;
    printf("[GET case #%d]: %s => ", count, key);
    if (cluster->Get(key, output)) {
        std::cout << output << std::endl;
    }
    count++;
}

//////////////////////ASYNC REDIS CLUSTER API TESTS/////////////////////////////

void ClusterExample::async_cluster_test()
{
    if (_ev_base == NULL) {
        _ev_base = event_base_new();
    }
    if (_asyncCluster == NULL) {
        _asyncCluster = new AsyncCluster(IP, PORT3, 1, 1, _ev_base, NULL, DEBUG_MODE);
    }

    _asyncCluster->SetCallback(new TestAsyncClusterCallback());
    _asyncCluster->Connect();
    if (event_base_dispatch(_ev_base) == -1) {
        std::cout << "[event_base_dispatch error]" << std::endl;
    }
}

////////////////////STRESS ASYNC REDIS CLUSTER API TESTS////////////////////////

void ClusterExample::stress_cluster_test()
{
    if (_cluster == NULL) {
        _cluster = new Cluster(IP, PORT3, TIMEOUT, DEBUG_MODE);
    }

    char key[_TESTCASES + 1];
    if (_cluster->Connect()) {
        std::cout << "Connection Sccuessed." << std::endl;
    }
    for (int i = 0; i < _TESTCASES; i++) {
        sprintf(key, "%d", i);
        cluster_set_test(_cluster, key, key);
    }
    _cluster->DisConnect();
}

void ClusterExample::stress_async_cluster_test()
{
    if (_ev_base == NULL) {
        _ev_base = event_base_new();
    }
    if (_asyncCluster == NULL) {
        _asyncCluster = new AsyncCluster(IP, PORT3, 1, 1, _ev_base, NULL, DEBUG_MODE);
    }

    _asyncCluster->SetCallback(new TestStressAsyncClusterCallback());
    _asyncCluster->Connect();
    if (event_base_dispatch(_ev_base) == -1) {
        std::cout << "[event_base_dispatch error]" << std::endl;
    }
}

//////////////////////// TEST ASYNC CLUSTER CALLBACK ///////////////////////////

void ClusterExample::async_cluster_set_test(AsyncCluster *asyncCluster, 
                                            const char *key, 
                                            const char *val)
{
    static int count = 1;
    TestAsyncClusterData *data = new TestAsyncClusterData("SET", count, NULL);

    if (asyncCluster->Set(key, val, data) == false) {
        std::cout << "[SET failed]" << std::endl;
    }
    count++;
}

void ClusterExample::async_cluster_get_test(AsyncCluster *asyncCluster, 
                                            const char *key, 
                                            std::string *buff)
{
    static int count = 1;
    TestAsyncClusterData *data = new TestAsyncClusterData("GET", count, buff);
    if (asyncCluster->Get(key, data) == false) {
        std::cout << "[GET failed]" << std::endl;
    }
    count++;
}

void TestAsyncClusterCallback::OnDisconnect(const redisAsyncContext *context, 
                                            int status)
{

}

void TestAsyncClusterCallback::OnConnect(const redisAsyncContext *context, 
                                         int status)
{
    // to prevent trigger OnConnect() 'MASTER_COUNT' times 
    static bool only_test_one_time = true;
    if (only_test_one_time == false) {
        return;
    } 
    only_test_one_time = false;
    
    int a;
    std::cout << "continue? [Y/N]: ";
    std::cin >> a;
        
    AsyncCluster *asyncCluster = (AsyncCluster *)context->data;
    // #9
    ClusterExample::async_cluster_set_test(asyncCluster, "GeForce_RTX_3070", "$1400$");   // 6379
    ClusterExample::async_cluster_set_test(asyncCluster, "GeForce_RTX_3080", "$2300$");   // 6379
    ClusterExample::async_cluster_set_test(asyncCluster, "GeForce_RTX_3060", "$900$");    // 6381
    ClusterExample::async_cluster_set_test(asyncCluster, "GeForce_RTX_3060Ti", "$1400$"); // 6380
    ClusterExample::async_cluster_set_test(asyncCluster, "GeForce_RTX_3090", "$3100$");   // 6380
    ClusterExample::async_cluster_set_test(asyncCluster, "Radeon_RX_6700_XT", "$970$");   // 6380
    ClusterExample::async_cluster_set_test(asyncCluster, "Radeon_RX_6800", "$1300$");     // 6380
    ClusterExample::async_cluster_set_test(asyncCluster, "Radeon_RX_6800_XT", "$1600$");  // 6381
    ClusterExample::async_cluster_set_test(asyncCluster, "Radeon_RX_6900_XT", "$1800$");  // 6380
    // #9
    std::string buff; 
    ClusterExample::async_cluster_get_test(asyncCluster, "GeForce_RTX_3070", &buff);
    ClusterExample::async_cluster_get_test(asyncCluster, "GeForce_RTX_3080", &buff);
    ClusterExample::async_cluster_get_test(asyncCluster, "GeForce_RTX_3060", &buff);
    ClusterExample::async_cluster_get_test(asyncCluster, "GeForce_RTX_3060Ti", &buff);
    ClusterExample::async_cluster_get_test(asyncCluster, "GeForce_RTX_3090", &buff);
    ClusterExample::async_cluster_get_test(asyncCluster, "Radeon_RX_6700_XT", &buff);
    ClusterExample::async_cluster_get_test(asyncCluster, "Radeon_RX_6800", &buff);
    ClusterExample::async_cluster_get_test(asyncCluster, "Radeon_RX_6800_XT", &buff);
    ClusterExample::async_cluster_get_test(asyncCluster, "Radeon_RX_6900_XT", &buff);

    timeval exit = {20, 0};
    event_base_loopexit(asyncCluster->GetEvBase(), &exit);
}

void TestAsyncClusterCallback::OnCommand(redisReply *reply, void *self, void *privdata)
{
    if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
        return;
    }
    
    AsyncCluster *cluster = (AsyncCluster *)self;
    TestAsyncClusterData *data = (TestAsyncClusterData *)privdata;

    printf("[%s | #%d | %s]\n", data->cmd, data->cnt, reply->str);
    delete data;
}

//////////////////////// STRESS TEST ASYNC CLUSTER CALLBACK ////////////////////

void TestStressAsyncClusterCallback::OnDisconnect(const redisAsyncContext *context, 
                                                  int status)
{

}

void TestStressAsyncClusterCallback::OnConnect(const redisAsyncContext *context, 
                                               int status)
{
    // to prevent trigger OnConnect() 'MASTER_COUNT' times 
    static bool only_test_one_time = true;
    if (only_test_one_time == false) {
        return;
    } 
    only_test_one_time = false;

    int count = _TESTCASES;
    char key[10];
    AsyncCluster *cluster = (AsyncCluster *)context->data;

    char *allKeys[count];
    for (int i = 0; i < count; i++) {
        allKeys[i] = (char *)malloc(sizeof(char) * (10));
        sprintf(key, "%d", i);
        strcpy(allKeys[i], key);
    }
    
    /* 
    gettimeofday(&_start, NULL);
    for (int i = 0; i < count; i++) {
        cluster->Set(allKeys[i], allKeys[i], (void *)"SET");
    }
     */

    gettimeofday(&_start, NULL);
    printf("[start | sec: %ld | usec: %ld]\n", _start.tv_sec, _start.tv_usec);
    for (int i = 0; i < count; i++) {
        cluster->Get(allKeys[i], (void *)"GET");
    }
    // TODO: do free after actual excution time
    for (int i = 0; i < count; i++) {
        free(allKeys[i]);
    }
}

void TestStressAsyncClusterCallback::OnCommand(redisReply *reply, 
                                               void *self, 
                                               void *privdata)
{
    static int count = 0;
    count++;
    if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
        return;
    }
    AsyncCluster *cluster = (AsyncCluster *)self;
    const char *command = (const char *)privdata;
    
    if (count == _TESTCASES) {
        
        // FIX: might get wrong excution time
        gettimeofday(&_end, NULL);
        printf("[end | sec: %ld | usec: %ld]\n", _end.tv_sec, _end.tv_usec);
        
        long sec = _end.tv_sec - _start.tv_sec;
        long micros = ((sec * 1000000) + _end.tv_usec) - (_start.tv_usec);
        
        char arr[11];
        sprintf(arr, "%ld", micros);
        std::string micros_s(arr);
        micros_s.insert(floor(log10(sec)) + 1, ".");

        double elapsed = std::stod(micros_s);

        std::cout << "[async | "
                  << command
                  << " | "
                  << count
                  << " | Execution time: "
                  << elapsed
                  << "s | average per second: "
                  << (double)(count / elapsed) << "]\n";
        event_base_loopbreak(cluster->GetEvBase());
    }
}

} // RedisClusterAPI
