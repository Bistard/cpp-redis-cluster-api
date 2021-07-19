# cpp-reids-cluster-api
C++ redis cluster API.

Supporting features:
> * sync cluster API
> * async cluster API


# Sync cluster API
## Intro
> Given one of the IP:PORT in the cluster, the API will maintain a connection pool that auto connects all the masters.

> The local connection pool is not up-to-dated since it is a sync api.
> 
> If the API cannot send command to the master with coressponding hash slot, the API will try to send 'CLUSTER SLOTS' command to the other masters in order to update the local connection pool. If updates succcessed, the API will resend the same command to the correct master.
> 
> If it still cannot send out the command, the API will return false.

# Async cluster API
> Async API shares the same interfaces except some minor changes.
# Todo List
* sync and async cannot handle ASK/MOVED command.