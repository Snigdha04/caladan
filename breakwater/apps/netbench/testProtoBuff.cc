extern "C" {
#include <base/log.h>
#include <base/time.h>
#include <net/ip.h>
#include <unistd.h>
#include <breakwater/breakwater.h>
#include <breakwater/seda.h>
#include <breakwater/dagor.h>
#include <breakwater/nocontrol.h>
}

#include "cc/net.h"
#include "cc/runtime.h"
#include "cc/sync.h"
#include "cc/thread.h"
#include "cc/timer.h"
#include "breakwater/rpc++.h"
#include "./router.pb.h"

#include "synthetic_worker.h"

#include <algorithm>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <numeric>
#include <random>
#include <sstream>
#include <string>
#include <utility>
#include <vector>
#include <google/protobuf/message_lite.h>

#include <ctime>

#define MAXDATASIZE 4096
std::time_t timex;

barrier_t barrier;

constexpr uint16_t kBarrierPort = 41;

const struct crpc_ops *crpc_ops;
const struct srpc_ops *srpc_ops;

namespace {

// <- ARGUMENTS FOR EXPERIMENT ->
// the number of worker threads to spawn.
int threads;
// the remote UDP address of the server.
netaddr raddr;
// the mean service time in us.


/* server-side stat */
constexpr uint64_t kRPCSStatPort = 8002;
constexpr uint64_t kRPCSStatMagic = 0xDEADBEEF;

struct uptime {
  uint64_t idle;
  uint64_t busy;
};

constexpr uint64_t kNetbenchPort = 8001;
struct payload {
  uint64_t work_iterations;
  char data[20];
};


// rx queue
// tx queue

// FILE * f;

void ServerWorker(std::unique_ptr<rt::TcpConn> c){
    char recvbuf[MAXDATASIZE];
    char buf[MAXDATASIZE];
    printf("----------Started Server Worker-----------\n");
    // while(true) {


    printf("----------Packet Read before -----------\n");
    ssize_t ret = c->ReadFull(recvbuf, MAXDATASIZE);
    recvbuf[ret] = '\0';
    std::string data = recvbuf;
    router::RequestHeader p;
    p.ParseFromString(data);
    printf("People:\n");
    printf("user_id:\t %d \n", p.user_id());
    printf("request id:\t %d \n", p.user_request_id());

    // printf("----------Packet Read after-----------\n %lu \n", sizeof(p));
    // if (ret != static_cast<ssize_t>(sizeof(p))) {
    //     if (ret == 0 || ret == -ECONNRESET) break;
    //     log_err("read failed, ret = %ld", ret);
    //     break;
    // }

    // Perform fake work if requested.
    // uint64_t workn = ntoh64(p.work_iterations);
    // sleep(workn);
    // tweak the payload
    // we will be just printing it for now
    // or create a new file in local and write the data to it

    // printf("-------------received data--------------\n%s\n", p.data);

    // Send a work response
    std::string msg;
    router::RequestHeader req;
    req.set_user_id(2);
    req.set_user_request_id(89898);

    printf("Send Response:\n");
    printf("user_id:\t %d \n", req.user_id());
    printf("request id:\t %d \n", req.user_request_id());

    req.SerializeToString(&msg);

    sprintf(buf, "%s", msg.c_str());

    ssize_t sret = c->WriteFull(buf, sizeof(buf));
    if (sret != static_cast<ssize_t>(sizeof(buf)))
        panic("write failed, ret = %ld", sret);

    // }
    printf("----------Exitted while loop-----------\n");
}

void ServerHandler(void *arg) {
    std::unique_ptr<rt::TcpQueue> q(rt::TcpQueue::Listen({0, kNetbenchPort}, 4096));
    if (q == nullptr) panic("couldn't listen for connections");

    while (true) {
        rt::TcpConn *c = q->Accept();
        if (c == nullptr) panic("couldn't accept a connection");
        rt::Thread([=] { ServerWorker(std::unique_ptr<rt::TcpConn>(c)); }).Detach();
    }
}

void ClientWorker(rt::TcpConn *c, rt::WaitGroup *starter){
    printf("Initiate the worker thread\n");
    uint64_t startTime;
    char buf[MAXDATASIZE];
    char recvbuf[MAXDATASIZE];
    // router::RequestHeader p;
    // initiate a new reciever thread
    
    auto th = rt::Thread([&]{
        // payload rp;

        // while(1) {
        printf("Initiate the receive worker\n");
        ssize_t ret = c->ReadFull(recvbuf, MAXDATASIZE);
        recvbuf[ret] = '\0';
        uint64_t finishTime = microtime();
        std::string data = recvbuf;
        // printf("Data : %s\n", data.c_str());
        router::RequestHeader p;
        p.ParseFromString(data);
        printf("People:\n");
        printf("user_id:\t %d \n", p.user_id());
        printf("request id:\t %d \n", p.user_request_id());

            // can tweak the received payload here
        // }
        printf("Time taken : %lu \n", finishTime - startTime);
    });

    // Synchronized start of load generation.
    starter->Done();
    starter->Wait();

    printf("-------------Populating payload-----------\n");

    std::string msg;
    router::RequestHeader req;
    req.set_user_id(1);
    req.set_user_request_id(12312);

    req.SerializeToString(&msg);
    

    sprintf(buf, "%s", msg.c_str());
    // send(fd, buf, sizeof(buf), 0);
    
    // printf("The input file content:\n %s \n", p.data);
    // p.data = input;

    // initiate a writer on this thread
    // while(true) {
        
    printf("-----------packet write----------------");
    startTime = microtime();
    ssize_t ret = c->WriteFull(buf, sizeof(buf));
    if (ret != static_cast<ssize_t>(sizeof(buf)))
        panic("write failed, ret = %ld", ret);
    printf("-----------packet write finished----------------");
    // sleep(request_rate_per_us);
    // }

    
    th.Join();
    c->Shutdown(SHUT_RDWR);
}

void ClientHandler(void *arg) {
    rt::TcpConn * c(rt::TcpConn::Dial({0, 0}, raddr));
    if (unlikely(c == nullptr)) panic("couldn't connect to raddr.");
    
    rt::WaitGroup starter(1 + 1);
    auto th = rt::Thread([&]{
        ClientWorker(c, &starter);
    });

    // Give the workers time to initialize, then start recording.
    starter.Done();
    starter.Wait();
    th.Join();
    c->Abort();
}

int StringToAddr(const char *str, uint32_t *addr) {
  uint8_t a, b, c, d;

  if (sscanf(str, "%hhu.%hhu.%hhu.%hhu", &a, &b, &c, &d) != 4) return -EINVAL;

  *addr = MAKE_IP_ADDR(a, b, c, d);
  return 0;
}

} // anonymous namespace

int main(int argc, char * argv[]) {

    int ret;

    if (argc < 4) {
        std::cerr << "usage: [alg] [cfg_file] [cmd] ...\n"
            << "\talg: overload control algorithms (breakwater/seda/dagor)\n"
            << "\tcfg_file: Shenango configuration file\n"
            << "\tcmd: netbenchd command (server/client/agent)" << std::endl;
        return -EINVAL;
    }

    std::string olc = argv[1]; // overload control
    if (olc.compare("breakwater") == 0) {
        crpc_ops = &cbw_ops;
        srpc_ops = &sbw_ops;
    } else if (olc.compare("seda") == 0) {
        crpc_ops = &csd_ops;
        srpc_ops = &ssd_ops;
    } else if (olc.compare("dagor") == 0) {
        crpc_ops = &cdg_ops;
        srpc_ops = &sdg_ops;
    } else if (olc.compare("nocontrol") == 0) {
        crpc_ops = &cnc_ops;
        srpc_ops = &snc_ops;
    } else {
        std::cerr << "invalid algorithm: " << olc << std::endl;
        std::cerr << "usage: [alg] [cfg_file] [cmd] ...\n"
            << "\talg: overload control algorithms (breakwater/seda/dagor)\n"
            << "\tcfg_file: Shenango configuration file\n"
            << "\tcmd: netbenchd command (server/client/agent)" << std::endl;
        return -EINVAL;
    }

    std::string cmd = argv[3];
    if (cmd.compare("server") == 0) {
        ret = runtime_init(argv[2], ServerHandler, NULL);
        if (ret) {
        printf("failed to start runtime\n");
        return ret;
        }
    } else if (cmd.compare("client") != 0) {
        std::cerr << "invalid command: " << cmd << std::endl;
        std::cerr << "usage: [alg] [cfg_file] [cmd] ...\n"
            << "\talg: overload control algorithms (breakwater/seda/dagor)\n"
            << "\tcfg_file: Shenango configuration file\n"
            << "\tcmd: netbenchd command (server/client/agent)" << std::endl;
        return -EINVAL;
    }

    if (argc < 6) {
        std::cerr << "usage: [alg] [cfg_file] client [nclients] "
            "[server_ip] [service_us] [service_dist] [slo] [nagents] "
            "[offered_load]\n"
            << "\talg: overload control algorithms (breakwater/seda/dagor)\n"
            << "\tcfg_file: Shenango configuration file\n"
            << "\tnclients: the number of client connections\n"
            << "\tserver_ip: server IP address\n"
            << "\tservice_us: average request processing time (in us)\n"
            << "\tservice_dist: request processing time distribution (exp/const/bimod)\n"
            << "\tslo: RPC service level objective (in us)\n"
            << "\tnagents: the number of agents\n"
            << "\toffered_load: load geneated by client and agents in requests per second"
            << std::endl;
        return -EINVAL;
    }

    threads = std::stoi(argv[4], nullptr, 0);

    ret = StringToAddr(argv[5], &raddr.ip);
    if (ret) return -EINVAL;
    raddr.port = kNetbenchPort;


    // call the client handler
    ret = runtime_init(argv[2], ClientHandler, NULL);
    if (ret) {
        printf("failed to start runtime\n");
        return ret;
    }

    return 0;
}
