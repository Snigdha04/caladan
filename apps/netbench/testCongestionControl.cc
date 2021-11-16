extern "C" {
#include <base/log.h>
#include <net/ip.h>
#include <unistd.h>
}

#include "net.h"
#include "runtime.h"
#include "sync.h"
#include "synthetic_worker.h"
#include "thread.h"
#include "timer.h"

#include <algorithm>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <numeric>
#include <random>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace {

using namespace std::chrono;
using sec = duration<double, std::micro>;

// <- ARGUMENTS FOR EXPERIMENT ->
// the number of worker threads to spawn.
// int threads;
// the remote UDP address of the server.
netaddr raddr;
// the mean service time in us.
// double st;
// number of iterations required for 1us on target server
constexpr uint64_t kIterationsPerUS = 65;  // 83
// Number of seconds to warmup at rate 0
constexpr uint64_t kWarmupUpSeconds = 5;

static std::vector<std::pair<double, uint64_t>> rates;

constexpr uint64_t kUptimePort = 8002;
constexpr uint64_t kUptimeMagic = 0xDEADBEEF;

struct uptime {
  uint64_t idle;
  uint64_t busy;
};

#define DATA_SIZE (10)

constexpr uint64_t kNetbenchPort = 8001;
struct payload {
  uint64_t work_iterations;
  char data[DATA_SIZE];
};

int request_rate_per_us;

// rx queue
// tx queue

FILE * f;

void ServerWorker(std::unique_ptr<rt::TcpConn> c){
    payload p;
    printf("----------Started Server Worker-----------\n");
    while(true) {
        // Receive a work request.
        // printf("----------Packet Read before -----------\n");
        ssize_t ret = c->ReadFull(&p, sizeof(p));
        // printf("----------Packet Read after-----------\n %lu \n", sizeof(p));
        if (ret != static_cast<ssize_t>(sizeof(p))) {
            if (ret == 0 || ret == -ECONNRESET) break;
            log_err("read failed, ret = %ld", ret);
            break;
        }

        // Perform fake work if requested.
        // uint64_t workn = ntoh64(p.work_iterations);
        // sleep(workn);
        // tweak the payload
        // we will be just printing it for now
        // or create a new file in local and write the data to it

        // printf("-------------received data--------------\n%s\n", p.data);

        // Send a work response
        // ssize_t sret = c->WriteFull(&p, ret);
        // if (sret != ret) {
        //     if (sret == -EPIPE || sret == -ECONNRESET) break;
        //     log_err("write failed, ret = %ld", sret);
        //     break;
        // }
    }
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
    // initiate a new reciever thread
    auto th = rt::Thread([&]{
        payload rp;

        while(1) {
            printf("Initiate the receive worker\n");
            ssize_t ret = c->ReadFull(&rp, sizeof(rp));
            if (ret != static_cast<ssize_t>(sizeof(rp))) {
                if (ret == 0 || ret < 0) break;
                panic("read failed, ret = %ld", ret);
            }
            // can tweak the received payload here
        }
    });

    // Synchronized start of load generation.
    starter->Done();
    starter->Wait();

    payload p;
    printf("-------------Populating payload-----------\n");

    // populate the payload here
    // read the data from the file
    f = fopen("./router.proto", "r");
    // char input[100000];

    int ch = getc(f);
    int i = 0;
    while (ch != EOF && i < DATA_SIZE) {
        p.data[i] = ch;
        printf("%c \n", ch);
        i++;
        ch = getc(f);
    }
    p.data[i] = EOF;
    fclose(f);
    // getchar();
    printf("The input file content:\n %s \n", p.data);
    // p.data = input;

    // initiate a writer on this thread
    while(true) {
        // payload p;

        // // populate the payload here
        // // read the data from the file
        // f = fopen("./router.proto", "r");
        // char input[1000000];

        // int ch = getc(f);
        // int i = 0;
        // while (ch != EOF) {
        //     input[i] = ch;
        //     i++;
        //     ch = getc(f);
        // }
        // input[i] = EOF;
        // fclose(f);
        // getchar();
        // printf("The input file content:\n %s \n", input);
        // p.data = input;
        // printf("-----------packet write----------------");
        ssize_t ret = c->WriteFull(&p, sizeof(payload));
        if (ret != static_cast<ssize_t>(sizeof(payload)))
            panic("write failed, ret = %ld", ret);
        // printf("-----------packet write finished----------------");
        // sleep(1);
        // rt::Sleep(100);
    }

    c->Shutdown(SHUT_RDWR);
    th.Join();
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
    if(argc < 3) {
        std::cerr << "usage: [cfg_file] [cmd] ..." << std::endl;
        return -EINVAL;
    }

    std::string cmd = argv[2];

    if(cmd.compare("server") == 0) {
        // call the server handler
        ret = runtime_init(argv[1], ServerHandler, NULL);
        if (ret) {
            printf("failed to start runtime\n");
            return ret;
        }
    }
    else if(cmd.compare("client") != 0) {
        std::cerr << "Invalid arguments" << std::endl;
        return -EINVAL;
    }

    // check the argument count
    if(argc < 6) {
        std::cerr << "usage: [cfg_file] client [#threads] [server_IP] [request_rate]" << std::endl;
        //  [service_duration] [request_rate]
        return -EINVAL;
    }

    ret = StringToAddr(argv[4], &raddr.ip);
    if (ret) return -EINVAL;
    raddr.port = kNetbenchPort;

    // st = std::stoi(argv[4]);
    request_rate_per_us = std::stoi(argv[5]);

    // call the client handler
    ret = runtime_init(argv[1], ClientHandler, NULL);
    if (ret) {
        printf("failed to start runtime\n");
        return ret;
    }

    return 0;
}
