#include "kv.pb.h"
#include "log.h"
#include "protocol.h"
#include "rpc.h"
#include "storage.h"

#include <array>
#include <cstdio>
#include <cstring>
#include <cerrno>
#include <sstream>
#include <string>
#include <unordered_map>

#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <unistd.h>

#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <queue>
#include <thread>
#include <csignal>

static_assert(EAGAIN == EWOULDBLOCK);

using namespace NLogging;
using namespace NProtocol;
using namespace NRpc;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr int max_events = 32;
volatile std::sig_atomic_t running = 1;

////////////////////////////////////////////////////////////////////////////////

auto create_and_bind(std::string const& port)
{
    struct addrinfo hints;

    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = AF_UNSPEC; /* Return IPv4 and IPv6 choices */
    hints.ai_socktype = SOCK_STREAM; /* TCP */
    hints.ai_flags = AI_PASSIVE; /* All interfaces */

    struct addrinfo* result;
    int sockt = getaddrinfo(nullptr, port.c_str(), &hints, &result);
    if (sockt != 0) {
        LOG_ERROR("getaddrinfo failed");
        return -1;
    }

    struct addrinfo* rp = nullptr;
    int socketfd = 0;
    for (rp = result; rp != nullptr; rp = rp->ai_next) {
        socketfd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (socketfd == -1) {
            continue;
        }

        sockt = bind(socketfd, rp->ai_addr, rp->ai_addrlen);
        if (sockt == 0) {
            break;
        }

        close(socketfd);
    }

    if (rp == nullptr) {
        LOG_ERROR("bind failed");
        return -1;
    }

    freeaddrinfo(result);

    return socketfd;
}

////////////////////////////////////////////////////////////////////////////////

auto make_socket_nonblocking(int socketfd)
{
    int flags = fcntl(socketfd, F_GETFL, 0);
    if (flags == -1) {
        LOG_ERROR("fcntl failed (F_GETFL)");
        return false;
    }

    flags |= O_NONBLOCK;
    int s = fcntl(socketfd, F_SETFL, flags);
    if (s == -1) {
        LOG_ERROR("fcntl failed (F_SETFL)");
        return false;
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

SocketStatePtr invalid_state()
{
    return std::make_shared<SocketState>();
}

SocketStatePtr accept_connection(
    int socketfd,
    struct epoll_event& event,
    int epollfd)
{
    struct sockaddr in_addr;
    socklen_t in_len = sizeof(in_addr);
    int infd = accept(socketfd, &in_addr, &in_len);
    if (infd == -1) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return nullptr;
        } else {
            LOG_PERROR("accept failed with error");
            return invalid_state();
        }
    }

    std::string hbuf(NI_MAXHOST, '\0');
    std::string sbuf(NI_MAXSERV, '\0');
    auto ret = getnameinfo(
        &in_addr, in_len,
        const_cast<char*>(hbuf.data()), hbuf.size(),
        const_cast<char*>(sbuf.data()), sbuf.size(),
        NI_NUMERICHOST | NI_NUMERICSERV);

    if (ret == 0) {
        LOG_INFO_S("accepted connection on fd " << infd
            << "(host=" << hbuf << ", port=" << sbuf << ")");
    }

    if (!make_socket_nonblocking(infd)) {
        LOG_PERROR("make_socket_nonblocking failed");
        return invalid_state();
    }

    event.data.fd = infd;
    event.events = EPOLLIN | EPOLLOUT | EPOLLET;
    if (epoll_ctl(epollfd, EPOLL_CTL_ADD, infd, &event) == -1) {
        LOG_PERROR("epoll_ctl failed");
        return invalid_state();
    }

    auto state = std::make_shared<SocketState>();
    state->fd = infd;
    return state;
}


}   // namespace

////////////////////////////////////////////////////////////////////////////////

void signal_handler(int) {
    running = 0;
}

int main(int argc, const char** argv)
{
    if (argc < 2) {
        return 1;
    }

    /*
     * socket creation and epoll boilerplate
     * TODO extract into struct Bootstrap
     */

    signal(SIGINT, signal_handler);

    auto socketfd = ::create_and_bind(argv[1]);
    if (socketfd == -1) {
        return 1;
    }

    if (!::make_socket_nonblocking(socketfd)) {
        return 1;
    }

    if (listen(socketfd, SOMAXCONN) == -1) {
        LOG_ERROR("listen failed");
        return 1;
    }

    int epollfd = epoll_create1(0);
    if (epollfd == -1) {
        LOG_ERROR("epoll_create1 failed");
        return 1;
    }

    struct epoll_event event;
    event.data.fd = socketfd;
    event.events = EPOLLIN | EPOLLET;
    if (epoll_ctl(epollfd, EPOLL_CTL_ADD, socketfd, &event) == -1) {
        LOG_ERROR("epoll_ctl failed");
        return 1;
    }

    /*
     * handler function
     */

    // TODO on-disk storage
//    std::unordered_map<std::string, uint64_t> storage;
    Storage storage;
    PersistentStorage storage_;
    std::unordered_map<int, std::queue<std::string>> states_put_requests;
    std::mutex put_request_queue_mutex, state_output_queue_mutex;

    auto handle_get_number = [&] (const std::string& request) {
        NProto::TGetNumberRequest get_request;
        if (!get_request.ParseFromArray(request.data(), request.size())) {
            // TODO proper handling

            abort();
        }

        LOG_DEBUG_S("get_number_request: " << get_request.ShortDebugString());

        NProto::TGetNumberResponse get_response;
        get_response.set_request_id(get_request.request_id());
        auto p = storage_.find(get_request.key());
        if (p != nullptr) {
            get_response.set_offset(*p);
        }

        std::stringstream response;
        serialize_header(GET_NUMBER_RESPONSE, get_response.ByteSizeLong(), response);
        get_response.SerializeToOstream(&response);

        return response.str();
    };

    auto handle_put_number = [&] (int fd, const std::string& request) {
        NProto::TPutNumberRequest put_request;
        if (!put_request.ParseFromArray(request.data(), request.size())) {
            // TODO proper handling

            abort();
        }

        LOG_DEBUG_S("put_number_request: " << put_request.ShortDebugString());

        storage_.put(put_request.key(), put_request.offset());

        NProto::TPutNumberResponse put_response;
        put_response.set_request_id(put_request.request_id());

        std::stringstream response;
        serialize_header(PUT_NUMBER_RESPONSE, put_response.ByteSizeLong(), response);
        put_response.SerializeToOstream(&response);

        std::lock_guard<std::mutex> guard(put_request_queue_mutex);
        states_put_requests[fd].push(response.str());

        std::string r;

        return r;
    };

    auto handle_get = [&] (const std::string& request) {
        NProto::TGetRequest get_request;
        if (!get_request.ParseFromArray(request.data(), request.size())) {
            // TODO proper handling

            abort();
        }

        LOG_DEBUG_S("get_request: " << get_request.ShortDebugString());

        NProto::TGetResponse get_response;
        get_response.set_request_id(get_request.request_id());

        std::string ret;
        if (storage.get(get_request.key(), &ret))
            get_response.set_value(ret);

        std::stringstream response;
        serialize_header(GET_RESPONSE, get_response.ByteSizeLong(), response);
        get_response.SerializeToOstream(&response);

        return response.str();
    };

    auto handle_put = [&] (int fd, const std::string& request) {
        NProto::TPutRequest put_request;
        if (!put_request.ParseFromArray(request.data(), request.size())) {
            // TODO proper handling

            abort();
        }

        LOG_DEBUG_S("put_request2: " << put_request.ShortDebugString());

        storage.put(put_request.key(), put_request.value());

        NProto::TPutResponse put_response;
        put_response.set_request_id(put_request.request_id());

        std::stringstream response;
        serialize_header(PUT_RESPONSE, put_response.ByteSizeLong(), response);
        put_response.SerializeToOstream(&response);

        std::lock_guard<std::mutex> guard(put_request_queue_mutex);
        states_put_requests[fd].push(response.str());

        std::string r;

        return r;
    };

    Handler handler = [&] (int fd, char request_type, const std::string& request) {
        switch (request_type) {
            case PUT_REQUEST: return handle_put(fd, request);
            case GET_REQUEST: return handle_get(request);
            case PUT_NUMBER_REQUEST: return handle_put_number(fd, request);
            case GET_NUMBER_REQUEST: return handle_get_number(request);
        }

        // TODO proper handling

        abort();
        return std::string();
    };

    /*
     * rpc state and event loop
     * TODO extract into struct Rpc
     */

    std::array<struct epoll_event, ::max_events> events;
    std::unordered_map<int, SocketStatePtr> states;

    auto finalize = [&] (int fd) {
        LOG_INFO_S("close " << fd);

        close(fd);
        states.erase(fd);
        states_put_requests.erase(fd);
    };

    std::thread put_requests_thread(
            [&]() {
                while (running) {
                    storage.sync();
                    for(auto &item: states_put_requests) {
                        auto &q = states_put_requests[item.first];
                        auto state = states.at(item.first);
                        std::lock_guard<std::mutex> guard2(state_output_queue_mutex);
                        std::lock_guard<std::mutex> guard(put_request_queue_mutex);
                        while(!q.empty()) {
                            state->output_queue.push_back(q.front());
                            q.pop();
                        }
                        process_output(*state);
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                }
            }
            );

    while (running) {
        const auto n = epoll_wait(epollfd, events.data(), ::max_events, -1);

        {
            LOG_INFO_S("got " << n << " events");
        }

        for (int i = 0; i < n; ++i) {
            const auto fd = events[i].data.fd;

            if (events[i].events & EPOLLERR
                    || events[i].events & EPOLLHUP
                    || !(events[i].events & (EPOLLIN | EPOLLOUT)))
            {
                LOG_ERROR_S("epoll event error on fd " << fd);

                finalize(fd);

                continue;
            }

            if (socketfd == fd) {
                while (true) {
                    auto state = ::accept_connection(socketfd, event, epollfd);
                    if (!state) {
                        break;
                    }

                    states[state->fd] = state;
                    states_put_requests[state->fd] = std::queue<std::string>();
                }

                continue;
            }

            bool closed = false;
            if (events[i].events & EPOLLIN) {
                auto state = states.at(fd);
                std::lock_guard<std::mutex> guard(state_output_queue_mutex);
                if (!process_input(*state, handler)) {
                    LOG_INFO_S("FINILIZING1");
                    finalize(fd);
                    closed = true;
                }
            }

            if (events[i].events & EPOLLOUT && !closed) {
                auto state = states.at(fd);
                std::lock_guard<std::mutex> guard(state_output_queue_mutex);
                if (!process_output(*state)) {
                    LOG_INFO_S("FINILIZING2")
                    finalize(fd);
                }
            }
        }
    }

    LOG_INFO("exiting");

    put_requests_thread.join();
    close(socketfd);

    return 0;
}
