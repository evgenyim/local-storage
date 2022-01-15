#include "kv.pb.h"
#include "log.h"
#include "protocol.h"
#include "rpc.h"

#include <array>
#include <cstdlib>
#include <cstring>
#include <sstream>
#include <string>

#include <errno.h>
#include <unistd.h>

#include <arpa/inet.h>
#include <resolv.h>
#include <sys/epoll.h>
#include <sys/socket.h>

static_assert(EAGAIN == EWOULDBLOCK);

using namespace NLogging;
using namespace NProtocol;
using namespace NRpc;

namespace {

////////////////////////////////////////////////////////////////////////////////
// TODO extract common code from client and simple_client

constexpr int max_events = 32;
constexpr int timeout = 1000;

}   // namespace

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv) {
    /*
     * simplistic arg parsing
     * TODO proper argparse lib
     */

    if (argc < 3) {
        return 1;
    }

    const auto port = atoi(argv[1]);
    const std::string key = argv[2];
    std::string v;
    if (argc > 3) {
        v = argv[3];
    }

    /*
     * socket initialization
     */

    int socketfd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (socketfd == -1) {
        return 1;
    }

    int epollfd = epoll_create1(0);
    if (epollfd == -1) {
        LOG_ERROR("epoll_create1 failed");
        return 1;
    }

    struct epoll_event event;
    event.events = EPOLLIN | EPOLLOUT | EPOLLET;
    event.data.fd = socketfd;
    if (epoll_ctl(epollfd, EPOLL_CTL_ADD, socketfd, &event) == -1) {
        LOG_ERROR("epoll_ctl failed");
        return 1;
    }

    struct sockaddr_in dest;
    bzero(&dest, sizeof(dest));
    dest.sin_family = AF_INET;
    dest.sin_port = htons(port);

    if (inet_pton(AF_INET, "127.0.0.1", &dest.sin_addr.s_addr) == 0) {
        perror("failed to convert address");
        return errno;
    }

    if (connect(socketfd, (struct sockaddr*)&dest, sizeof(dest)) != 0) {
        if (errno != EINPROGRESS) {
            perror("failed to connect");
            return errno;
        }
    }

    SocketState state;
    state.fd = socketfd;

    if (!v.empty()) {
        NProto::TPutRequest put_request;
        put_request.set_key(key);
        put_request.set_value(v);

        std::stringstream message;
        serialize_header(PUT_REQUEST, put_request.ByteSizeLong(), message);
        put_request.SerializeToOstream(&message);

        state.output_queue.push_back(message.str());
    } else {
        NProto::TGetRequest get_request;
        get_request.set_key(key);

        std::stringstream message;
        serialize_header(GET_REQUEST, get_request.ByteSizeLong(), message);
        get_request.SerializeToOstream(&message);

        state.output_queue.push_back(message.str());
    }

    /*
     * handler function
     */

    bool have_response = false;

    auto handle_get = [&] (const std::string& response) {
        NProto::TGetResponse get_response;
        if (!get_response.ParseFromArray(response.data(), response.size())) {
            // TODO proper handling

            abort();
        }

        LOG_INFO_S("get_response: " << get_response.ShortDebugString());

        have_response = true;

        return std::string();
    };

    auto handle_put = [&] (const std::string& response) {
        NProto::TPutResponse put_response;
        if (!put_response.ParseFromArray(response.data(), response.size())) {
            // TODO proper handling

            abort();
        }

        LOG_INFO_S("put_response: " << put_response.ShortDebugString());

        have_response = true;

        return std::string();
    };

    Handler handler = [&] (int fd, char message_type, const std::string& response) {
        switch (message_type) {
            case PUT_RESPONSE: return handle_put(response);
            case GET_RESPONSE: return handle_get(response);
        }

        // TODO proper handling

        abort();
        return std::string();
    };

    /*
     * rpc state and event loop
     */

    std::array<struct epoll_event, ::max_events> events;

    int num_ready = epoll_wait(epollfd, events.data(), max_events, timeout);
    for (int i = 0; i < num_ready; i++) {
        if (events[i].events & EPOLLIN) {
            VERIFY(events[i].data.fd == socketfd, "fd mismatch");

            LOG_INFO_S("socket " << socketfd << " connected");
        }
    }

    if (!process_output(state)) {
        LOG_ERROR("failed to send request");
        return 3;
    }

    while (!have_response) {
        num_ready = epoll_wait(epollfd, events.data(), max_events, timeout);
        for (int i = 0; i < num_ready; i++) {
            VERIFY(events[i].data.fd == socketfd, "fd mismatch");

            if (events[i].events & EPOLLIN) {
                if (!process_input(state, handler)) {
                    LOG_ERROR("failed to read response");
                    return 2;
                }
            }

            if ((events[i].events & EPOLLOUT)) {
                if (!process_output(state)) {
                    LOG_ERROR("failed to send request");
                    return 3;
                }
            }
        }
    }

    close(socketfd);

    return 0;
}
