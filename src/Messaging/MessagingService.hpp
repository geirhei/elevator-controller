#pragma once

#include <chrono>
#include <functional>
#include <future>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace Messaging {

struct Message {
    using Uuid = std::string;
    using Frame = std::string;

    Uuid uuid;
    std::vector<Frame> payload;
};

struct Event {
    enum class EventType {
        Enter,
        Evasive,
        Exit,
        Join,
        Leave,
        Shout,
        Silent,
        Stop,
        Whisper,
    } type;
    std::string peerName;
    std::string peerUuid;
    std::optional<Message> message;
};

class MessagingError : public std::runtime_error {
    using std::runtime_error::runtime_error;
};

class RequestTimedOutError : public MessagingError {
    using MessagingError::MessagingError;
};

/**
 * The implementations of this class shall be thread-safe.
 */
class MessagingService {
public:
    virtual ~MessagingService() = default;

    virtual void run() = 0;

    [[nodiscard]] virtual std::string peerName() const = 0;

    virtual std::future<std::string> sendRequest(
        std::string peerName, std::string message, std::chrono::milliseconds timeout)
        = 0;

    virtual std::future<std::string> sendRequest(
        std::string peerName, Message message, std::chrono::milliseconds timeout)
        = 0;

    virtual void setMessageHandler(std::function<std::string(const std::string&)> handler) = 0;
};
}