#pragma once

#include <chrono>
#include <functional>
#include <future>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace Messaging {

/**
 * Represents a message that can be sent or received through the messaging service.
 * A message consists of a unique identifier (UUID) and a payload containing one or more frames.
 */
class Message {
public:
    using Uuid = std::string;
    using Frame = std::string;
    using Payload = std::vector<Frame>;

    /**
     * Creates a new empty message with an automatically generated UUID.
     */
    Message();

    /**
     * Creates a new message with specified payload and an automatically generated UUID.
     *
     * @param payload The data frames to include in the message.
     */
    explicit Message(Payload payload);

    /**
     * Creates a new message with specified UUID and payload.
     *
     * @param uuid Unique identifier for the message.
     * @param payload The data frames to include in the message.
     */
    Message(Uuid uuid, Payload payload);

    [[nodiscard]] Uuid uuid() const;
    [[nodiscard]] const Payload& payload() const;
    [[nodiscard]] Payload& payload();

    /**
     * Generates a new unique identifier suitable for message identification.
     *
     * @return A new unique identifier string.
     */
    [[nodiscard]] static Uuid generateUuid();

private:
    Uuid m_uuid;
    Payload m_payload;
};

/**
 * Represents events that occur within the messaging network.
 * Events include peer connection/disconnection, group membership changes,
 * and message transmission events.
 */
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
    std::optional<std::string> groupName;
    std::optional<Message> message;
};

/**
 * Base exception class for all messaging-related errors.
 */
class MessagingError : public std::runtime_error {
    using std::runtime_error::runtime_error;
};

/**
 * Exception thrown when a request does not receive a response within the specified timeout period.
 */
class RequestTimedOutError : public MessagingError {
    using MessagingError::MessagingError;
};

/**
 * Defines the messaging service interface for peer-to-peer communication.
 */
class MessagingService {
public:
    using RequestHandler = std::function<Message(Message)>;
    using SubscriptionHandler = std::function<void(Message)>;

    virtual ~MessagingService() = default;

    /**
     * Initializes and starts the messaging service.
     * This method should be called before any other operations.
     */
    virtual void run() = 0;

    /**
     * Returns the unique name of this peer in the messaging network.
     *
     * @return The name of this peer as a string.
     */
    [[nodiscard]] virtual std::string peerName() const = 0;

    /**
     * Sends a request with a simple string payload to a specified peer and waits for a response.
     *
     * @param peerName The name of the target peer to send the request to.
     * @param message A string message to send as the request payload.
     * @param timeout Maximum time to wait for a response.
     * @return A future that will contain the response message when completed.
     * @throws MessagingError If the peer is unknown or the request could not be sent.
     * @throws RequestTimedOutError If no response is received within the timeout period.
     */
    virtual std::future<Message> sendRequest(
        std::string peerName, std::string message, std::chrono::milliseconds timeout)
        = 0;

    /**
     * Sends a structured message request to a specified peer and waits for a response.
     *
     * @param peerName The name of the target peer to send the request to.
     * @param message A structured Message object containing the request payload.
     * @param timeout Maximum time to wait for a response.
     * @return A future that will contain the response message when completed.
     * @throws MessagingError If the peer is unknown or the request could not be sent.
     * @throws RequestTimedOutError If no response is received within the timeout period.
     */
    virtual std::future<Message> sendRequest(std::string peerName, Message message, std::chrono::milliseconds timeout)
        = 0;

    /**
     * Sends a message to a topic that none or more other peers may be subscribed to.
     * Provides no guarantee of delivery or acknowledgment.
     *
     * @param topic The topic name to publish to.
     * @param message The message to publish.
     */
    virtual void publish(std::string topic, Message message) = 0;

    /**
     * Sets the handler function for processing incoming requests from other peers.
     * Only one handler can be active at a time; setting a new handler replaces any existing one.
     * The caller must ensure the handler itself is thread-safe
     *
     * @param handler The function to call when requests are received.
     *        The handler receives the request Message and must return a response Message.
     */
    virtual void setRequestHandler(RequestHandler handler) = 0;

    /**
     * Subscribes to a topic to receive messages published to it.
     * Multiple topics can be subscribed to, each with its own handler.
     * The caller must ensure the handler itself is thread-safe
     *
     * @param topic The topic name to subscribe to.
     * @param handler The function to call when messages are received on this topic.
     */
    virtual void subscribeToTopic(std::string topic, SubscriptionHandler handler) = 0;
};
}