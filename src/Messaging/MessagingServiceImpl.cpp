#include "MessagingServiceImpl.hpp"

#include <zyre.h>

#include <boost/asio.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/asio/experimental/concurrent_channel.hpp>

#include <chrono>
#include <format>
#include <functional>
#include <future>
#include <iostream>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

using namespace Messaging;
using namespace boost;
using namespace asio::experimental::awaitable_operators;

MessagingServiceImpl::MessagingServiceImpl(const std::string_view name, const bool verbose)
    : m_workGuard { asio::make_work_guard(m_ioContext) }
    , m_peerName { name }
    , m_node { zyre_new(name.data()) }
    , m_verbose { verbose }
{
    if (!m_node)
        throw MessagingError { std::format("Failed to create zyre node {}", name) };
    if (m_verbose)
        zyre_set_verbose(m_node);

    m_workerThread = std::thread { [this] {
        try {
            const auto numHandlersExecuted = m_ioContext.run();
            if (m_verbose)
                std::println(std::cout, "Messaging service {} stopped gracefully ({} handlers executed)", m_peerName,
                    numHandlersExecuted);
        } catch (const std::exception& e) {
            std::println(std::cerr, "Unexpected error in run thread: {}", e.what());
        }
    } };
}

MessagingServiceImpl::~MessagingServiceImpl()
{
    m_cancellationSignal.emit(asio::cancellation_type::all);
    post(m_ioContext, [this] {
        if (m_node)
            zyre_stop(m_node);
        m_workGuard.reset();
    });
    if (m_workerThread.joinable())
        m_workerThread.join();
    if (m_node)
        zyre_destroy(&m_node);
}

void MessagingServiceImpl::run()
{
    co_spawn(m_ioContext, handleEvents(), asio::bind_cancellation_slot(m_cancellationSignal.slot(), asio::detached));
}

std::string MessagingServiceImpl::peerName() const
{
    return m_peerName;
}

std::future<Message> MessagingServiceImpl::sendRequest(
    std::string peerName, std::string request, std::chrono::milliseconds timeout)
{
    return sendRequest(std::move(peerName), Message { { std::move(request) } }, timeout);
}

std::future<Message> MessagingServiceImpl::sendRequest(
    std::string peerName, Message request, std::chrono::milliseconds timeout)
{
    return co_spawn(
        m_ioContext, asyncSendReceive(std::move(peerName), std::move(request), std::move(timeout)), asio::use_future);
}

void MessagingServiceImpl::publish(std::string topic, Message message)
{
    co_spawn(m_ioContext, asyncPublish(std::move(topic), std::move(message)), asio::detached);
}

void MessagingServiceImpl::subscribeToTopic(std::string topic, SubscriptionHandler handler)
{
    post(m_ioContext, [this, topic = std::move(topic), handler = std::move(handler)]() mutable {
        const auto& [topicEntry, _] = m_subscriptionHandlers.insert_or_assign(std::move(topic), std::move(handler));
        zyre_join(m_node, topicEntry->first.c_str());
    });
}

void MessagingServiceImpl::setRequestHandler(const RequestHandler requestHandler)
{
    m_requestHandler = requestHandler;
}

asio::awaitable<Event> MessagingServiceImpl::asyncReceiveEvent()
{
    zyre_event_t* zyreEvent = co_await async_zyre_event_new(m_node, asio::use_awaitable);
    if (!zyreEvent)
        throw system::system_error { asio::error::operation_aborted };

    if (m_verbose)
        zyre_event_print(zyreEvent);

    Event event {
        .type = eventStringToEventType(zyre_event_type(zyreEvent)),
        .peerName { zyre_event_peer_name(zyreEvent) },
        .peerUuid { zyre_event_peer_uuid(zyreEvent) },
    };

    if (event.type == Event::EventType::Whisper || event.type == Event::EventType::Shout) {
        if (event.type == Event::EventType::Shout)
            event.groupName.emplace(zyre_event_group(zyreEvent));

        zmsg_t* zyreMessage = zyre_event_msg(zyreEvent);
        zframe_t* currentFrame = zmsg_first(zyreMessage);

        Message::Uuid uuid { zframe_strdup(currentFrame) };

        Message::Payload payload {};
        while ((currentFrame = zmsg_next(zyreMessage)) != nullptr)
            payload.emplace_back(reinterpret_cast<char*>(zframe_data(currentFrame)), zframe_size(currentFrame));

        event.message.emplace(std::move(uuid), std::move(payload));
    }

    zyre_event_destroy(&zyreEvent);
    co_return std::move(event);
}

asio::awaitable<void> MessagingServiceImpl::handleEvents()
{
    if (const int rc = zyre_start(m_node); rc != 0)
        throw MessagingError { std::format("Failed to start zyre node {}", zyre_name(m_node)) };

    for (;;) {
        switch (auto [type, peerName, peerUuid, groupName, message] = co_await asyncReceiveEvent(); type) {
        case Event::EventType::Enter:
            m_peersAvailable.insert_or_assign(peerName, std::move(peerUuid));
            break;
        case Event::EventType::Exit:
        case Event::EventType::Stop:
            m_peersAvailable.erase(peerName);
            break;
        case Event::EventType::Shout:
            if (auto handlerEntry = m_subscriptionHandlers.find(*groupName);
                handlerEntry != m_subscriptionHandlers.end())
                handlerEntry->second(std::move(*message));
            break;
        case Event::EventType::Whisper:
            if (m_pendingResponseChannels.contains(message->uuid()))
                handleResponse(std::move(*message));
            else
                handleRequest(peerUuid, std::move(*message));
            break;
        default:
            break;
        }
    }
}

asio::awaitable<Message> MessagingServiceImpl::asyncSendReceive(
    std::string peerName, Message request, std::chrono::milliseconds timeout)
{
    const auto peerUuidIter = m_peersAvailable.find(peerName);
    if (peerUuidIter == m_peersAvailable.end())
        throw MessagingError { std::format("Failed to send request: Unknown peer name {}", peerName) };

    if (request.payload().size() > 1)
        throw MessagingError { std::format(
            "Failed to send request: Only one payload is allowed in a message, got {}", request.payload().size()) };

    const auto [responseChannelEntry, _] = m_pendingResponseChannels.emplace(request.uuid(), m_ioContext);

    sendMessage(peerUuidIter->second, std::move(request));

    const std::variant<Message, std::monostate> result
        = co_await (waitForResponse(responseChannelEntry->second) || waitForTimeout(timeout));

    responseChannelEntry->second.close();
    m_pendingResponseChannels.erase(responseChannelEntry);

    // If the result contains a message, we return it. Otherwise, we have timed out.
    if (result.index() == 0)
        co_return std::get<0>(result);

    throw RequestTimedOutError { std::format("Request timed out after {}", timeout) };
}

asio::awaitable<void> MessagingServiceImpl::asyncPublish(std::string topic, Message message)
{
    zmsg_t* zyreMessage = zmsg_new();

    try {
        addStringToMessage(zyreMessage, std::move(message.uuid()));
        for (auto& frame : std::move(message.payload()))
            addStringToMessage(zyreMessage, std::move(frame));
    } catch (const MessagingError& e) {
        std::println(std::cerr, "Failed to create message for publishing to topic {}: {}", topic, e.what());
        if (zyreMessage != nullptr)
            zmsg_destroy(&zyreMessage);
        co_return;
    }

    if (const int rc = zyre_shout(m_node, topic.c_str(), &zyreMessage); rc != 0)
        std::println(std::cerr, "Failed to publish message to topic {}", topic);
    co_return;
}

asio::awaitable<Message> MessagingServiceImpl::waitForResponse(ResponseChannel& channel)
{
    co_return co_await channel.async_receive(asio::use_awaitable);
}

asio::awaitable<void> MessagingServiceImpl::waitForTimeout(std::chrono::milliseconds timeout)
{
    co_await asio::steady_timer { co_await asio::this_coro::executor, timeout }.async_wait(asio::use_awaitable);
}

void MessagingServiceImpl::addStringToMessage(zmsg_t* msg, std::string_view stringToAdd)
{
    if (zmsg_addstr(msg, stringToAdd.data()) != 0)
        throw MessagingError { std::format("Failed to add string {} to message", stringToAdd) };
}

void MessagingServiceImpl::sendMessage(std::string peerUuid, Message message) const
{
    zmsg_t* zyreMessage = zmsg_new();

    addStringToMessage(zyreMessage, std::move(message.uuid()));
    for (auto& frame : std::move(message.payload()))
        addStringToMessage(zyreMessage, std::move(frame));

    if (const int rc = zyre_whisper(m_node, peerUuid.c_str(), &zyreMessage); rc != 0)
        throw MessagingError { std::format("Failed to send message to peer UUID {}", peerUuid) };
}

void MessagingServiceImpl::handleResponse(Message response)
{
    if (const auto sendOk
        = m_pendingResponseChannels.at(response.uuid()).try_send(system::error_code {}, std::move(response));
        !sendOk)
        throw MessagingError { "Failed to put response into channel" };
}

void MessagingServiceImpl::handleRequest(const std::string& peerUuid, Message request) const
{
    if (!m_requestHandler) {
        std::println(std::cout, "No message handler set, dropping request");
        return;
    }
    Message response = m_requestHandler(std::move(request));
    sendMessage(peerUuid, std::move(response));
}
