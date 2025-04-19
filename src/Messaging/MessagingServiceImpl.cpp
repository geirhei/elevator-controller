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
                std::cout << std::format(
                    "Messaging service {} stopped gracefully ({} handlers executed)", m_peerName, numHandlersExecuted)
                          << std::endl;
        } catch (const std::exception& e) {
            std::cerr << std::format("Unexpected error in run thread: {}", e.what()) << std::endl;
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

std::future<std::string> MessagingServiceImpl::sendRequest(
    std::string peerName, std::string request, std::chrono::milliseconds timeout)
{
    Message messageToSend {
        .uuid = {},
        .payload = { std::move(request) },
    };
    return sendRequest(std::move(peerName), std::move(messageToSend), std::move(timeout));
}

std::future<std::string> MessagingServiceImpl::sendRequest(
    std::string peerName, Message request, std::chrono::milliseconds timeout)
{
    return co_spawn(
        m_ioContext, asyncSendReceive(std::move(peerName), std::move(request), std::move(timeout)), asio::use_future);
}

void MessagingServiceImpl::setMessageHandler(std::function<std::string(const std::string&)> messageHandler)
{
    m_messageHandler = messageHandler;
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

    if (event.type == Event::EventType::Whisper) {
        zmsg_t* zmsg = zyre_event_msg(zyreEvent);
        zframe_t* currentFrame = zmsg_first(zmsg);

        auto& [_, payload] = event.message.emplace(zframe_strdup(currentFrame), std::vector<Message::Frame> {});

        while ((currentFrame = zmsg_next(zmsg)) != nullptr)
            payload.emplace_back(reinterpret_cast<char*>(zframe_data(currentFrame)), zframe_size(currentFrame));
    }

    zyre_event_destroy(&zyreEvent);
    co_return std::move(event);
}

asio::awaitable<void> MessagingServiceImpl::handleEvents()
{
    if (const int rc = zyre_start(m_node); rc != 0)
        throw MessagingError { std::format("Failed to start zyre node {}", zyre_name(m_node)) };

    for (;;) {
        switch (auto [type, peerName, peerUuid, message] = co_await asyncReceiveEvent(); type) {
        case Event::EventType::Enter:
            m_nameIdMap[peerName] = peerUuid;
            break;
        case Event::EventType::Exit:
        case Event::EventType::Stop:
            m_nameIdMap.erase(peerName);
            break;
        case Event::EventType::Whisper:
            if (m_requestChannels.contains(message->uuid))
                handleResponse(std::move(*message));
            else
                handleRequest(peerUuid, std::move(*message));
            break;
        default:
            break;
        }
    }
}

asio::awaitable<std::string> MessagingServiceImpl::asyncSendReceive(
    std::string peerName, Message request, std::chrono::milliseconds timeout)
{
    const auto peerUuidIter = m_nameIdMap.find(peerName);
    if (peerUuidIter == m_nameIdMap.end())
        throw MessagingError { std::format("Failed to send request: Unknown peer name {}", peerName) };

    if (request.payload.size() > 1)
        throw MessagingError { std::format(
            "Failed to send request: Only one payload is allowed in a message, got {}", request.payload.size()) };

    if (request.uuid.empty())
        request.uuid = generateUuid();

    const auto [requestChannelEntry, _] = m_requestChannels.emplace(request.uuid, m_ioContext);

    sendMessage(peerUuidIter->second, std::move(request));

    const std::variant<std::string, std::monostate> result
        = co_await (waitForResponse(requestChannelEntry->second) || waitForTimeout(timeout));

    requestChannelEntry->second.close();
    m_requestChannels.erase(requestChannelEntry);

    // If the result contains a value, we return the response data. Otherwise, we have timed out.
    if (result.index() == 0)
        co_return std::get<0>(result);

    throw RequestTimedOutError { std::format("Request timed out after {}", timeout) };
}

asio::awaitable<std::string> MessagingServiceImpl::waitForResponse(ResponseChannel& channel)
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
    zmsg_t* msg = zmsg_new();

    addStringToMessage(msg, std::move(message.uuid));
    for (auto& frame : std::move(message.payload))
        addStringToMessage(msg, std::move(frame));

    if (const int rc = zyre_whisper(m_node, peerUuid.c_str(), &msg); rc != 0)
        throw MessagingError { std::format("Failed to send message to peer UUID {}", peerUuid) };
}

void MessagingServiceImpl::handleResponse(Message response)
{
    if (const auto sendOk
        = m_requestChannels.at(response.uuid).try_send(system::error_code {}, std::move(response.payload.front()));
        !sendOk)
        throw MessagingError { "Failed to put response into channel" };
}

void MessagingServiceImpl::handleRequest(const std::string& peerUuid, Message requestAndResponse) const
{
    if (!m_messageHandler) {
        std::cout << "No message handler set, dropping request" << std::endl;
        return;
    }

    std::string requestPayload = std::move(requestAndResponse.payload.front());
    std::string responsePayload = m_messageHandler(std::move(requestPayload));

    requestAndResponse.payload = std::vector { std::move(responsePayload) };
    sendMessage(peerUuid, std::move(requestAndResponse));
}

Message::Uuid MessagingServiceImpl::generateUuid()
{
    zuuid_t* zuuid = zuuid_new();
    const Message::Uuid uuid { zuuid_str(zuuid) };
    zuuid_destroy(&zuuid);
    return uuid;
}