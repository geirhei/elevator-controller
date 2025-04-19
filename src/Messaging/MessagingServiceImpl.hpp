#pragma once

#ifdef _MSC_VER
#include <cstdlib>
namespace std {
using ::_strtoui64;
}
#endif

#include "MessagingService.hpp"

#include <zyre.h>

#include <boost/asio.hpp>
#include <boost/asio/experimental/concurrent_channel.hpp>

#include <array>
#include <chrono>
#include <functional>
#include <future>
#include <ranges>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>

namespace Messaging {

class MessagingServiceImpl : public MessagingService {
public:
    explicit MessagingServiceImpl(std::string_view name, bool verbose = false);
    ~MessagingServiceImpl() noexcept override;
    MessagingServiceImpl(const MessagingServiceImpl&) = delete;
    MessagingServiceImpl& operator=(const MessagingServiceImpl&) = delete;

    void run() override;
    [[nodiscard]] std::string peerName() const override;
    std::future<std::string> sendRequest(
        std::string peerName, std::string request, std::chrono::milliseconds timeout) override;
    std::future<std::string> sendRequest(
        std::string peerName, Message request, std::chrono::milliseconds timeout) override;
    void setMessageHandler(std::function<std::string(const std::string&)> messageHandler) override;

private:
    using ResponseChannel = boost::asio::experimental::concurrent_channel<void(boost::system::error_code, std::string)>;

    template <boost::asio::completion_token_for<void(boost::system::error_code, zyre_event_t*)> CompletionToken>
    auto async_zyre_event_new(zyre_t* node, CompletionToken&&);

    boost::asio::awaitable<void> handleEvents();
    [[nodiscard]] boost::asio::awaitable<Event> asyncReceiveEvent();
    [[nodiscard]] boost::asio::awaitable<std::string> asyncSendReceive(
        std::string peerName, Message request, std::chrono::milliseconds timeout);

    void sendMessage(std::string peerUuid, Message) const;
    void handleResponse(Message response);
    void handleRequest(const std::string& peerUuid, Message requestAndResponse) const;

    static boost::asio::awaitable<std::string> waitForResponse(ResponseChannel&);
    static boost::asio::awaitable<void> waitForTimeout(std::chrono::milliseconds);
    static void addStringToMessage(zmsg_t*, std::string_view);
    [[nodiscard]] static Message::Uuid generateUuid();

    boost::asio::io_context m_ioContext;
    boost::asio::executor_work_guard<boost::asio::io_context::executor_type> m_workGuard;
    boost::asio::cancellation_signal m_cancellationSignal;

    std::thread m_workerThread;
    std::string m_peerName;
    zyre_t* m_node;
    bool m_verbose;
    std::unordered_map<Message::Uuid, ResponseChannel> m_requestChannels;
    std::unordered_map<std::string, std::string> m_nameIdMap;
    std::function<std::string(const std::string&)> m_messageHandler;

    static constexpr auto eventStringToEventType(std::string_view eventString)
    {
        constexpr std::array<std::pair<std::string_view, Event::EventType>, 9> eventTypeMappings { {
            { "ENTER", Event::EventType::Enter },
            { "EVASIVE", Event::EventType::Evasive },
            { "EXIT", Event::EventType::Exit },
            { "JOIN", Event::EventType::Join },
            { "LEAVE", Event::EventType::Leave },
            { "SHOUT", Event::EventType::Shout },
            { "SILENT", Event::EventType::Silent },
            { "STOP", Event::EventType::Stop },
            { "WHISPER", Event::EventType::Whisper },
        } };

        if (const auto eventTypeEntry
            = std::ranges::find(eventTypeMappings, eventString, &std::pair<std::string_view, Event::EventType>::first);
            eventTypeEntry != std::ranges::end(eventTypeMappings))
            return eventTypeEntry->second;
        throw MessagingError { std::format("Unknown event type: {}", eventString) };
    }
};

/**
 * This function creates a new event in a non-blocking way by wrapping the blocking zyre_event_new in an ASIO
 * asynchronous operation. The caller must destroy the event when finished with it.
 *
 * It is based on the following example:
 * https://live.boost.org/doc/libs/1_88_0/doc/html/boost_asio/example/cpp20/operations/callback_wrapper.cpp
 */
template <boost::asio::completion_token_for<void(boost::system::error_code, zyre_event_t*)> CompletionToken>
auto MessagingServiceImpl::async_zyre_event_new(zyre_t* node, CompletionToken&& token)
{
    auto init
        = [node](boost::asio::completion_handler_for<void(boost::system::error_code, zyre_event_t*)> auto handler) {
              auto work = boost::asio::make_work_guard(handler);

              std::thread { [handler = std::move(handler), work = std::move(work), node]() mutable {
                  // Blocking call
                  zyre_event_t* event = zyre_event_new(node);

                  auto alloc = boost::asio::get_associated_allocator(handler, boost::asio::recycling_allocator<void>());
                  boost::asio::post(work.get_executor(),
                      boost::asio::bind_allocator(alloc, [handler = std::move(handler), event]() mutable {
                          std::move(handler)(boost::system::error_code {}, event);
                      }));
              } }.detach();
          };

    return boost::asio::async_initiate<CompletionToken, void(boost::system::error_code, zyre_event_t*)>(init, token);
}
}
