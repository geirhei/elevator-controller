#include "MessagingService.hpp"

#include "MessagingServiceImpl.hpp"

#include <zyre.h>

#include <boost/asio.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <chrono>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <string>
#include <utility>

using namespace Messaging;
using namespace boost;
using namespace std::chrono_literals;

static constexpr inline auto MessagePayloadEq = [](const Message::Payload& expectedPayload) {
    return ::testing::Property("payload", &Message::payload, ::testing::ContainerEq(expectedPayload));
};

class MessagingServiceTest : public ::testing::Test {
protected:
    static constexpr auto serviceStartupTime = 100ms;
    static constexpr auto defaultTimeout = 5s;

    std::unique_ptr<MessagingService> messagingService1 = std::make_unique<MessagingServiceImpl>("node1", true);
    std::unique_ptr<MessagingService> messagingService2 = std::make_unique<MessagingServiceImpl>("node2", true);

    static void waitForInitialization() { std::this_thread::sleep_for(serviceStartupTime); }

    static std::function<Message(Message)> requestHandlerAdapter(std::string response)
    {
        return [response = std::move(response)](Message message) -> Message {
            message.payload().clear();
            message.payload().emplace_back(response);
            return std::move(message);
        };
    }

    static std::function<void()> setPromisedValue(auto& promise)
    {
        return [&promise]() -> void { promise.set_value(); };
    }
};

TEST_F(MessagingServiceTest, sendRequestStringAndReceiveResponse)
{
    const std::string node1Response { "node1Response" };
    const std::string node2Response { "node2Response" };

    messagingService1->setRequestHandler(requestHandlerAdapter(node1Response));
    messagingService2->setRequestHandler(requestHandlerAdapter(node2Response));

    messagingService1->run();
    messagingService2->run();
    waitForInitialization();

    std::future<Message> responseFromNode2
        = messagingService1->sendRequest(messagingService2->peerName(), "Foo", defaultTimeout);
    std::future<Message> responseFromNode1
        = messagingService2->sendRequest(messagingService1->peerName(), "Bar", defaultTimeout);

    EXPECT_THAT(responseFromNode2.get(), MessagePayloadEq({ node2Response }));
    EXPECT_THAT(responseFromNode1.get(), MessagePayloadEq({ node1Response }));
}

TEST_F(MessagingServiceTest, sendRequestMessageAndReceiveResponse)
{
    const std::string responseStr { "Bar" };
    messagingService2->setRequestHandler(requestHandlerAdapter(responseStr));

    messagingService1->run();
    messagingService2->run();
    waitForInitialization();

    std::future<Message> response
        = messagingService1->sendRequest(messagingService2->peerName(), "Foo", defaultTimeout);
    EXPECT_THAT(response.get(), MessagePayloadEq({ responseStr }));
}

TEST_F(MessagingServiceTest, sendRequestThrowOnTimeout)
{
    messagingService1->run();
    messagingService2->run();
    waitForInitialization();
    EXPECT_THROW(messagingService1->sendRequest(messagingService2->peerName(), "Foo", 1ms).get(), RequestTimedOutError);
}

TEST_F(MessagingServiceTest, sendRequestThrowWhenUnknownPeerName)
{
    messagingService1->run();
    waitForInitialization();
    EXPECT_THROW(
        messagingService1->sendRequest(messagingService2->peerName(), "Foo", defaultTimeout).get(), MessagingError);
}

TEST_F(MessagingServiceTest, sendRequestThrowOnMoreThanOneFrameInPayload)
{
    messagingService1->run();
    messagingService2->run();
    waitForInitialization();
    EXPECT_THROW(
        messagingService1->sendRequest(messagingService2->peerName(), Message { { "Foo", "Bar" } }, defaultTimeout)
            .get(),
        MessagingError);
}

TEST_F(MessagingServiceTest, publishAndReceiveByOtherPeers)
{
    const std::string topic1 { "topic1" };
    const std::string topic2 { "topic2" };

    const Message::Payload payload1 { "Foo" };
    const Message::Payload payload2 { "Bar" };

    std::unique_ptr<MessagingService> messagingService3 = std::make_unique<MessagingServiceImpl>("node3", true);

    ::testing::MockFunction<MessagingService::SubscriptionHandler> subscriptionHandlerMock1;
    ::testing::MockFunction<MessagingService::SubscriptionHandler> subscriptionHandlerMock2;

    std::promise<void> promise1;
    std::future<void> future1 = promise1.get_future();
    std::promise<void> promise2;
    std::future<void> future2 = promise2.get_future();

    EXPECT_CALL(subscriptionHandlerMock1, Call(MessagePayloadEq(payload1))).WillOnce(setPromisedValue(promise1));
    EXPECT_CALL(subscriptionHandlerMock2, Call(MessagePayloadEq(payload2))).WillOnce(setPromisedValue(promise2));

    messagingService1->subscribeToTopic(topic1, subscriptionHandlerMock1.AsStdFunction());
    messagingService2->subscribeToTopic(topic2, subscriptionHandlerMock2.AsStdFunction());

    messagingService1->run();
    messagingService2->run();
    messagingService3->run();
    waitForInitialization();

    messagingService3->publish(topic1, Message { payload1 });
    messagingService3->publish(topic2, Message { payload2 });

    EXPECT_EQ(std::future_status::ready, future1.wait_for(defaultTimeout)) << "Timeout waiting for topic1 message";
    EXPECT_EQ(std::future_status::ready, future2.wait_for(defaultTimeout)) << "Timeout waiting for topic2 message";
}