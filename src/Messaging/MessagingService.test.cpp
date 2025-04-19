#include "MessagingService.hpp"

#include "MessagingServiceImpl.hpp"

#include <zyre.h>

#include <boost/asio.hpp>
#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <iostream>
#include <memory>
#include <string>

using namespace Messaging;
using namespace boost;
using namespace std::chrono_literals;

class MessagingServiceTest : public ::testing::Test {
protected:
    static constexpr auto serviceStartupTime = 100ms;
    static constexpr auto defaultTimeout = 5s;

    std::unique_ptr<MessagingService> messagingService1 = std::make_unique<MessagingServiceImpl>("node1", true);
    std::unique_ptr<MessagingService> messagingService2 = std::make_unique<MessagingServiceImpl>("node2", true);

    static void waitForInitialization() { std::this_thread::sleep_for(serviceStartupTime); }
};

TEST_F(MessagingServiceTest, sendRequestStringAndReceiveResponse)
{
    const std::string node1Response { "node1Response" };
    const std::string node2Response { "node2Response" };
    messagingService1->setMessageHandler([&node1Response](const std::string&) -> std::string { return node1Response; });
    messagingService2->setMessageHandler([&node2Response](const std::string&) -> std::string { return node2Response; });

    messagingService1->run();
    messagingService2->run();
    waitForInitialization();

    std::future<std::string> responseFromNode2
        = messagingService1->sendRequest(messagingService2->peerName(), "Foo", defaultTimeout);
    std::future<std::string> responseFromNode1
        = messagingService2->sendRequest(messagingService1->peerName(), "Bar", defaultTimeout);

    EXPECT_EQ(responseFromNode2.get(), node2Response);
    EXPECT_EQ(responseFromNode1.get(), node1Response);
}

TEST_F(MessagingServiceTest, sendRequestMessageAndReceiveResponse)
{
    const std::string responseStr { "Bar" };
    messagingService2->setMessageHandler([&](const std::string&) -> std::string { return responseStr; });

    messagingService1->run();
    messagingService2->run();
    waitForInitialization();

    std::future<std::string> response
        = messagingService1->sendRequest(messagingService2->peerName(), "Foo", defaultTimeout);
    EXPECT_EQ(response.get(), responseStr);
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
    EXPECT_THROW(messagingService1
                     ->sendRequest(messagingService2->peerName(), Message { .uuid = {}, .payload = { "Foo", "Bar" } },
                         defaultTimeout)
                     .get(),
        MessagingError);
}