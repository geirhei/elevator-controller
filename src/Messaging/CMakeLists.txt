add_library(messaging MessagingServiceImpl.cpp)
target_link_libraries(messaging PUBLIC Boost::asio zyre)

if (ELEVATOR_CONTROLLER_BUILD_TESTS)
    add_executable(messaging-test MessagingService.test.cpp)
    target_link_libraries(messaging-test PRIVATE messaging gtest gtest_main Boost::asio)
    gtest_discover_tests(messaging-test)
endif ()