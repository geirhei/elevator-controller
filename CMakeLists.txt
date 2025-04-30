cmake_minimum_required(VERSION 3.30)
project(elevator-controller LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)

include(cmake/CPM.cmake)

# Windows 11
add_definitions(-D_WIN32_WINNT=0x0A00)

CPMAddPackage(
        NAME Boost
        VERSION 1.87.0
        GITHUB_REPOSITORY "boostorg/boost"
        GIT_TAG "boost-1.87.0"
)

## https://github.com/chriskohlhoff/asio/issues/1590
if (MINGW)
    target_link_libraries(asio INTERFACE ws2_32)
endif()

CPMAddPackage("gh:google/googletest@1.13.0")

find_package(zyre REQUIRED)

set(ELEVATOR_CONTROLLER_BUILD_TESTS ON)

if (ELEVATOR_CONTROLLER_BUILD_TESTS)
    include(GoogleTest)
    enable_testing()
endif ()

include_directories("${CMAKE_SOURCE_DIR}/src")

add_subdirectory(src)