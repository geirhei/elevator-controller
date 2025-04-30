# Elevator Controller

This project implements a control system for controlling `n` elevators working in parallel to service `m` floors. It is based mainly on the description for the assignment in [this](https://github.com/TTK4145/Project) course.

## Messaging service
A large part of the assignment is ensuring that we have reliable communication between the elevators. We have TCP/IP and UDP via ethernet available. In order to achieve this, we implement a messaging service layer that can provides us with the guarantees that we need for message flow.

The project incorporates a robust messaging service for inter-component communication. This service acts as a higher-level C++ wrapper around the [Zyre library](https://github.com/zeromq/zyre).

Zyre itself provides a framework for building reliable peer-to-peer applications on local networks using ZeroMQ. It manages peer discovery via UDP beacons, maintains group memberships, and handles the underlying ZeroMQ socket communication for direct messages ('whispers') and group broadcasts ('shouts'). In addition to zyre's base functionality, the MessagingService API provides reliable request-reply communication, and keeps track of the named peers that have been seen in the network.

The implementation provided here wraps an ASIO execution context, which performs all the work. All operations are posted for asynchronous completion in the contained execution context. Since this is run by a single thread only, the API is inherently thread-safe. The caller is however responsible for any synchronization required as a part of the callbacks set for handling incoming messages.
