# CoroKafka library [![Build Status](https://travis-ci.com/bloomberg/corokafka.svg?branch=master)](https://travis-ci.com/bloomberg/corokafka)

Coroutine-based Kafka messaging library! **CoroKafka** is a scalable and simple to use C++ library built on top of [cppkafka](https://github.com/mfontanini/cppkafka) and [quantum](https://github.com/bloomberg/quantum), supporting any number of parallel producers and consumers.
Producing and consuming of messages is simplified by allowing applications to use their own native message formats which are automatically serialized and de-serialized by the connector. Message processing spreads to optimal number of parallel coroutines and threads. Integrated support for **RdKafka** headers. Currently the library only supports static topics and admin API is not yet available. 

Supports C++14 and later.

## Menu

- [Rationale](#rationale)
    - [Salient Features](#salient-features)
- [Quick start](#quick-start)
    - [Serializers and Deserializers](#serializers-and-de-serializers)
    - [Producer Example](#producer-example)
    - [Consumer Example](#consumer-example)
- [Building](#building)
    - [CMake options](#cmake-options)
- [Installation](#installation)
- [Linking and Using](#linking-and-using)
- [Contributions](#contributions)
- [License](#license)
- [Code of Conduct](#code-of-conduct)
- [Security Vulnerability Reporting](#security-vulnerability-reporting)

## Rationale

**RdKafka** is a popular high-performance C-based Kafka library, but using it requires a lot of extra code to be written which often leads to unwieldy bugs, misinterpreted API usage, and a lengthy development cycle. **CppKafka** was written with this in mind and provides a clean C++ raw-buffer wrapper on top of **RdKafka** with integrated object lifetime management as well as introducing various powerful utilities for producing and consuming, such as the `BufferedProducer` and the `BackoffCommitter`. Despite all this, **CppKafka** remains a thin library on top of **RdKafka** and the current library, **CoroKafka** extends its functionality into a rich-featured API.
Producing and consuming messages in a multi-threaded environment can easily be enhanced by using _coroutines_, which makes this a great integration scenario for Bloomberg's **Quantum** library.

**CoroKafka** offers a multitude of [configuration options](https://github.com/bloomberg/corokafka/blob/master/CONFIGURATION.md) besides those [offered](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md) natively by **RdKafka**. **CoroKafka** does not pretend to be the fastest or best performing Kafka implementation, its main goal being ease-of-use, convenience, as well as abstracting lots of boiler-plate code which all applications must write.

### Salient Features

* Application publishes and consumes using native types. Serialization and deserialization is handled internally by **CoroKafka** and provides strict compile-time type-safety on both ends.
* Full support for any number  of **RdKafka** headers for all message types.
* All **RdKafka** callbacks are supported as well as additional ones as provided by **CppKafka**.
* Producer and consumer polling is handled internally, so applications need not worry about blocking or missing heartbeats.
* Support for message retries, buffering, strict order guarantee delivery for all producers.
* Support for commit retries, auto-commit management as well as batching and pre-fetching of messages for increased performance. 
* Support for balanced round-robin partition reading.
* Serialization and de-serialization is highly parallelized on coroutines. Various configuration options allow tweaking this behavior and how messages are delivered to the end application.
* Support for throttling consumers and producers as well as pausing and resuming streams.
* Consumers can either use dynamic partition assignment or static via config files. 
* Processing received messages on the `quantum::Sequencer`, can easily create thousands of parallel streams of data yielding tremendous performance benefits.
* Pre-processing of messages prior to de-serializing can allow for early poison-message detection.
* **CoroKafka's** `OffsetManager` allows proper ordering of commits for parallel streams, preventing higher offsets from being committed before lower ones on the same partition.
* Synchronous and asynchronous producers.

## Quick Start

The following code snippet shows how to setup a basic consumer and producer. 

### Serializers and De-serializers

In **CoroKafka** applications must define serializers and de-serializer for each key, payload and header(s) they wish to send and receive. These specializations must be defined in either the global namespace or the `corokafka` namespace.

The _serializer_ **must** have the following signature: 

```c++
template <typename T>
struct Serialize
{
    std::vector<uint8_t> operator()(const T&);
}
```
Similarly the _de-serializer_ **must** have the following signature:

```c++
template <typename T>
struct Deserialize
{
    T operator()(const cppkafka::TopicPartition&, const cppkafka::Buffer&);
}
```
The first parameter indicates the partition on which this message arrived and can be used for logging, stats, anti-poison message prevention, etc.

```c++
//==========================================================================
//                       Serializers/De-serializers
//==========================================================================
//------------------------ serializers.h -----------------------------------
#include <corokafka/corokafka.h>

// Declare functions inside corokafka namespace...
namespace Bloomberg { namespace corokafka {
    // This serializer is used for the key
    template <>
    struct Serialize<size_t> {
        std::vector<uint8_t> operator()(const size_t& value) {
            return {reinterpret_cast<const uint8_t*>(&value),
                    reinterpret_cast<const uint8_t*>(&value) + sizeof(size_t)};
        }
    };
    
    // This serializer is used for the headers and the payload
    template <>
    struct Serialize<std::string> {
        std::vector<uint8_t> operator()(const std::string& value) {
            return {value.begin(), value.end()};
        }
    };
    
    // This is the deserializer for the key
    template <>
    struct Deserialize<size_t> {
        size_t operator()(const cppkafka::TopicPartition&, 
                          const cppkafka::Buffer& buffer) {
            return *static_cast<const size_t*>((void*)buffer.get_data());
        }
    };
    
    // This is the deserializer for the header and payload
    template <>
    struct Deserialize<std::string> {
        std::string operator()(const cppkafka::TopicPartition&, 
                               const cppkafka::Buffer& buffer) {
            return {buffer.begin(), buffer.end()};
        }
    };
}} //namespace Bloomberg::corokafka

//==========================================================================
//                                Topic
//==========================================================================
//------------------------------ mytopic.h ---------------------------------
#include <serializers.h>

using MyTopic = corokafka::Topic<size_t, std::string, coroakafka::Headers<std::string>>;

// Create a topic which will be shared between producer and consumer.
static MyTopic myTopic("my-topic", corokafka::Header<std::string>("Header1");
```

### Producer example

In the following example we will be producing some messages with a key of type `size_t`, a payload of type
`std::string` and a simple header called _Header1_. For full producer API see [here](https://github.com/bloomberg/corokafka/blob/master/corokafka/corokafka_producer_manager.h).
```c++
//==========================================================================
//                          Producer setup
//==========================================================================
#include <mytopic.h>

// Define a delivery report callback (optional - see documentation for all available callbacks)
void deliveryReportCallback(const corokafka::ProducerMetadata& metadata,
                            const corokafka::SentMessage& msg)
{
    std::cout << "Produced message for [" << metadata.getInternalName()
              << ":" << msg.getTopic() << "]"
              << std::endl;
    if (msg) {
        if (msg.getError()) {
            return;
        }
        //print message contents
        std::cout << "Message key: " << *reinterpret_cast<const size_t*>(msg.getKeyBuffer().get_data())
                  << " Message content: [" << (std::string)msg.getPayloadBuffer() << "]"
                  << " Message partition: " << msg.getPartition()
                  << " Message offset: " << msg.getOffset()
                  << std::endl;
    }
}

// Define a log callback (optional - see documentation for all available callbacks)
void logCallback(const corokafka::Metadata& metadata,
                 cppkafka::LogLevel level,
                 const std::string& facility,
                 const std::string& message)
{
    std::cout << "Log from: " << metadata.getInternalName()
              << " facility: " << facility
              << " text: " << message << std::endl;
}

// Create a topic configuration (optional)
std::initializer_list<cppkafka::ConfigurationOption > topicOptions = {
    { "produce.offset.report",  true },
    { "request.required.acks", -1 }
};

// Create a producer configuration (only 'metadata.broker.list' setting is mandatory)
std::initializer_list<cppkafka::ConfigurationOption > configOptions = {
    { "metadata.broker.list", "broker_url:port" },
    { "api.version.request", true },
    { "internal.producer.retries", 5 }
};

// Associate the topic and producer configuration with a topic name.
// Note: any number of producer configs/topics can be created.
corokafka::ProducerConfiguration config("my-topic", configOptions, topicOptions);

// Add the callbacks
config.setDeliveryReportCallback(deliveryReportCallback);
config.setLogCallback(logCallback);

// Create the connector config (optional)
corokafka::ConnectorConfiguration connectorConfiguration;
connectorConfiguration.setPollInterval(std::chrono::milliseconds(100));

// Combine all the configs together
corokafka::ConfigurationBuilder builder;
builder(config).(connectorConfiguration);

// Create the connector
corokafka::Connector connector(std::move(builder));

// Produce 10 messages
corokafka::ProducerManager& producer = connector.producer(); //get handle on producer

// Produce 10 messages. 'i' represents the 'key'
for (size_t i = 0; i < 10; ++i) { 
    //produce a message synchronously (async production also available)
    producer.send(myTopic, 0, i, std::string("Hello world"), std::string("This is some header"));
}

```

### Consumer example

In the following example we will be consuming messages with a key of type `size_t`, a payload of type
`std::string` and a simple header called _Header1_. For full consumer API see [here](https://github.com/bloomberg/corokafka/blob/master/corokafka/corokafka_consumer_manager.h).
```
//==========================================================================
//                 Message worker queue and processor
//==========================================================================
#include <mytopic.h>

std::mutex messageMutex;
std::deque<corokafka::ReceivedMessage<size_t, std::string>> messages;

void messageProcessor()
{
    while (1)
    {
        if (!messages.empty()) {
            std::lock_guard<std::mutex> lock(messageMutex);
            corokafka::ReceivedMessage<size_t, std::string> message = std::move(messages.front());
            messages.pop_front();
                       
            //get the headers
            const ckf::HeaderPack &headers = message.getHeaders();
            std::string headerName;
            std::string headerValue;
            if (headers) {
                headerName = headers.getAt<std::string>(0).name();
                //get header value (type unsafe)
                headerValue = headers.getAt<std::string>(0).value();
                //get header value (type-safe)
                //headerValue = message.getHeaderAt<0>();
            }
            std::ostringstream oss;
            oss << "Received message from topic: " << message.getTopic()
                << " partition: " << message.getPartition()
                << " offset: " << message.getOffset()
                << " key: " << message.getKey()
                << " header: " << headerName << " :: " << headerValue
                << " payload: " << message.getPayload()
                << std::endl;
            std::cout << oss.str() << std::endl;
            
            //commit the message. This can also be done automatically (see consumer configuration).
            message.commit();
        }
        // Pause a little. Typically this could be implemented with a condition variable being signalled
        // when a message gets enqueued.
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

//==========================================================================
//                         Consumer setup
//==========================================================================
#include <corokafka/corokafka.h>

// Define a log callback (optional - see documentation for all available callbacks)
void logCallback(const corokafka::Metadata& metadata,
                 cppkafka::LogLevel level,
                 const std::string& facility,
                 const std::string& message)
{
    std::cout << "Log from: " << metadata.getInternalName()
              << " facility: " << facility
              << " text: " << message << std::endl;
}

// Define a receiver callback (mandatory)
void receiverCallback(MyTopic::receivedMessage message)
{
    if (!message) return; //invalid message
     
    if (message.getError()) {
        std::cout << "Enqueued message from topic: " << message.getTopic()
                  << " with error: " << message.getError().to_string()
                  << std::endl;
    }
    else {
        std::cout << "Enqueued message from topic: " << message.getTopic()
                  << " partition: " << message.getPartition()
                  << " offset: " << message.getOffset()
                  << " key: " << message.getKey()
                  << " payload: " << (std::string)message.getPayload()
                  << std::endl;
    }

    // Post message unto a worker queue...
    std::lock_guard<std::mutex> lock(messageMutex);
    messages.emplace_back(std::move(message));
}

// Create the producer and topic config
std::initializer_list<cppkafka::ConfigurationOption > configOptions = {
    { "metadata.broker.list", "broker_url:port" },
    { "group.id", "my-group" },
    { "api.version.request", true },
    { "enable.partition.eof", true },
    { "enable.auto.offset.store", true },
    { "enable.auto.commit", false },
    { "auto.offset.reset", "earliest" },
    { "partition.assignment.strategy", "roundrobin" },
    { "internal.consumer.pause.on.start", false }
};

//create the consumer configuration
corokafka::ConsumerConfiguration config(myTopic, configOptions, {}, receiverCallback);

//add the callbacks
config.setLogCallback(logCallback);

// Optionally set initial partition assignment (4 partitions per topic)
config.assignInitialPartitions(corokafka::PartitionStrategy::Dynamic,
    {
    {topic, 0, cppkafka::TopicPartition::OFFSET_BEGINNING},
    {topic, 1, cppkafka::TopicPartition::OFFSET_BEGINNING},
    {topic, 2, cppkafka::TopicPartition::OFFSET_BEGINNING},
    {topic, 3, cppkafka::TopicPartition::OFFSET_BEGINNING}
    });

// Create the connector (this will subscribe all consumers and start receiving messages)
corokafka::Connector connector(corokafka::ConfigurationBuilder(config));

// Start processing messages in the background...
std::thread t(messageProcessor);

```

## Building

Create a `build` directory and call **CMake** with various options below. Then simply call `make`. 
```shell
> cmake -B build <cmake_options> .
> cd build && make
```

### Cmake options

Various **CMake** options can be used to configure the output:
* `COROKAFKA_BUILD_SHARED`     : Build **CoroKafka** as a shared library. Default `OFF`.
* `COROKAFKA_ENABLE_PIC`       : Enable position independent code for shared library builds. Default `ON`.
* `COROKAFKA_CPPKAFKA_STATIC_LIB` : Link with **CppKafka** static library. Default `ON`.
* `COROKAFKA_RDKAFKA_STATIC_LIB`  : Link with **RdKafka** static library. Default `ON`.
* `COROKAFKA_BUILD_DOC`        : Build **Doxygen** documentation. Default `OFF`.
* `COROKAFKA_ENABLE_DOT`       : Enable generation of DOT viewer files. Default `OFF`.
* `COROKAFKA_VERBOSE_MAKEFILE` : Enable verbose cmake output. Default `ON`.
* `COROKAFKA_ENABLE_TESTS`     : Builds the `tests` target. Default `OFF`.
* `COROKAFKA_BOOST_STATIC_LIBS`: Link with **Boost** static libraries. Default `ON`.
* `COROKAFKA_BOOST_USE_MULTITHREADED` : Use **Boost** multi-threaded libraries. Default `ON`.
* `COROKAFKA_BOOST_USE_VALGRIND` : Enable **Valgrind** on **Boost**. Default `OFF`.
* `COROKAFKA_INSTALL_ROOT`     : Specify custom install path. Default is `/usr/local/include` for Linux or `c:/Program Files` for Windows.
* `RDKAFKA_ROOT`               : Specify a different **RdKafka** install directory.
* `CPPKAFKA_ROOT`              : Specify a different **CppKafka** install directory.
* `QUANTUM_ROOT`               : Specify a different **Quantum** install directory.
* `BOOST_ROOT`                 : Specify a different **Boost** install directory.
* `GTEST_ROOT`                 : Specify a different **GTest** install directory.
* `COROKAFKA_PKGCONFIG_DIR`    : Install location of the **.pc** file. Default is `share/pkgconfig`.
* `COROKAFKA_EXPORT_PKGCONFIG` : Generate `corokafka.pc` file. Default `ON`.
* `COROKAFKA_CMAKE_CONFIG_DIR` : Install location of the package config file and exports. Default is `share/cmake/CoroKafka`.
* `COROKAFKA_EXPORT_CMAKE_CONFIG` : Generate **CMake** config, target and version files. Default `ON`.

Note: options must be preceded with `-D` when passed as arguments to **CMake**.

## Installation

To install, simply run `make install` in the build directory
```shell
> cd build && make install
```

## Linking and Using

To use the library simply include `<corokafka/corokafka.h>` in your application. 

In you application **CMakeLists.txt** you can load the libraries simply by calling:
```cmake
find_package(CoroKafka REQUIRED)
target_link_libraries(<your_target> CoroKafka::corokafka <other_dependencies>)
```

## Contributions

We :heart: contributions.

Have you had a good experience with this project? Why not share some love and contribute code, or just let us know about any issues you had with it?

We welcome issue reports [here](../../issues); be sure to choose the proper issue template for your issue, so that we can be sure you're providing the necessary information.

Before sending a [Pull Request](../../pulls), please make sure you read our
[Contribution Guidelines](https://github.com/bloomberg/.github/blob/master/CONTRIBUTING.md).

## License

Please read the [LICENSE](LICENSE) file.

## Code of Conduct

This project has adopted a [Code of Conduct](https://github.com/bloomberg/.github/blob/master/CODE_OF_CONDUCT.md).
If you have any concerns about the Code, or behavior which you have experienced in the project, please
contact us at opensource@bloomberg.net.

## Security Vulnerability Reporting

If you believe you have identified a security vulnerability in this project, please send email to the project
team at opensource@bloomberg.net, detailing the suspected issue and any methods you've found to reproduce it.

Please do NOT open an issue in the GitHub repository, as we'd prefer to keep vulnerability reports private until
we've had an opportunity to review and address them.
