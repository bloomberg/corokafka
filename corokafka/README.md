# CoroKafka library
Coroutine-based Kafka messaging library! **CoroKafka** is a scalable and simple to use C++ library built on top of [cppkafka](https://github.com/mfontanini/cppkafka) and [quantum](https://github.com/bloomberg/quantum), supporting any number of parallel producers and consumers.
Producing and consuming of messages is simplified by allowing applications to use their own native message formats which are automatically serialized and de-serialized by the connector. Message processing spreads to optimal number of parallel coroutines and threads. Integrated support for `rdkafka` headers. Currently the library only supports static topics and admin API is not yet available.

Compiles for -std=gnu++11, -std=gnu++14 and -std=gnu++17.

See [API documentation](https://bbgithub.dev.bloomberg.com/eor/corokafka/tree/master/corokafka) and available [configuration](https://bbgithub.dev.bloomberg.com/eor/corokafka/blob/master/corokafka/CONFIGURATION.md) options for more details.

## Building and installing
```shell
> cmake -B build <options> .
> cd build && make install
```
To use the library simply include `<corokafka/corokafka.h>` in your application. 

In CMake you can load the libraries simply by calling:
```cmake
find_package(CoroKafka REQUIRED)
target_link_libraries(<your_target> CoroKafka::corokafka <other_dependencies>)
```
Note that if `RdKafka`, `CppKafka`, `Quantum` or `Boost` are not installed in default locations, you may need to define the respective \<package\>_ROOT variables below. See [tests/CMakeLists.txt](https://bbgithub.dev.bloomberg.com/eor/corokafka/blob/master/corokafka/tests/CMakeLists.txt#L11) for a working example.

### CMake options
Various **CMake** options can be used to configure the output:
* `COROKAFKA_BUILD_SHARED`     : Build corokafka as a shared library. Default `OFF`.
* `COROKAFKA_ENABLE_PIC`       : Enable position independent code for shared library builds. Default `ON`.
* `COROKAFKA_CPPKAFKA_STATIC_LIB` : Link with cppkafka static library. Default `ON`.
* `COROKAFKA_RDKAFKA_STATIC_LIB`  : Link with rdkafka static library. Default `ON`.
* `COROKAFKA_BUILD_DOC`        : Build Doxygen documentation. Default `OFF`.
* `COROKAFKA_ENABLE_DOT`       : Enable generation of DOT viewer files. Default `OFF`.
* `COROKAFKA_VERBOSE_MAKEFILE` : Enable verbose cmake output. Default `ON`.
* `COROKAFKA_ENABLE_TESTS`     : Builds the `tests` target. Default `OFF`.
* `COROKAFKA_BOOST_STATIC_LIBS`: Link with Boost static libraries. Default `ON`.
* `COROKAFKA_BOOST_USE_MULTITHREADED` : Use Boost multi-threaded libraries. Default `ON`.
* `COROKAFKA_BOOST_USE_VALGRIND` : Enable valgrind on Boost. Default `OFF`.
* `COROKAFKA_INSTALL_ROOT`     : Specify custom install path. Default is `/usr/local/include` for Linux or `c:/Program Files` for Windows.
* `RDKAFKA_ROOT`               : Specify a different RdKafka install directory.
* `CPPKAFKA_ROOT`              : Specify a different CppKafka install directory.
* `QUANTUM_ROOT`               : Specify a different Quantum install directory.
* `BOOST_ROOT`                 : Specify a different Boost install directory.
* `GTEST_ROOT`                 : Specify a different GTest install directory.
* `COROKAFKA_PKGCONFIG_DIR`    : Install location of the .pc file. Default is `share/pkgconfig`.
* `COROKAFKA_EXPORT_PKGCONFIG` : Generate `corokafka.pc` file. Default `ON`.
* `COROKAFKA_CMAKE_CONFIG_DIR` : Install location of the package config file and exports. Default is `share/cmake/CoroKafka`.
* `COROKAFKA_EXPORT_CMAKE_CONFIG` : Generate CMake config, target and version files. Default `ON`.

Note: options must be preceded with `-D` when passed as arguments to CMake.

## Configuration
Since `corokafka` is built on top of `rdkafka`, all configuration [options](https://github.com/accelerated/librdkafka/blob/master/CONFIGURATION.md) available in `rdkafka` can be
used directly. Additional configuration options specific to `corokafka` are also exposed. 
* [Connector configuration](https://bbgithub.dev.bloomberg.com/eor/corokafka/blob/master/corokafka/corokafka/corokafka_connector_configuration.h#L30)
* [Producer configuration](https://bbgithub.dev.bloomberg.com/eor/corokafka/blob/master/corokafka/CONFIGURATION.md#producer-configuration)
* [Consumer configuration](https://bbgithub.dev.bloomberg.com/eor/corokafka/blob/master/corokafka/CONFIGURATION.md#consumer-configuration)

## Producer example
In the following example we will be producing some messages with a key of type `size_t`, a payload of type
`std::string` and a simple header called _Header1_. For full producer API see [here](https://bbgithub.dev.bloomberg.com/eor/corokafka/blob/master/corokafka/corokafka/corokafka_producer_manager.h#L34).
```
//==========================================================================
//                       Serializers/Deserializers
//==========================================================================
//------------------------ serializers.h -----------------------------------
// This serializer is used for the key
std::vector<uint8_t> serialize(const size_t&);

// This serializer is used for the headers and the payload
std::vector<uint8_t> serialize(const std::string&);

// This is the deserializer for the key
size_t deserialize(const cppkafka::TopicPartition&, const cppkafka::Buffer&, size_t*)

// This is the deserializer for the header and payload
std::string deserialize(const cppkafka::TopicPartition&, const cppkafka::Buffer&, std::string*);

//------------------------ serializers.cpp -----------------------------------
std::vector<uint8_t> serialize(const size_t& key)
{
    return {reinterpret_cast<const uint8_t*>(&key),
            reinterpret_cast<const uint8_t*>(&key) + sizeof(size_t)};
}

std::vector<uint8_t> serialize(const std::string& header)
{
    return {header.begin(), header.end()};
}

size_t deserialize(const cppkafka::TopicPartition&, 
                   const cppkafka::Buffer& key, 
                   size_t* /*unused*/)
{
    return *static_cast<const size_t*>((void*)key.get_data());
}

std::string deserialize(const cppkafka::TopicPartition&, 
                        const cppkafka::Buffer& headerOrPayload, 
                        std::string* /*unused*/)
{
    return {headerOrPayload.begin(), headerOrPayload.end()};
}

//==========================================================================
//                                Topic
//==========================================================================
//------------------------------ mytopic.h ---------------------------------
#include <serializers.h>
#include <corokafka/corokafka.h>

using MyTopic = corokafka::Topic<size_t, std::string, coroakafka::Headers<std::string>>;

// Create a topic which will be shared between producer and consumer.
static MyTopic myTopic("my-topic", corokafka::Header<std::string>("Header1");

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

## Consumer example
In the following example we will be consuming messages with a key of type `size_t`, a payload of type
`std::string` and a simple header called _Header1_. For full consumer API see [here](https://bbgithub.dev.bloomberg.com/eor/corokafka/blob/master/corokafka/corokafka/corokafka_consumer_manager.h#L32).
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
                //headerValue = message.getHeader<0>();
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
