# CoroKafka library
Coroutine-based Kafka connector library. Powerful, scalable and easy to use C++ library built on top of
`librdkafka`, `cppkafka` and `quantum`, supporting any number of parallel producers and consumers.
Producing and consuming of messages is simplified by allowing applications to use their own native message
formats which are automatically serialized and deserialized by the connector. Integrated support for `rdkafka` headers.
Currently the library only supports static topics.

Compiles for -std=gnu++11, -std=gnu++14 and -std=gnu++17.

See [API documentation](https://bbgithub.dev.bloomberg.com/pages/eor/corokafka/) for more details.

## Building and installing
```shell
> cmake -B build <options> .
> cd build && make install
```
Note that this project is dependent on the following libraries which must appear on your application link line:
* `cppkafka`
* `rdkafka` (Only if statically linking. Otherwise cppkafka.so has librdkafka.so in its rpath)
* `boost_context`
* `pthread`

To use the library simply include `<corokafka/corokafka.h>` in your application. 

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
* `RDKAFKA_ROOT_DIR`           : Specify a different RdKafka install directory.
* `CPPKAFKA_ROOT_DIR`          : Specify a different CppKafka install directory.
* `QUANTUM_ROOT_DIR`           : Specify a different Quantum install directory.
* `BOOST_ROOT`                 : Specify a different Boost install directory.
* `GTEST_ROOT`                 : Specify a different GTest install directory.

Note: options must be preceded with `-D` when passed as arguments to CMake.

## Configuration
Since `corokafka` is built on top of `rdkafka`, all configuration [options](https://github.com/accelerated/librdkafka/blob/master/CONFIGURATION.md) available in `rdkafka` can be
used directly. Additional configuration options specific to `corokafka` are also exposed. 
* [Connector configuration](https://bbgithub.dev.bloomberg.com/eor/corokafka/blob/master/corokafka/src/corokafka_connector_configuration.h#L30)
* [Producer configuration](https://bbgithub.dev.bloomberg.com/eor/corokafka/blob/master/corokafka/src/corokafka_producer_configuration.h#L26:L86)
* [Consumer configuration](https://bbgithub.dev.bloomberg.com/eor/corokafka/blob/master/corokafka/src/corokafka_consumer_configuration.h#L26:L145)

## Producer example
In the following example we will be producing some messages with a key of type `size_t`, a payload of type
`std::string` and no header.
```
//==========================================================================
//                          Producer setup
//==========================================================================
#include <corokafka/corokafka.h>

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
               int level,
               const std::string& facility,
               const std::string& message)
{
    std::cout << "Log from: " << metadata.getInternalName()
              << " facility: " << facility
              << " text: " << message << std::endl;
}

// Define a serializer (mandatory - see documentation for mandatory callbacks)
// We will send a message with a 'size_t' key and a 'std::string' payload
corokafka::ConcreteSerializerWithoutHeader<size_t, std::string>::Packed
serializerCallback(const size_t& key, const std::string& payload)
{
    return corokafka::ConcreteSerializerWithoutHeader<size_t, std::string>::Packed
        (corokafka::ByteArray(reinterpret_cast<const uint8_t*>(&key),
         reinterpret_cast<const uint8_t*>(&key) + sizeof(size_t)),
         corokafka::ByteArray(payload.begin(), payload.end()));
}

// Create a topic configuration (optional)
std::initializer_list<corokafka::ConfigurationOption > topicOptions = {
	{ "produce.offset.report",  true },
	{ "request.required.acks", -1 }
};

// Create a producer configuration (only 'metadata.broker.list' setting is mandatory)
std::initializer_list<corokafka::ConfigurationOption > configOptions = {
	{ "metadata.broker.list", "some_broker.query.bms.bloomberg.com:port" },
	{ "api.version.request", true },
	{ "internal.producer.retries", 5 }
};

// Associate the topic and producer configuration with a topic name.
// Note: any number of producer configs/topics can be created.
corokafka::ProducerConfiguration config("my-topic", configOptions, topicOptions);

// Add the callbacks
config.setCallback(logCallback);
config.setCallback(deliveryReportCallback);
config.setCallback<size_t, std::string>(serializerCallback);

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

for (size_t i = 0; i < 10; ++i) {
	std::ostringstream ss;
	ss << "Hello world " << i;
	std::tuple<size_t, std::string> message(i, ss.str());

	//produce a message synchronously (async production also available)
	producer.send("my-topic", message);
}

```

## Consumer example
In the following example we will be consuming messages with a key of type `size_t`, a payload of type
`std::string` and no header.
```
//==========================================================================
//                 Message worker queue and processor
//==========================================================================
#include <corokafka/corokafka.h>

std::mutex messageMutex;
std::deque<corokafka::ReceivedMessageWithoutHeader<size_t, std::string>> messages;

void messageProcessor()
{
    while (1)
    {
        if (!messages.empty()) {
            std::lock_guard<std::mutex> lock(messageMutex);
            corokafka::ReceivedMessageWithoutHeader<size_t, std::string> message = std::move(messages.front());
            messages.pop_front();
            if (message && !message.getError()) {
                std::cout << "Dequeued message from topic: " << message.getTopic()
                          << " with key: " << message.getKey()
                          << " payload: " << (std::string)message.getPayload()
                          << std::endl;
                message.commit(); //commit the message which will trigger 'on commit' callback
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
}

//==========================================================================
//                             Consumer setup
//==========================================================================

// Define some callback (optional - see documentation for all available callbacks)
void logCallback(const corokafka::Metadata& metadata,
               int level,
               const std::string& facility,
               const std::string& message)
{
    std::cout << "Log from: " << metadata.getInternalName()
              << " facility: " << facility
              << " text: " << message << std::endl;
}

// Define a deserializer callback (mandatory)
corokafka::ConcreteDeserializerWithoutHeader<size_t, std::string>::Unpacked
deserializerCallback(const corokafka::Buffer& key, const corokafka::Buffer& payload)
{
    return corokafka::ConcreteDeserializerWithoutHeader<size_t, std::string>::Unpacked
        (*reinterpret_cast<const size_t*>(key.get_data()), std::string(payload.begin(), payload.end()));
}

// Define a receiver callback (mandatory)
void receiverCallback(corokafka::ReceivedMessageWithoutHeader<size_t, std::string> message)
{
    if (message.getError()) {
        std::cout << "Enqueued message from topic: " << message.getTopic()
                  << " with error: " << message.getError().to_string()
                  << std::endl;
    }
    else {
        std::cout << "Enqueued message from topic: " << message.getTopic()
                  << " with key: " << message.getKey()
                  << " payload: " << (std::string)message.getPayload()
                  << std::endl;
    }

    // Post message unto a worker queue...
    std::lock_guard<std::mutex> lock(messageMutex);
    messages.emplace_back(std::move(message));
}

// Create the producer and topic config
std::initializer_list<corokafka::ConfigurationOption > configOptions = {
	{ "metadata.broker.list", "some_broker.query.bms.bloomberg.com:port" },
	{ "group.id", "my-group" },
	{ "api.version.request", true },
	{ "enable.partition.eof", true },
	{ "enable.auto.offset.store", true },
	{ "enable.auto.commit", false },
	{ "auto.offset.reset", "earliest" },
	{ "partition.assignment.strategy", "roundrobin" },
	{ "internal.consumer.pause.on.start", false }
};
corokafka::ConsumerConfiguration config(topic, configOptions, {});

//add the callbacks
config.setCallback(logCallback);
config.setCallback<size_t, std::string>(deserializerCallback);
config.setCallback<size_t, std::string>(receiverCallback);

// Optionally set initial partition assignment
config.assignInitialPartitions(corokafka::PartitionStrategy::Dynamic,
						   {{topic, 0, corokafka::TopicPartition::OFFSET_BEGINNING},
						    {topic, 1, corokafka::TopicPartition::OFFSET_BEGINNING},
						    {topic, 2, corokafka::TopicPartition::OFFSET_BEGINNING},
						    {topic, 3, corokafka::TopicPartition::OFFSET_BEGINNING}});

// Create the connector (this will subscribe all consumers and start receiving messages)
corokafka::Connector connector(corokafka::ConfigurationBuilder(config));

// Start processing messages in the background...
std::thread t(messageProcessor);

```
