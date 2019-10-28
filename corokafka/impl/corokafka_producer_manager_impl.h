/*
** Copyright 2019 Bloomberg Finance L.P.
**
** Licensed under the Apache License, Version 2.0 (the "License");
** you may not use this file except in compliance with the License.
** You may obtain a copy of the License at
**
**     http://www.apache.org/licenses/LICENSE-2.0
**
** Unless required by applicable law or agreed to in writing, software
** distributed under the License is distributed on an "AS IS" BASIS,
** WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
** See the License for the specific language governing permissions and
** limitations under the License.
*/
#ifndef BLOOMBERG_COROKAFKA_PRODUCER_MANAGER_IMPL_H
#define BLOOMBERG_COROKAFKA_PRODUCER_MANAGER_IMPL_H

#include <unordered_map>
#include <atomic>
#include <future>
#include <mutex>
#include <condition_variable>
#include <corokafka/corokafka_message.h>
#include <corokafka/corokafka_metadata.h>
#include <corokafka/corokafka_configuration_builder.h>
#include <corokafka/corokafka_callbacks.h>
#include <corokafka/corokafka_producer_topic_entry.h>
#include <corokafka/corokafka_delivery_report.h>
#include <corokafka/corokafka_packed_opaque.h>

namespace Bloomberg {
namespace corokafka {

class ProducerManagerImpl
{
    friend class ProducerManager;
public:
    ~ProducerManagerImpl();
    
private:
    using ConfigMap = ConfigurationBuilder::ConfigMap<ProducerConfiguration>;
    using BuilderTuple = std::tuple<ProducerTopicEntry*, ProducerMessageBuilder<ByteArray>>;
    using MessageFuture = quantum::ThreadContextPtr<BuilderTuple>;
    using Producers = std::unordered_map<std::string,
                                         ProducerTopicEntry,
                                         std::hash<std::string>,
                                         StringEqualCompare>; //index by topic
    
    ProducerManagerImpl(quantum::Dispatcher& dispatcher,
                        const ConnectorConfiguration& connectorConfiguration,
                        const ConfigMap& configs);
    
    ProducerManagerImpl(quantum::Dispatcher& dispatcher,
                        const ConnectorConfiguration& connectorConfiguration,
                        ConfigMap&& configs);

    ProducerMetadata getMetadata(const std::string& topic);
    
    const ProducerConfiguration& getConfiguration(const std::string& topic) const;
    
    std::vector<std::string> getTopics() const;

    template <typename TOPIC, typename K, typename P, typename ...H>
    size_t send(const TOPIC& topic,
                void* opaque,
                const K& key,
                const P& payload,
                const H&... headers);
    
    template <typename TOPIC, typename K, typename P, typename ...H>
    quantum::GenericFuture<DeliveryReport>
    post(const TOPIC& topic,
         void* opaque,
         K&& key,
         P&& payload,
         H&&... headers);
    
    void waitForAcks(const std::string& topic,
                     std::chrono::milliseconds timeout);
    
    void shutdown();
    
    void poll();
    
    void post();
    
    void resetQueueFullTrigger(const std::string& topic);
    
    void enableMessageFanout(bool value);
    
    // Callbacks
    static int32_t partitionerCallback(ProducerTopicEntry& topicEntry,
                                       const cppkafka::Topic& topic,
                                       const cppkafka::Buffer& key,
                                       int32_t partitionCount);
    static void errorCallback2(ProducerTopicEntry& topicEntry,
                               cppkafka::KafkaHandleBase& handle,
                               int error,
                               const std::string& reason);
    static void errorCallback(ProducerTopicEntry& topicEntry,
                               cppkafka::KafkaHandleBase& handle,
                               int error,
                               const std::string& reason,
                               void* opaque);
    static void throttleCallback(ProducerTopicEntry& topicEntry,
                                 cppkafka::KafkaHandleBase& handle,
                                 const std::string& brokerName,
                                 int32_t brokerId,
                                 std::chrono::milliseconds throttleDuration);
    static void logCallback(ProducerTopicEntry& topicEntry,
                            cppkafka::KafkaHandleBase& handle,
                            int level,
                            const std::string& facility,
                            const std::string& message);
    static void statsCallback(ProducerTopicEntry& topicEntry,
                              cppkafka::KafkaHandleBase& handle,
                              const std::string& json);
    static void produceSuccessCallback(ProducerTopicEntry& topicEntry,
                                       const cppkafka::Message& kafkaMessage);
    static void produceTerminationCallback(ProducerTopicEntry& topicEntry,
                                           const cppkafka::Message& kafkaMessage);
    static bool flushFailureCallback(ProducerTopicEntry& topicEntry,
                                         const cppkafka::MessageBuilder& builder,
                                         cppkafka::Error error);
    static void flushTerminationCallback(ProducerTopicEntry& topicEntry,
                                         const cppkafka::MessageBuilder& builder,
                                         cppkafka::Error error);
    static void queueFullCallback(ProducerTopicEntry& topicEntry,
                                  const cppkafka::MessageBuilder& builder);
    //log + error callback wrapper
    static void report(ProducerTopicEntry& topicEntry,
                       cppkafka::LogLevel level,
                       int error,
                       const std::string& reason,
                       void* opaque);
    
    static void adjustThrottling(ProducerTopicEntry& topicEntry,
                                 const std::chrono::steady_clock::time_point& now);
    
    // Coroutines and async IO
    static int pollTask(ProducerTopicEntry& entry);
    static int produceTask(ProducerTopicEntry& entry,
                           ProducerMessageBuilder<ByteArray>&& builder);
    static int produceTaskSync(ProducerTopicEntry& entry,
                               const ProducerMessageBuilder<ByteArray>& builder);
    template <typename TOPIC, typename K, typename P, typename ...H>
    static BuilderTuple serializeCoro(quantum::VoidContextPtr ctx,
                             const TOPIC& topic,
                             ProducerTopicEntry& entry,
                             PackedOpaque* opaque,
                             K&& key,
                             P&& payload,
                             H&& ...headers);
    template <typename TOPIC, typename K, typename P, typename ...H>
    static ProducerMessageBuilder<ByteArray>
    serializeMessage(const TOPIC& topic,
                     ProducerTopicEntry& entry,
                     void* opaque,
                     const K& key,
                     const P& payload,
                     const H&... headers);
    
    static void produceMessage(const ProducerTopicEntry& topicEntry,
                               const ProducerMessageBuilder<ByteArray>& builder);
    static void flush(const ProducerTopicEntry& topicEntry);
    
    // Misc methods
    void setup(const std::string& topic, ProducerTopicEntry& topicEntry);
    static void exceptionHandler(const std::exception& ex,
                                 const ProducerTopicEntry& topicEntry);
    static ProducerMetadata makeMetadata(const ProducerTopicEntry& topicEntry);
    // Returns raw user data pointer
    static void* setPackedOpaqueFuture(const cppkafka::Message& kafkaMessage);
    static void* setPackedOpaqueFuture(const cppkafka::MessageBuilder& builder, cppkafka::Error error);
    
    Producers::iterator findProducer(const std::string& topic);
    Producers::const_iterator findProducer(const std::string& topic) const;
    
    // Members
    quantum::Dispatcher&        _dispatcher;
    Producers                   _producers;
    std::atomic_flag            _shutdownInitiated ATOMIC_FLAG_INIT;
    bool                        _shuttingDown{false};
    std::mutex                  _messageQueueMutex;
    std::condition_variable     _emptyCondition;
    std::deque<MessageFuture>   _messageQueue;
    bool                        _messageFanout{false};
};

template <typename BufferType>
BufferType makeBuffer(ByteArray& buffer);

template <> inline
cppkafka::Buffer makeBuffer<cppkafka::Buffer>(ByteArray& buffer)
{
    return cppkafka::Buffer(buffer.data(), buffer.size());
}

template <> inline
ByteArray makeBuffer<ByteArray>(ByteArray& buffer)
{
    return std::move(buffer);
}

//=============================================================================
//                          Implementations
//=============================================================================
template <typename TOPIC>
void serializeHeaders(const TOPIC&, size_t, ProducerMessageBuilder<ByteArray>&)
{}
template <typename TOPIC, typename H, typename ... Hs>
void serializeHeaders(const TOPIC& topic, size_t i, ProducerMessageBuilder<ByteArray>& builder, const H& h, const Hs&...hs) {
    ByteArray b = serialize(h);
    if (b.empty()) { throw std::exception{}; }
    builder.header(cppkafka::Header<ByteArray>{topic.headers().names()[i], std::move(b)});
    serializeHeaders(topic, ++i, builder, hs...);
}
template <typename TOPIC, typename ... Hs>
void serializeHeaders(const TOPIC& topic, size_t i, ProducerMessageBuilder<ByteArray>& builder, const NullHeader& h, const Hs&...hs) {
    serializeHeaders(topic, ++i, builder, hs...);
}

template <typename TOPIC, typename K, typename P, typename ...H>
ProducerMessageBuilder<ByteArray>
ProducerManagerImpl::serializeMessage(const TOPIC& topic,
                                      ProducerTopicEntry& entry,
                                      void* opaque,
                                      const K& key,
                                      const P& payload,
                                      const H&... headers)
{
    bool failed = false;
    ProducerMessageBuilder<ByteArray> builder(entry._configuration.getTopic());
    try {
        //Serialize key
        ByteArray b = serialize(key);
        if (b.empty()) { throw std::exception{}; }
        builder.key(std::move(b));
    }
    catch (const std::exception& ex) {
        failed = true;
        report(entry, cppkafka::LogLevel::LogErr, RD_KAFKA_RESP_ERR__KEY_SERIALIZATION, "Failed to serialize key", opaque);
    }
    try {
        //Serialize payload
        ByteArray b = serialize(payload);
        if (b.empty()) { throw std::exception{}; }
        builder.payload(std::move(b));
    }
    catch (const std::exception& ex) {
        failed = true;
        report(entry, cppkafka::LogLevel::LogErr, RD_KAFKA_RESP_ERR__VALUE_SERIALIZATION, "Failed to serialize payload", opaque);
    }
    try {
        //Serialize all headers (if any)
        serializeHeaders(topic, 0, builder, headers...);
    }
    catch (const std::exception& ex) {
        failed = true;
        report(entry, cppkafka::LogLevel::LogErr, RD_KAFKA_RESP_ERR__VALUE_SERIALIZATION, "Failed to serialize a header", opaque);
    }
    if (failed) {
        return {};
    }
    // Add timestamp
    builder.timestamp(std::chrono::high_resolution_clock::now());
    return builder;
}

template <typename TOPIC, typename K, typename P, typename ...H>
ProducerManagerImpl::BuilderTuple
ProducerManagerImpl::serializeCoro(quantum::VoidContextPtr ctx,
                                 const TOPIC& topic,
                                 ProducerTopicEntry& entry,
                                 PackedOpaque* opaque,
                                 K&& key,
                                 P&& payload,
                                 H&& ...headers)
{
    using Builder = ProducerMessageBuilder<ByteArray>;
    ProducerManagerImpl::BuilderTuple builderTuple =
        {&entry, Builder(entry._configuration.getTopic())};
    Builder& builder = std::get<1>(builderTuple);
    try {
        builder.key(serialize(key));
    }
    catch (const std::exception& ex) {
        report(entry, cppkafka::LogLevel::LogErr, RD_KAFKA_RESP_ERR__KEY_SERIALIZATION, "Failed to serialize key", opaque);
        throw std::runtime_error("Key serialization failed");
    }
    try {
        builder.payload(serialize(payload));
    }
    catch (const std::exception& ex) {
        report(entry, cppkafka::LogLevel::LogErr, RD_KAFKA_RESP_ERR__VALUE_SERIALIZATION, "Failed to serialize payload", opaque);
        throw std::runtime_error("Payload serialization failed");
    }
    try {
        serializeHeaders(topic, 0, builder, headers...);
    }
    catch (const std::exception& ex) {
        report(entry, cppkafka::LogLevel::LogErr, RD_KAFKA_RESP_ERR__VALUE_SERIALIZATION, "Failed to serialize a header", opaque);
        throw std::runtime_error("Header serialization failed");
    }
    // Add timestamp
    builder.timestamp(std::chrono::high_resolution_clock::now());
    builder.user_data(opaque);
    return builderTuple;
}

template <typename TOPIC, typename K, typename P, typename ...H>
size_t ProducerManagerImpl::send(const TOPIC& topic,
                                 void* opaque,
                                 const K& key,
                                 const P& payload,
                                 const H&... headers)
{
    auto ctx = quantum::local::context();
    if (ctx) {
        return post(topic, opaque, key, payload, headers...).get().getNumBytesWritten();
    }
    ProducerTopicEntry& topicEntry = findProducer(topic.topic())->second;
    ProducerMessageBuilder<ByteArray> builder = serializeMessage(topic, topicEntry, opaque, key, payload, headers...);
    if (builder.topic().empty()) {
        //Serializing failed
        return 0;
    }
    builder.user_data(new PackedOpaque(opaque, quantum::Promise<DeliveryReport>()));
    if (!builder.payload().empty()) {
        produceMessage(topicEntry, builder); //blocks until delivery report is received
    }
    return builder.payload().size();
}

template <typename TOPIC, typename K, typename P, typename ...H>
quantum::GenericFuture<DeliveryReport>
ProducerManagerImpl::post(const TOPIC& topic,
                          void* opaque,
                          K&& key,
                          P&& payload,
                          H&&...headers)
{
    ProducerTopicEntry& topicEntry = findProducer(topic.topic())->second;
    if (topicEntry._payloadPolicy == cppkafka::Producer::PayloadPolicy::PASSTHROUGH_PAYLOAD) {
        throw std::runtime_error("Invalid async operation for pass-through payload policy - use send() instead.");
    }
    if (topicEntry._preserveMessageOrder && (topicEntry._producer->get_buffer_size() > topicEntry._maxQueueLength)) {
        throw std::runtime_error("Internal queue full");
    }
    quantum::Promise<DeliveryReport> deliveryPromise;
    quantum::GenericFuture<DeliveryReport> deliveryFuture;
    auto ctx = quantum::local::context();
    if (ctx) {
        deliveryFuture = {deliveryPromise.getICoroFuture(), ctx};
    }
    else {
        deliveryFuture = deliveryPromise.getIThreadFuture();
    }
    // Post the serialization future and return
    {
        std::unique_lock<std::mutex> lock(_messageQueueMutex);
        _messageQueue.emplace_back(_dispatcher.post(
                serializeCoro<TOPIC,K,P,H...>,
                topic,
                topicEntry,
                new PackedOpaque(opaque, std::move(deliveryPromise)),
                std::forward<K>(key),
                std::forward<P>(payload),
                std::forward<H>(headers)...));
    }
    _emptyCondition.notify_one();
    return deliveryFuture;
}
 
}}

#endif //BLOOMBERG_COROKAFKA_PRODUCER_MANAGER_IMPL_H
