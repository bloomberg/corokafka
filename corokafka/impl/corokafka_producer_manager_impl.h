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

class ProducerManagerImpl : public Interruptible
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
    DeliveryReport send(const TOPIC& topic,
                        const void* opaque,
                        const K& key,
                        const P& payload,
                        const H&... headers);
    
    template <typename TOPIC, typename K, typename P, typename ...H>
    quantum::GenericFuture<DeliveryReport>
    post(const TOPIC& topic,
         const void* opaque,
         K&& key,
         P&& payload,
         H&&... headers);
    
    template <typename TOPIC, typename K, typename P, typename ...H>
    quantum::GenericFuture<DeliveryReport>
    postImpl(ExecMode mode,
             const TOPIC& topic,
             const void* opaque,
             K&& key,
             P&& payload,
             H&&... headers);
    
    bool waitForAcks(const std::string& topic);
    
    bool waitForAcks(const std::string& topic,
                     std::chrono::milliseconds timeout);
    
    void shutdown();
    
    void poll();
    
    void pollEnd();
    
    void resetQueueFullTrigger(const std::string& topic);
    
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
                               cppkafka::Error error,
                               const std::string& reason,
                               const void* sendOpaque);
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
                       cppkafka::Error error,
                       const std::string& reason,
                       const void* sendOpaque);
    
    static void adjustThrottling(ProducerTopicEntry& topicEntry,
                                 const std::chrono::steady_clock::time_point& now);
    
    // Coroutines and async IO
    static int flushTask(ProducerTopicEntry& entry, IoTracker);
    template <typename BUILDER>
    static int produceMessage(ExecMode mode,
                              ProducerTopicEntry& entry,
                              BUILDER&& builder,
                              IoTracker);
    template <typename TOPIC, typename K, typename P, typename ...H>
    static ProducerMessageBuilder<ByteArray>
    serializeMessage(const TOPIC& topic,
                     ProducerTopicEntry& entry,
                     const void* opaque,
                     const K& key,
                     const P& payload,
                     const H&... headers);
    
    static void flush(const ProducerTopicEntry& topicEntry);
    
    // Misc methods
    void setup(const std::string& topic, ProducerTopicEntry& topicEntry);
    static void exceptionHandler(const std::exception& ex,
                                 const ProducerTopicEntry& topicEntry);
    static ProducerMetadata makeMetadata(const ProducerTopicEntry& topicEntry);
    // Returns raw user data pointer
    static const void* setPackedOpaqueFuture(const cppkafka::Message& kafkaMessage);
    static const void* setPackedOpaqueFuture(const cppkafka::MessageBuilder& builder, cppkafka::Error error);
    
    Producers::iterator findProducer(const std::string& topic);
    Producers::const_iterator findProducer(const std::string& topic) const;
    static int32_t mapKeyToQueue(const uint8_t* obj,
                                 size_t len,
                                 const ProducerTopicEntry& topicEntry);
    static bool isFlushNonBlocking(const ProducerTopicEntry& topicEntry);
    
    // Members
    quantum::Dispatcher&        _dispatcher;
    Producers                   _producers;
    std::atomic_flag            _shutdownInitiated{0};
    std::deque<MessageFuture>   _messageQueue;
    std::chrono::milliseconds   _shutdownIoWaitTimeoutMs{2000};
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
bool serializeHeaders(const TOPIC&, size_t, ProducerMessageBuilder<ByteArray>&)
{
    return true; //nothing to serialize
}
template <typename TOPIC, typename H, typename ... Hs>
bool serializeHeaders(const TOPIC& topic, size_t i, ProducerMessageBuilder<ByteArray>& builder, const H& h, const Hs&...hs) {
    ByteArray b = Serialize<H>{}(h);
    if (b.empty()) {
        return false;
    }
    builder.header(cppkafka::Header<ByteArray>{topic.headers().names()[i], std::move(b)});
    //Serialize next header in the list
    return serializeHeaders(topic, ++i, builder, hs...);
}
template <typename TOPIC, typename ... Hs>
bool serializeHeaders(const TOPIC& topic, size_t i, ProducerMessageBuilder<ByteArray>& builder, const NullHeader& h, const Hs&...hs) {
    return serializeHeaders(topic, ++i, builder, hs...);
}

template <typename TOPIC, typename K, typename P, typename ...H>
ProducerMessageBuilder<ByteArray>
ProducerManagerImpl::serializeMessage(const TOPIC& topic,
                                      ProducerTopicEntry& entry,
                                      const void* opaque,
                                      const K& key,
                                      const P& payload,
                                      const H&... headers)
{
    bool failed = false;
    ProducerMessageBuilder<ByteArray> builder(entry._configuration.getTopic());
    try {
        //Serialize key
        ByteArray b = Serialize<K>{}(key);
        if (b.empty()) {
            failed = true;
            report(entry, cppkafka::LogLevel::LogErr, RD_KAFKA_RESP_ERR__KEY_SERIALIZATION, "Failed to serialize key", opaque);
        }
        else {
            builder.key(std::move(b));
        }
    }
    catch (const std::exception& ex) {
        failed = true;
        report(entry, cppkafka::LogLevel::LogErr, RD_KAFKA_RESP_ERR__KEY_SERIALIZATION,
            std::string("Failed to serialize key with error: ") + ex.what(), opaque);
    }
    try {
        //Serialize payload
        ByteArray b = Serialize<P>{}(payload);
        if (b.empty()) {
            failed = true;
            report(entry, cppkafka::LogLevel::LogErr, RD_KAFKA_RESP_ERR__VALUE_SERIALIZATION, "Failed to serialize payload", opaque);
        }
        else {
            builder.payload(std::move(b));
        }
    }
    catch (const std::exception& ex) {
        failed = true;
        report(entry, cppkafka::LogLevel::LogErr, RD_KAFKA_RESP_ERR__VALUE_SERIALIZATION,
            std::string("Failed to serialize payload with error: ") + ex.what(), opaque);
    }
    try {
        //Serialize all headers (if any)
        if (!serializeHeaders(topic, 0, builder, headers...)) {
            failed = true;
            report(entry, cppkafka::LogLevel::LogErr, RD_KAFKA_RESP_ERR__VALUE_SERIALIZATION, "Failed to serialize a header", opaque);
        }
    }
    catch (const std::exception& ex) {
        failed = true;
        report(entry, cppkafka::LogLevel::LogErr, RD_KAFKA_RESP_ERR__VALUE_SERIALIZATION,
            std::string("Failed to serialize a header with error: ") + ex.what() , opaque);
    }
    if (failed) {
        return {}; //Topic will be empty
    }
    // Add timestamp
    builder.timestamp(std::chrono::high_resolution_clock::now());
    // Add user data
    builder.user_data(const_cast<void*>(opaque));
    return builder;
}

template <typename TOPIC, typename K, typename P, typename ...H>
DeliveryReport ProducerManagerImpl::send(const TOPIC& topic,
                                         const void* opaque,
                                         const K& key,
                                         const P& payload,
                                         const H&... headers)
{
    return postImpl(ExecMode::Sync, topic, opaque, key, payload, headers...).get();
}

template <typename TOPIC, typename K, typename P, typename ...H>
quantum::GenericFuture<DeliveryReport>
ProducerManagerImpl::post(const TOPIC& topic,
                          const void* opaque,
                          K&& key,
                          P&& payload,
                          H&&...headers)
{
    return postImpl(ExecMode::Async, topic, opaque, std::forward<K>(key), std::forward<P>(payload), std::forward<H>(headers)...);
}

template <typename TOPIC, typename K, typename P, typename ...H>
quantum::GenericFuture<DeliveryReport>
ProducerManagerImpl::postImpl(ExecMode mode,
                              const TOPIC& topic,
                              const void* opaque,
                              K&& key,
                              P&& payload,
                              H&&...headers)
{
    quantum::Promise<DeliveryReport> deliveryPromise;
    quantum::GenericFuture<DeliveryReport> deliveryFuture;
    auto ctx = quantum::local::context();
    if (ctx) {
        deliveryFuture = {deliveryPromise.getICoroFuture(), ctx};
    }
    else {
        deliveryFuture = deliveryPromise.getIThreadFuture();
    }
    ProducerTopicEntry& topicEntry = findProducer(topic.topic())->second;
    if (mode == ExecMode::Async) {
        if (topicEntry._preserveMessageOrder) {
            if ((topicEntry._maxQueueLength > -1) &&
                (topicEntry._producer->get_buffer_size() > (size_t)topicEntry._maxQueueLength)) {
                deliveryPromise.set(DeliveryReport{{}, 0, RD_KAFKA_RESP_ERR__QUEUE_FULL, opaque});
                return deliveryFuture;
            }
        }
        else {
            if (topicEntry._payloadPolicy == cppkafka::Producer::PayloadPolicy::PASSTHROUGH_PAYLOAD) {
                //Application needs to set PayloadPolicy::COPY_PAYLOAD
                deliveryPromise.set(DeliveryReport{{}, 0, RD_KAFKA_RESP_ERR__INVALID_ARG, opaque});
                return deliveryFuture;
            }
        }
    }
    ProducerMessageBuilder<ByteArray> builder = serializeMessage(topic, topicEntry, opaque, key, payload, headers...);
    if (builder.topic().empty()) {
        //Serializing failed
        deliveryPromise.set(DeliveryReport{{}, 0, RD_KAFKA_RESP_ERR__VALUE_SERIALIZATION, opaque});
        return deliveryFuture;
    }
    builder.user_data(new PackedOpaque(opaque, std::move(deliveryPromise)));
    int rc = 0;
    if (ctx && (mode == ExecMode::Sync)) {
        //Find thread id
        int numThreads = topicEntry._syncProducerThreadRange.second-topicEntry._syncProducerThreadRange.first+1;
        int threadId = mapKeyToQueue(reinterpret_cast<const uint8_t*>(&key), sizeof(key), topicEntry);
        //Post this asynchronously
        rc = ctx->postAsyncIo(threadId,
                              false,
                              produceMessage<ProducerMessageBuilder<ByteArray>>,
                              ExecMode(mode),
                              topicEntry,
                              std::move(builder),
                              IoTracker(topicEntry._ioTracker))->get(ctx);
    }
    else {
        rc = produceMessage(mode, topicEntry, std::move(builder), IoTracker{});
    }
    if (rc != 0) {
        deliveryPromise.set(DeliveryReport{{}, 0, RD_KAFKA_RESP_ERR__BAD_MSG, opaque});
    }
    return deliveryFuture;
}

template <typename BUILDER>
int ProducerManagerImpl::produceMessage(ExecMode mode,
                                        ProducerTopicEntry& entry,
                                        BUILDER&& builder,
                                        IoTracker)
{
    try {
        if (mode == ExecMode::Sync) {
            entry._producer->sync_produce(builder, entry._waitForAcksTimeout);
        }
        else {
            //ExecMode::Async
            if (entry._preserveMessageOrder) {
                // Save to local buffer so we can flush synchronously later.
                // This will incur one buffer copy.
                entry._producer->add_message(std::forward<BUILDER>(builder));
            }
            else {
                entry._producer->produce(builder);
            }
        }
    }
    catch (const std::exception& ex) {
        exceptionHandler(ex, entry);
        return -1;
    }
    return 0;
}
 
}}

#endif //BLOOMBERG_COROKAFKA_PRODUCER_MANAGER_IMPL_H
