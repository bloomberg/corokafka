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
    using BuilderTuple = std::tuple<ProducerTopicEntry*, std::unique_ptr<ConcreteMessageBuilder<ByteArray>>>;
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

    template <typename K, typename P>
    size_t send(const std::string& topic,
                const K& key,
                const P& payload,
                const HeaderPack& headers,
                void* opaque);
    
    template <typename K, typename P>
    quantum::GenericFuture<DeliveryReport>
    post(const std::string& topic,
         K&& key,
         P&& payload,
         const HeaderPack& headers,
         void* opaque);
    
    template <typename K, typename P>
    quantum::GenericFuture<DeliveryReport>
    post(const std::string& topic,
         K&& key,
         P&& payload,
         HeaderPack&& headers,
         void* opaque);
    
    void waitForAcks(const std::string& topic,
                     std::chrono::milliseconds timeout);
    
    void shutdown();
    
    void poll();
    
    void post();
    
    template <typename K, typename P, typename HEADERS>
    quantum::GenericFuture<DeliveryReport>
    postImpl(const std::string& topic,
             K&& key,
             P&& payload,
             HEADERS&& message,
             void* opaque);
    
    void resetQueueFullTrigger(const std::string& topic);
    
    void enableMessageFanout(bool value);
    
    // Callbacks
    static int32_t partitionerCallback(ProducerTopicEntry& topicEntry,
                                       const Topic& topic,
                                       const Buffer& key,
                                       int32_t partitionCount);
    static void errorCallback2(ProducerTopicEntry& topicEntry,
                               KafkaHandleBase& handle,
                               int error,
                               const std::string& reason);
    static void errorCallback(ProducerTopicEntry& topicEntry,
                               KafkaHandleBase& handle,
                               int error,
                               const std::string& reason,
                               void* opaque);
    static void throttleCallback(ProducerTopicEntry& topicEntry,
                                 KafkaHandleBase& handle,
                                 const std::string& brokerName,
                                 int32_t brokerId,
                                 std::chrono::milliseconds throttleDuration);
    static void logCallback(ProducerTopicEntry& topicEntry,
                            KafkaHandleBase& handle,
                            int level,
                            const std::string& facility,
                            const std::string& message);
    static void statsCallback(ProducerTopicEntry& topicEntry,
                              KafkaHandleBase& handle,
                              const std::string& json);
    static void produceSuccessCallback(ProducerTopicEntry& topicEntry,
                                       const Message& kafkaMessage);
    static void produceTerminationCallback(ProducerTopicEntry& topicEntry,
                                           const Message& kafkaMessage);
    static bool flushFailureCallback(ProducerTopicEntry& topicEntry,
                                         const MessageBuilder& builder,
                                         Error error);
    static void flushTerminationCallback(ProducerTopicEntry& topicEntry,
                                         const MessageBuilder& builder,
                                         Error error);
    static void queueFullCallback(ProducerTopicEntry& topicEntry,
                                  const MessageBuilder& builder);
    //log + error callback wrapper
    static void report(ProducerTopicEntry& topicEntry,
                       LogLevel level,
                       int error,
                       const std::string& reason,
                       void* opaque);
    
    static void adjustThrottling(ProducerTopicEntry& topicEntry,
                                 const std::chrono::steady_clock::time_point& now);
    
    // Coroutines and async IO
    static int pollTask(quantum::ThreadPromise<int>::Ptr promise,
                        ProducerTopicEntry& entry);
    static int produceTask(quantum::ThreadPromise<int>::Ptr promise,
                           ProducerTopicEntry& entry,
                           ConcreteMessageBuilder<ByteArray>&& builder);
    static int produceTaskSync(ProducerTopicEntry& entry,
                               const ConcreteMessageBuilder<ByteArray>& builder);
    template <typename K, typename P, typename HEADERS>
    static int serializeCoro(quantum::CoroContext<BuilderTuple>::Ptr ctx,
                             ProducerTopicEntry& entry,
                             K&& key,
                             P&& payload,
                             HEADERS&& headers,
                             PackedOpaque* opaque);
    static ConcreteMessageBuilder<ByteArray>
    serializeMessage(ProducerTopicEntry& entry,
                     const void* key,
                     const void* payload,
                     const HeaderPack* headers,
                     void* opaque);
    
    static void produceMessage(const ProducerTopicEntry& topicEntry,
                               const ConcreteMessageBuilder<ByteArray>& builder);
    static void flush(const ProducerTopicEntry& topicEntry);
    
    // Misc methods
    void setup(const std::string& topic, ProducerTopicEntry& topicEntry);
    static void exceptionHandler(const std::exception& ex,
                                 const ProducerTopicEntry& topicEntry);
    static ProducerMetadata makeMetadata(const ProducerTopicEntry& topicEntry);
    // Returns raw user data pointer
    static void* setPackedOpaqueFuture(const Message& kafkaMessage);
    static void* setPackedOpaqueFuture(const MessageBuilder& builder, Error error);
    
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
Buffer makeBuffer<Buffer>(ByteArray& buffer)
{
    return Buffer(buffer.data(), buffer.size());
}

template <> inline
ByteArray makeBuffer<ByteArray>(ByteArray& buffer)
{
    return std::move(buffer);
}

//=============================================================================
//                          Implementations
//=============================================================================
template <typename K, typename P, typename HEADERS>
int ProducerManagerImpl::serializeCoro(quantum::CoroContext<BuilderTuple>::Ptr ctx,
                                       ProducerTopicEntry& entry,
                                       K&& key,
                                       P&& payload,
                                       HEADERS&& headers,
                                       PackedOpaque* opaque)
{
    ConcreteMessageBuilder<ByteArray> builder = serializeMessage(entry, &key, &payload, &headers, opaque->first);
    if (builder.topic().empty()) {
        //Serializing failed
        return -1;
    }
    builder.user_data(opaque);
    return ctx->set(BuilderTuple(&entry, std::unique_ptr<ConcreteMessageBuilder<ByteArray>>
                                             (new ConcreteMessageBuilder<ByteArray>(std::move(builder)))));
}

template <typename K, typename P>
size_t ProducerManagerImpl::send(const std::string& topic,
                                 const K& key,
                                 const P& payload,
                                 const HeaderPack& headers,
                                 void* opaque)
{
    auto ctx = quantum::local::context();
    if (ctx) {
        return post(topic, key, payload, headers, opaque).get().getNumBytesProduced();
    }
    auto it = _producers.find(topic);
    if (it == _producers.end()) {
        throw std::runtime_error("Invalid topic");
    }
    ProducerTopicEntry& topicEntry = it->second;
    ConcreteMessageBuilder<ByteArray> builder = serializeMessage(topicEntry, &key, &payload, &headers, opaque);
    if (builder.topic().empty()) {
        //Serializing failed
        return 0;
    }
    builder.user_data(new PackedOpaque(opaque, quantum::Promise<DeliveryReport>()));
    if (!builder.payload().empty()) {
        produceMessage(topicEntry, builder);
    }
    return builder.payload().size();
}

template <typename K, typename P, typename HEADERS>
quantum::GenericFuture<DeliveryReport>
ProducerManagerImpl::postImpl(const std::string& topic,
                              K&& key,
                              P&& payload,
                              HEADERS&& headers,
                              void* opaque)
{
    auto it = _producers.find(topic);
    if (it == _producers.end()) {
        throw std::runtime_error("Invalid topic");
    }
    ProducerTopicEntry& topicEntry = it->second;
    if (topicEntry._payloadPolicy == Producer::PayloadPolicy::PASSTHROUGH_PAYLOAD) {
        throw std::runtime_error("Invalid async operation for pass-through payload policy - use send() instead.");
    }
    if (topicEntry._preserveMessageOrder && (topicEntry._producer->get_buffer_size() > topicEntry._maxQueueLength)) {
        throw std::runtime_error("Internal queue full");
    }
    quantum::Promise<DeliveryReport> deliveryPromise;
    // Post the serialization future and return
    {
        std::unique_lock<std::mutex> lock(_messageQueueMutex);
        _messageQueue.emplace_back(_dispatcher.post(
                serializeCoro<K,P,HEADERS>,
                topicEntry,
                std::forward<K>(key),
                std::forward<P>(payload),
                std::forward<HEADERS>(headers),
                new PackedOpaque(opaque, std::move(deliveryPromise))));
    }
    _emptyCondition.notify_one();
    // Get future
    auto ctx = quantum::local::context();
    if (ctx) {
        return {deliveryPromise.getICoroFuture(), ctx};
    }
    return deliveryPromise.getIThreadFuture();
}

template <typename K, typename P>
quantum::GenericFuture<DeliveryReport>
ProducerManagerImpl::post(const std::string& topic,
                          K&& key,
                          P&& payload,
                          const HeaderPack& headers,
                          void* opaque)
{
    return postImpl(topic, std::forward<K>(key), std::forward<P>(payload), headers, opaque);
}

template <typename K, typename P>
quantum::GenericFuture<DeliveryReport>
ProducerManagerImpl::post(const std::string& topic,
                          K&& key,
                          P&& payload,
                          HeaderPack&& headers,
                          void* opaque)
{
    return postImpl(topic, std::forward<K>(key), std::forward<P>(payload), std::move(headers), opaque);
}
 
}}

#endif //BLOOMBERG_COROKAFKA_PRODUCER_MANAGER_IMPL_H
