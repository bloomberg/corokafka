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
#ifndef BLOOMBERG_COROKAFKA_PRODUCER_MANAGER_H
#define BLOOMBERG_COROKAFKA_PRODUCER_MANAGER_H

#include <corokafka/interface/corokafka_impl.h>
#include <corokafka/corokafka_utils.h>
#include <corokafka/corokafka_configuration_builder.h>
#include <corokafka/interface/corokafka_iproducer_manager.h>
#include <corokafka/impl/corokafka_producer_manager_impl.h>
#include <corokafka/interface/corokafka_imessage.h>
#include <corokafka/corokafka_delivery_report.h>
#include <corokafka/corokafka_header_pack.h>
#include <quantum/quantum.h>
#include <vector>
#include <map>
#include <chrono>
#include <future>

namespace Bloomberg {
namespace corokafka {

namespace mocks {
    struct ConnectorMock;
}

/**
 * @brief The ProducerManager is the object through which producers send messages.
 */
class ProducerManager : public Impl<IProducerManager>
{
public:
    /**
     * @brief Synchronous send. When this function returns, the message has been ack-ed by N broker replicas based on
     *        the broker settings for this topic and the final ack response has been received by this library.
     * @tparam TOPIC Type Topic<Key,Payload,Headers> which represents this producer.
     * @tparam K The Key type used in selecting the partition where this message will be sent.
     * @tparam P The Payload type for this message.
     * @param topic The topic to publish to.
     * @param opaque An opaque data pointer which will be returned inside the delivery callback or the returned future.
     * @param key The message key.
     * @param payload The message payload.
     * @param headers The header pack for this message in order of definition in the TopicTraits.
     * @return A message delivery report.
     * @remark This call will block for the duration of the 'internal.producer.wait.for.acks.timeout.ms'.
     *         Contrary to post(), this will create an IO operation on the internal dispatcher. Depending
     *         on poll settings and system load, better synchronous performance could be achieved by
     *         calling post().get() which won't overload the IO thread pool.
     * @remark If the application uses *only* synchronous sends, better performance can be achieved by setting
     *         'internal.producer.payload.policy = passthrough', which will prevent the payload from being copied
     *         inside RdKafka.
     * @remark To guarantee strict message ordering, set 'internal.producer.preserve.message.order = true' which will
     *         also set the RdKafka option 'max.in.flight = 1' and 'message.send.max.retries=0' as it
     *         may cause re-ordering or packets. Alternatively one can set 'enable.idempotence=true'.
     */
    template <typename TOPIC, typename K, typename P, typename ...H>
    DeliveryReport send(const TOPIC& topic,
                        const void* opaque,
                        const K& key,
                        const P& payload,
                        const H&...headers);
    
    /**
     * @brief Asynchronous send. No message delivery guarantee is made and messages are sent in batches unless
     *        strict ordering is needed.
     * @tparam TOPIC Type Topic<Key,Payload,Headers> which represents this producer.
     * @tparam K The Key type used in selecting the partition where this message will be sent.
     * @tparam P The Payload type for this message.
     * @param topic The topic to publish to.
     * @param opaque An opaque data pointer which will be returned inside the delivery callback or the returned future.
     * @param key The message key.
     * @param payload The message payload.
     * @param headers The header pack for this message.
     * @return A future containing a message delivery report.
     * @remark To guarantee strict message ordering, set 'internal.producer.preserve.message.order = true' which will
     *         also set the RdKafka option 'max.in.flight = 1' and 'message.send.max.retries=0' as it
     *         may cause re-ordering or packets. Alternatively one can set 'enable.idempotence=true'.
     * @remark A message delivery can be tracked by registering a delivery report callback or by blocking on the
     *         returned future. Note that both these methods can be used jointly if needed.
     * @warning This method will make an extra copy of the message.
     */
    template <typename TOPIC, typename K, typename P, typename ...H>
    quantum::GenericFuture<DeliveryReport>
    post(const TOPIC& topic,
         const void* opaque,
         K&& key,
         P&& payload,
         H&&...headers);
    
    /**
     * @brief Wait for all pending 'posted' messages to be ack-ed by the broker.
     * @param topic The topic to wait for.
     * @warning This function may throw. The time to wait is the internal producer specified time
     *          'internal.producer.wait.for.acks.timeout.ms' or if not specified,
     *          the 'internal.producer.timeout.ms'
     */
    bool waitForAcks(const std::string& topic) final;
    
    /**
     * @brief Wait for all pending 'posted' messages to be ack-ed by the broker.
     * @param topic The topic to wait for.
     * @param timeout The maximum time to wait for (-1 waits forever).
     * @returns true if succeeded, false if timed-out before all the acks arrived.
     * @warning This function may throw.
     */
    bool waitForAcks(const std::string& topic,
                     std::chrono::milliseconds timeout) final;
    
    /**
     * @brief Gracefully shut down all producers and wait until all buffered messages are sent.
     * @remark Note that this method is automatically called in the destructor.
     */
    void shutdown() final;
    
    /**
     * @brief Get Kafka metadata associated with this topic.
     * @param topic The topic to query.
     * @return The metadata object.
     */
    ProducerMetadata getMetadata(const std::string& topic) final;
    
    /**
     * @brief Get the configuration associated with this topic.
     * @param topic The topic.
     * @return A reference to the configuration.
     */
    const ProducerConfiguration& getConfiguration(const std::string& topic) const final;
    
    /**
     * @brief Get all the managed topics.
     * @return The topic list.
     */
    std::vector<std::string> getTopics() const final;
    
    /**
     * @brief In edgeTriggered mode, re-enable the queue full notification callback.
     * @param topic The topic for which to reset the callback.
     * @note This method only works if the application has previously registered a QueueFullCallback with this topic.
     */
    void resetQueueFullTrigger(const std::string& topic) final;
    
private:
    friend class ConnectorImpl;
    friend struct mocks::ConnectorMock;
    using ImplType = Impl<IProducerManager>;
    using ConfigMap = ConfigurationBuilder::ConfigMap<ProducerConfiguration>;
    
    ProducerManager(quantum::Dispatcher& dispatcher,
                    const ConnectorConfiguration& connectorConfiguration,
                    const ConfigMap& config,
                    std::atomic_bool& interrupt);
    
    ProducerManager(quantum::Dispatcher& dispatcher,
                    const ConnectorConfiguration& connectorConfiguration,
                    ConfigMap&& config,
                    std::atomic_bool& interrupt);
    
    /**
     * @brief For mocking only via dependency injection
     */
    using ImplType::ImplType;
    
    void poll() final;
    void pollEnd() final;
    
    //empty stubs
    DeliveryReport send() final;
    quantum::GenericFuture<DeliveryReport> post() final;
    
    //members
    ProducerManagerImpl*   _producerPtr{nullptr};
};

// Implementations
template <typename TOPIC, typename K, typename P, typename ...H>
DeliveryReport
ProducerManager::send(const TOPIC& topic,
                      const void* opaque,
                      const K& key,
                      const P& payload,
                      const H&... headers) {
    static_assert(TOPIC::isSerializable(), "Topic contains types which are not serializable");
    static_assert(std::is_same<typename TOPIC::KeyType, std::decay_t<K>>::value, "Invalid key type");
    static_assert(std::is_same<typename TOPIC::PayloadType, std::decay_t<P>>::value, "Invalid payload type");
    static_assert(matchAllTypes<typename TOPIC::HeadersType::HeaderTypes, std::decay_t<H>...>(), "Invalid header types");
    if (_producerPtr) {
        return _producerPtr->template send<TOPIC, K, P, H...>(
                topic,
                opaque,
                key,
                payload,
                headers...);
    }
    return impl()->send(); //call stub
}

template <typename TOPIC, typename K, typename P, typename ...H>
quantum::GenericFuture<DeliveryReport>
ProducerManager::post(const TOPIC& topic,
                      const void* opaque,
                      K&& key,
                      P&& payload,
                      H&&...headers) {
    static_assert(TOPIC::isSerializable(), "Topic contains types which are not serializable");
    static_assert(std::is_same<typename TOPIC::KeyType, std::decay_t<K>>::value, "Invalid key type");
    static_assert(std::is_same<typename TOPIC::PayloadType, std::decay_t<P>>::value, "Invalid payload type");
    static_assert(matchAllTypes<typename TOPIC::HeadersType::HeaderTypes, std::decay_t<H>...>(), "Invalid header types");
    if (_producerPtr) {
        return _producerPtr->template post<TOPIC, K, P, H...>(
                topic,
                opaque,
                std::forward<K>(key),
                std::forward<P>(payload),
                std::forward<H>(headers)...);
    }
    return impl()->post(); //call stub
}

}}

#endif //BLOOMBERG_COROKAFKA_PRODUCER_MANAGER_H
