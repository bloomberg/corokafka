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

#include <vector>
#include <map>
#include <chrono>
#include <future>
#include <corokafka/corokafka_utils.h>
#include <corokafka/corokafka_configuration_builder.h>
#include <corokafka/impl/corokafka_producer_manager_impl.h>
#include <corokafka/corokafka_message.h>
#include <corokafka/corokafka_delivery_report.h>
#include <corokafka/corokafka_header_pack.h>
#include <quantum/quantum.h>

namespace Bloomberg {
namespace corokafka {

/**
 * @brief The ProducerManager is the object through which producers send messages.
 */
class ProducerManager
{
public:
    /**
     * @brief Synchronous send. When this function returns, the message has been ack-ed by N broker replicas based on
     *        the broker settings for this topic and the final ack response has been received by this library.
     * @tparam TOPIC Type Topic<Key,Payload,Headers> which represents this producer.
     * @tparam K The Key type used in selecting the partition where this message will be sent.
     * @tparam P The Payload type for this message.
     * @param topic The topic to publish to.
     * @param key The message key.
     * @param payload The message payload.
     * @param headers The header pack for this message in order of definition in the TopicTraits.
     * @param opaque An opaque data pointer which will be returned inside the delivery callback.
     * @return The number of bytes sent.
     * @remark If the application uses *only* synchronous sends, better performance can be achieved by setting
     *         'internal.producer.payload.policy = passthrough', which will prevent the payload from being copied
     *         inside RdKafka.
     * @remark To guarantee strict message ordering, set 'internal.producer.preserve.message.order = true' which will
     *         also set the rdkafka option 'max.in.flight = 1' as it may cause re-ordering or packets.
     */
    template <typename TOPIC, typename K, typename P, typename ...H>
    int send(const TOPIC& topic,
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
     * @param key The message key.
     * @param payload The message payload.
     * @param headers The header pack for this message.
     * @param opaque An opaque data pointer which will be returned inside the delivery callback or the returned future.
     * @remark To guarantee strict message ordering, set 'internal.producer.preserve.message.order = true' which will
     *         also set the rdkafka option 'max.in.flight = 1' as it may cause re-ordering or packets.
     * @remark A message delivery can be tracked by registering a delivery report callback or by blocking on the
     *         returned future. Note that both these methods can be used jointly if needed.
     * @return A future containing a message delivery report.
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
     * @param timeout The maximum time to wait for. (==0 waits forever)
     */
    void waitForAcks(const std::string& topic,
                     std::chrono::milliseconds timeout = std::chrono::milliseconds::zero());
    
    /**
     * @brief Gracefully shut down all producers and wait until all buffered messages are sent.
     * @remark Note that this method is automatically called in the destructor.
     */
    void shutdown();
    
    /**
     * @brief Get Kafka metadata associated with this topic.
     * @param topic The topic to query.
     * @return The metadata object.
     */
    ProducerMetadata getMetadata(const std::string& topic);
    
    /**
     * @brief Get the configuration associated with this topic.
     * @param topic The topic.
     * @return A reference to the configuration.
     */
    const ProducerConfiguration& getConfiguration(const std::string& topic) const;
    
    /**
     * @brief Get all the managed topics.
     * @return The topic list.
     */
    std::vector<std::string> getTopics() const;
    
    /**
     * @brief In edgeTriggered mode, re-enable the queue full notification callback.
     * @param topic The topic for which to reset the callback.
     * @note This method only works if the application has previously registered a QueueFullCallback with this topic.
     */
    void resetQueueFullTrigger(const std::string& topic);
    
    /**
     * @brief Enables or disables parallel message fan-out to IO threads. Disabled by default.
     * @note When enabled messages are distributed among all IO threads on a per-topic basis.
     *       When disabled, messages are produced sequentially after serialization.
     * @note This setting only affects async producers (i.e. post() methods). This method can be set/reset anytime.
     * @warning This should be typically used when some producers get backed-up, in which case posting the
     *          messages sequentially would also impact other producers. The fan-out lets other producers
     *          work independently.
     */
     void enableMessageFanout(bool value);
    
protected:
    using ConfigMap = ConfigurationBuilder::ConfigMap<ProducerConfiguration>;
    
    ProducerManager(quantum::Dispatcher& dispatcher,
                    const ConnectorConfiguration& connectorConfiguration,
                    const ConfigMap& config);
    
    ProducerManager(quantum::Dispatcher& dispatcher,
                    const ConnectorConfiguration& connectorConfiguration,
                    ConfigMap&& config);
    
    virtual ~ProducerManager();
    
    void poll();
    void post();
    
private:
    std::unique_ptr<ProducerManagerImpl>  _impl;
};

// Implementations
template <typename TOPIC, typename K, typename P, typename ...H>
int
ProducerManager::send(const TOPIC& topic,
                      const void* opaque,
                      const K& key,
                      const P& payload,
                      const H&... headers) {
    static_assert(TOPIC::isSerializable(), "Topic contains types which are not serializable");
    static_assert(std::is_same<typename TOPIC::KeyType, std::decay_t<K>>::value, "Invalid key type");
    static_assert(std::is_same<typename TOPIC::PayloadType, std::decay_t<P>>::value, "Invalid payload type");
    static_assert(matchAllTypes<typename TOPIC::HeadersType::HeaderTypes, std::decay_t<H>...>(), "Invalid header types");
    return _impl->template send<TOPIC,K,P,H...>(topic, opaque, key, payload, headers...);
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
    return _impl->template post<TOPIC,K,P,H...>(
        topic,
        opaque,
        std::forward<K>(key),
        std::forward<P>(payload),
        std::forward<H>(headers)...);
}

}}

#endif //BLOOMBERG_COROKAFKA_PRODUCER_MANAGER_H
