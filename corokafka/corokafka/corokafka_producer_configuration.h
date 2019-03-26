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
#ifndef BLOOMBERG_COROKAFKA_PRODUCER_CONFIGURATION_H
#define BLOOMBERG_COROKAFKA_PRODUCER_CONFIGURATION_H

#include <corokafka/corokafka_callbacks.h>
#include <corokafka/corokafka_utils.h>
#include <corokafka/corokafka_configuration.h>

namespace Bloomberg {
namespace corokafka {

//========================================================================
//                       PRODUCER CONFIGURATION
//========================================================================
/*
 * Kafka-connector specific configs
 * -----------------------------------------------------------------------
 * Description: These configuration options are additional to the librdkafka ones. Options
 *              pertaining to a producer start with 'internal.producer' and for topics start with 'internal.topic'.
 *              Default values are marked with a * or are in {}.
 *
 * internal.producer.timeout.ms: {1000} Sets the timeout on poll and flush operations.
 *
 * internal.producer.retries: {0} Sets the number of times to retry sending a message from the internal queue before
 * giving up. Note that this setting is independent of the rdkafka setting 'message.send.max.retries'. This setting
 * applies to each individual message and not to entire batches of messages which 'message.send.max.retries' does.
 *
 * internal.producer.payload.policy: {'passthrough',*'copy'}. Sets the payload policy on the producer.
 * For sync producers, use 'passthrough' for maximum performance.
 *
 * internal.producer.preserve.message.order: {'true',*'false'}. Set to 'true' if strict message order must be preserved.
 * When setting this, rdkafka option 'max.in.flight' will be set to 1 to avoid possible reordering of packets.
 * Messages will be buffered and sent sequentially, waiting for broker acks before the next one is processed.
 * This ensures delivery guarantee at the cost of performance. The internal buffer queue size can be
 * checked via ProducerMetadata::getInternalQueueLength().
 * An intermediate, solution with better performance would be to set this setting to 'false' and set 'max.in.flight=1'
 * which will skip internal buffering altogether. This will guarantee ordering but not delivery.
 *
 * internal.producer.max.queue.length: {10000} Maximum size of the internal queue when producing asynchronously
 * with strict order preservation.
 *
 * internal.producer.wait.for.acks: {'true',*'false'} If set to true, then the producer will wait up to the
 * timeout specified when producing a message via send() or post().
 *
 * internal.producer.wait.for.acks.timeout.ms: {0} Set the maximum timeout for the producer to wait for broker acks
 * when producing a message via send() or post(). Set to -1 for infinite timeout.
 *
 * internal.producer.flush.wait.for.acks: {'true',*'false'} If set to true, then the producer will wait up to the
 * timeout specified when flushing the internal queue.
 *
 * internal.producer.flush.wait.for.acks.timeout.ms: {0} Set the maximum timeout for the producer to wait for broker acks
 * when flushing the internal queue (see above). Set to -1 for infinite timeout.
 *
 * internal.producer.log.level: {'emergency','alert','critical','error','warning','notice',*'info','debug'}.
 * Sets the log level for this producer. Note that if the rdkafka 'debug' property is set, internal.producer.log.level
 * will be automatically adjusted to 'debug'.
 *
 * internal.producer.skip.unknown.headers: {*'true','false'}. If unknown headers are encountered (i.e. for which there
 * is no registered serializer), they will be skipped. If set to false, an error will be thrown.
 *
 * internal.producer.auto.throttle: {'true',*'false'}. When enabled, the producers will be automatically paused/resumed
 * when throttled by the 'throttle time x multiplier'.
 *
 * internal.producer.auto.throttle.multiplier: {1}. Change this value to pause the producer by
 * 'throttle time x multiplier' instead. This only works if 'internal.producer.auto.throttle=true'.
 *
 * internal.producer.queue.full.notification: {'edgeTriggered',*'oncePerMessage','eachOccurence'}.
 * When registering a QueueFullCallback, this setting determines when the callback will be raised. If set to
 * 'edgeTriggered', the callback will be raised once after which the application must reset the edge via
 * ProducerManager::resetQueueFullTrigger() in order to re-enable it again.
 *
 */
class ProducerConfiguration : public Configuration
{
    friend class Configuration;
public:
    /**
     * @brief Create a producer configuration.
     * @param topic The topic to which this configuration applies.
     * @param config The producer configuration options.
     * @param topicConfig The topic configuration options.
     */
    ProducerConfiguration(const std::string& topic,
                          Options config,
                          Options topicConfig);
    
    /**
     * @brief Create a producer configuration.
     * @param topic The topic to which this configuration applies.
     * @param config The producer configuration options.
     * @param topicConfig The topic configuration options.
     */
    ProducerConfiguration(const std::string& topic,
                          std::initializer_list<ConfigurationOption> config,
                          std::initializer_list<ConfigurationOption> topicConfig);
    
    using Configuration::setCallback;
    
    /**
     * @brief Set the delivery report callback.
     * @param callback The callback.
     */
    void setCallback(Callbacks::DeliveryReportCallback callback);
    
    /**
     * @brief Get the delivery report callback
     * @return The callback.
     */
    const Callbacks::DeliveryReportCallback& getDeliveryReportCallback() const;
    
    /**
     * @brief Set the partitioner callback.
     * @param callback The callback.
     * @remark A default hash partitioner is already supplied internally and as such using this callback is optional.
     */
    void setCallback(Callbacks::PartitionerCallback callback);
    
    /**
     * @brief Get the partitioner callback.
     * @return The callback.
     */
    const Callbacks::PartitionerCallback& getPartitionerCallback() const;
    
    /**
     * @brief Set the queue full callback.
     * @param callback The callback.
     */
    void setQueueFullCallback(Callbacks::QueueFullCallback callback);
    
    /**
     * @brief Get the queue full callback.
     * @return The callback.
     */
    const Callbacks::QueueFullCallback& getQueueFullCallback() const;
    
    /**
     * @brief Set the message key serializer callback.
     * @tparam T The key type.
     * @param callback The callback.
     * @remark Setting a key serializer callback is mandatory.
     */
    template <typename T>
    void setKeyCallback(Callbacks::SerializerCallback<T> callback)
    {
        _keySerializer.reset(new ConcreteSerializer<T>(std::move(callback)));
    }
    
    /**
     * @brief Set the payload serializer callback.
     * @tparam T The payload type.
     * @param callback The callback.
     * @remark Setting a payload serializer callback is mandatory.
     */
    template <typename T>
    void setPayloadCallback(Callbacks::SerializerCallback<T> callback)
    {
        _payloadSerializer.reset(new ConcreteSerializer<T>(std::move(callback)));
    }
    
    /**
     * @brief Set the header serializer callback.
     * @tparam T The header type.
     * @param name The name of the header.
     * @param callback The callback.
     */
    template <typename T>
    void setHeaderCallback(const std::string& name, Callbacks::SerializerCallback<T> callback)
    {
        _headerSerializers[name].reset(new ConcreteSerializer<T>(std::move(callback)));
    }
    
    /**
     * @brief Get the key serializer callback
     * @tparam T The key type.
     * @return The callback
     */
    template <typename T>
    const Callbacks::SerializerCallback<T>& getKeyCallback() const
    {
        return std::static_pointer_cast<ConcreteSerializer<T>>(_keySerializer)->getCallback();
    }
    
    /**
     * @brief Get the payload serializer callback
     * @tparam T The payload type.
     * @return The callback
     */
    template <typename T>
    const Callbacks::SerializerCallback<T>& getPayloadCallback() const
    {
        return std::static_pointer_cast<ConcreteSerializer<T>>(_payloadSerializer)->getCallback();
    }
    
    /**
     * @brief Get the header serializer callback
     * @tparam T The payload type.
     * @return The callback
     */
    template <typename T>
    const Callbacks::SerializerCallback<T>& getHeaderCallback(const std::string& name) const
    {
        return std::static_pointer_cast<ConcreteSerializer<T>>(_headerSerializers.at(name))->getCallback();
    }
    
    /**
     * @brief Get the Serializer functors.
     * @return The Serializer
     */
    const Serializer& getKeySerializer() const;
    const Serializer& getPayloadSerializer() const;
    const Serializer& getHeaderSerializer(const std::string& name) const;
    
private:
    using SerializerPtr = std::shared_ptr<Serializer>;
    
    Callbacks::DeliveryReportCallback           _deliveryReportCallback;
    Callbacks::PartitionerCallback              _partitionerCallback;
    Callbacks::QueueFullCallback                _queueFullCallback;
    SerializerPtr                               _keySerializer;
    SerializerPtr                               _payloadSerializer;
    std::map<std::string, SerializerPtr>        _headerSerializers;
    static const OptionSet                      s_internalOptions;
    static const OptionSet                      s_internalTopicOptions;
    static const std::string                    s_internalOptionsPrefix;
};

}}


#endif //BLOOMBERG_COROKAFKA_PRODUCER_CONFIGURATION_H
