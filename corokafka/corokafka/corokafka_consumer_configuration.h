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
#ifndef BLOOMBERG_COROKAFKA_CONSUMER_CONFIGURATION_H
#define BLOOMBERG_COROKAFKA_CONSUMER_CONFIGURATION_H

#include <corokafka/corokafka_callbacks.h>
#include <corokafka/corokafka_utils.h>
#include <corokafka/corokafka_configuration.h>

namespace Bloomberg {
namespace corokafka {

//========================================================================
//                       CONSUMER CONFIGURATION
//========================================================================
class ConsumerConfiguration : public Configuration
{
    friend class Configuration;
public:
    /**
     * @brief Create a consumer configuration.
     * @param topic The topic to which this configuration applies.
     * @param config The producer configuration options.
     * @param topicConfig The topic configuration options.
     */
    ConsumerConfiguration(const std::string& topic,
                          Options config,
                          Options topicConfig);
    
    /**
     * @brief Create a consumer configuration.
     * @param topic The topic to which this configuration applies.
     * @param config The producer configuration options.
     * @param topicConfig The topic configuration options.
     */
    ConsumerConfiguration(const std::string& topic,
                          std::initializer_list<ConfigurationOption> config,
                          std::initializer_list<ConfigurationOption> topicConfig);
    
    using Configuration::setCallback;
    
    /**
     * @brief Assign partitions and offsets on startup for this consumer.
     * @param strategy The strategy to use for this consumer.
     * @param partitions The partition list.
     * @remark When 'strategy == static', the partitions provided will be used in a call to librdkafka::rd_kafka_assign().
     *         When 'strategy == dynamic', the partition list *must* contain all partitions for this topic in order
     *         to cover any possible partition combinations assigned by Kafka. This setting will result in a call
     *         to librdkafka::rd_kafka_subscribe().
     */
    void assignInitialPartitions(PartitionStrategy strategy,
                                 TopicPartitionList partitions);
    
    /**
     * @brief Get the partition strategy used by this consumer.
     * @return The strategy.
     * @remark The default strategy is 'Dynamic' unless set otherwise via assignInitialPartitions().
     */
    PartitionStrategy getPartitionStrategy() const;
    
    /**
     * @brief Get the initial partition assignment.
     * @return The partition assignment.
     */
    const TopicPartitionList& getInitialPartitionAssignment() const;
    
    /**
     * @brief Set the offset commit callback.
     * @param callback The callback.
     */
    void setCallback(Callbacks::OffsetCommitCallback callback);
    
    /**
     * @brief Get the offset commit callback.
     * @return The callback.
     */
    const Callbacks::OffsetCommitCallback& getOffsetCommitCallback() const;
    
    /**
     * @brief Set the rebalance callback.
     * @param callback The callback.
     * @remark This library handles all partition assignments and revocations internally.
     *         As such, setting this callback is entirely optional and discretionary.
     */
    void setCallback(Callbacks::RebalanceCallback callback);
    
    /**
     * @brief Get the rebalance callback.
     * @return The callback.
     */
    const Callbacks::RebalanceCallback& getRebalanceCallback() const;
    
    /**
     * @brief Set the preprocessor callback. This will be called before a message is deserialized.
     * @param callback The callback.
     * @note The callback should return 'true' if the message should be skipped.
     */
    void setCallback(Callbacks::PreprocessorCallback callback);
    
    /**
     * @brief Get the preprocessor callback.
     * @return The callback.
     */
    const Callbacks::PreprocessorCallback& getPreprocessorCallback() const;
    
    /**
     * @brief Set the receiver callback.
     * @tparam K The Key type.
     * @tparam P The Payload type.
     * @param callback The callback.
     * @remark Setting a receiver callback is mandatory.
     */
    template <typename K, typename P>
    void setCallback(Callbacks::ReceiverCallback<K,P> callback)
    {
        _receiver.reset(new ConcreteReceiver<K,P>(std::move(callback)));
    }
    
    /**
     * @brief Get the receiver callback.
     * @tparam K The Key type.
     * @tparam P The Payload type.
     * @return The callback.
     */
    template <typename K, typename P>
    const Callbacks::ReceiverCallback<K,P>& getReceiverCallback() const
    {
        return std::static_pointer_cast<ConcreteReceiver<K,P>>(_receiver)->getCallback();
    }
    
    /**
     * @brief Get the Receiver functor.
     * @return The Receiver.
     */
    const Receiver& getReceiver() const;
    
    /**
     * @brief Set the key deserializer callback.
     * @tparam T The Key type.
     * @param callback The callback.
     * @remark Setting a key deserializer callback is mandatory.
     */
    template <typename T>
    void setKeyCallback(Callbacks::KeyDeserializerCallback<T> callback)
    {
        _keyDeserializer.reset(new ConcreteDeserializer<T>(std::move(callback)));
    }
    
    /**
     * @brief Set the payload deserializer callback.
     * @tparam T The Payload type.
     * @param callback The callback.
     * @remark Setting a payload deserializer callback is mandatory.
     */
    template <typename T>
    void setPayloadCallback(Callbacks::PayloadDeserializerCallback<T> callback)
    {
        _payloadDeserializer.reset(new ConcreteDeserializerWithHeaders<T>(std::move(callback)));
    }
    
    /**
     * @brief Set the payload deserializer callback.
     * @tparam T The Payload type.
     * @param name The header name.
     * @param callback The callback.
     * @remark Setting a payload deserializer callback is mandatory.
     */
    template <typename T>
    void setHeaderCallback(const std::string& name, Callbacks::HeaderDeserializerCallback<T> callback)
    {
        _headerDeserializers[name].reset(new ConcreteDeserializer<T>(std::move(callback)));
    }
    
    /**
     * @brief Get the key deserializer callback.
     * @tparam T The Key type.
     * @return The callback.
     */
    template <typename T>
    const Callbacks::KeyDeserializerCallback<T>& getKeyCallback() const
    {
        return std::static_pointer_cast<ConcreteDeserializer<T>>(_keyDeserializer)->getCallback();
    }
    
    /**
     * @brief Get the payload deserializer callback.
     * @tparam T The Key type.
     * @return The callback.
     */
    template <typename T>
    const Callbacks::PayloadDeserializerCallback<T>& getPayloadCallback() const
    {
        return std::static_pointer_cast<ConcreteDeserializer<T>>(_payloadDeserializer)->getCallback();
    }
    
    /**
     * @brief Get the specific header deserializer callback.
     * @tparam T The Header type.
     * @return The callback.
     */
    template <typename T>
    const Callbacks::HeaderDeserializerCallback<T>& getHeaderCallback(const std::string& name) const
    {
        return std::static_pointer_cast<ConcreteDeserializer<T>>(_headerDeserializers.at(name))->getCallback();
    }
    
    /**
     * @brief Get the Deserializer functors.
     * @return The functor.
     */
    const Deserializer& getKeyDeserializer() const;
    const Deserializer& getPayloadDeserializer() const;
    const Deserializer& getHeaderDeserializer(const std::string& name) const;
    
private:
    using DeserializerPtr = std::shared_ptr<Deserializer>;
    
    Callbacks::OffsetCommitCallback         _offsetCommitCallback;
    Callbacks::RebalanceCallback            _rebalanceCallback;
    Callbacks::PreprocessorCallback         _preprocessorCallback;
    DeserializerPtr                         _keyDeserializer;
    DeserializerPtr                         _payloadDeserializer;
    std::map<std::string, DeserializerPtr>  _headerDeserializers;
    std::shared_ptr<Receiver>               _receiver;
    TopicPartitionList                      _initialPartitionList;
    PartitionStrategy                       _strategy{PartitionStrategy::Dynamic};
    static const OptionSet                  s_internalOptions;
    static const OptionSet                  s_internalTopicOptions;
    static const std::string                s_internalOptionsPrefix;
};

}
}


#endif //BLOOMBERG_COROKAFKA_CONSUMER_CONFIGURATION_H
