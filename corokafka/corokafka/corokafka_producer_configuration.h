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
class ProducerConfiguration : public Configuration
{
    friend class Configuration;
public:
    /**
     * @brief Create a producer configuration.
     * @param topic The topic to which this configuration applies.
     * @param options The producer configuration options.
     * @param topicOptions The topic configuration options.
     */
    ProducerConfiguration(const std::string& topic,
                          Options options,
                          Options topicOptions);
    
    /**
     * @brief Create a producer configuration.
     * @param topic The topic to which this configuration applies.
     * @param options The producer configuration options.
     * @param topicOptions The topic configuration options.
     */
    ProducerConfiguration(const std::string& topic,
                          std::initializer_list<ConfigurationOption> options,
                          std::initializer_list<ConfigurationOption> topicOptions);
    
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
    void setKeyCallback(Callbacks::KeySerializerCallback<T> callback)
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
    void setPayloadCallback(Callbacks::PayloadSerializerCallback<T> callback)
    {
        _payloadSerializer.reset(new ConcreteSerializerWithHeaders<T>(std::move(callback)));
    }
    
    /**
     * @brief Set the header serializer callback.
     * @tparam T The header type.
     * @param name The name of the header.
     * @param callback The callback.
     */
    template <typename T>
    void setHeaderCallback(const std::string& name, Callbacks::HeaderSerializerCallback<T> callback)
    {
        _headerSerializers[name].reset(new ConcreteSerializer<T>(std::move(callback)));
    }
    
    /**
     * @brief Get the key serializer callback
     * @tparam T The key type.
     * @return The callback
     */
    template <typename T>
    const Callbacks::KeySerializerCallback<T>& getKeyCallback() const
    {
        return std::static_pointer_cast<ConcreteSerializer<T>>(_keySerializer)->getCallback();
    }
    
    /**
     * @brief Get the payload serializer callback
     * @tparam T The payload type.
     * @return The callback
     */
    template <typename T>
    const Callbacks::PayloadSerializerCallback<T>& getPayloadCallback() const
    {
        return std::static_pointer_cast<ConcreteSerializer<T>>(_payloadSerializer)->getCallback();
    }
    
    /**
     * @brief Get the header serializer callback
     * @tparam T The payload type.
     * @return The callback
     */
    template <typename T>
    const Callbacks::HeaderSerializerCallback<T>& getHeaderCallback(const std::string& name) const
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
