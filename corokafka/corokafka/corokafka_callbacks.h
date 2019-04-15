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
#ifndef BLOOMBERG_COROKAFKA_CALLBACKS_H
#define BLOOMBERG_COROKAFKA_CALLBACKS_H

#include <corokafka/corokafka_utils.h>
#include <corokafka/corokafka_sent_message.h>
#include <corokafka/corokafka_serializer.h>
#include <corokafka/corokafka_deserializer.h>
#include <corokafka/corokafka_receiver.h>
#include <corokafka/corokafka_consumer_metadata.h>
#include <corokafka/corokafka_producer_metadata.h>

namespace Bloomberg {
namespace corokafka {

struct Callbacks {
    // =========================================== CONNECTOR ===========================================================
    using ConnectorLogCallback = std::function<void(LogLevel level,
                                                    const std::string& facility,
                                                    const std::string& message)>;
    
    // ===================================== CONSUMER & PRODUCER =======================================================
    // Note: If an error happens during serialization or deserialization, the following values will be passed in the
    //       'opaque' pointer *if* an error callback is registered. The 'error' field shall contain
    //       RD_KAFKA_RESP_ERR__VALUE_SERIALIZATION/RD_KAFKA_RESP_ERR__VALUE_DESERIALIZATION.
    //       Opaque values:
    //       Serialization => the same 'opaque' data which was passed-in when calling 'send()' or 'sendAsync()'.
    //       Deserialization => can be safely cast to a 'Message*' which contains the raw unpacked message.
    //       All other contexts => NULL
    using ErrorCallback = std::function<void(const Metadata& metadata,
                                             Error error,
                                             const std::string& reason,
                                             void* opaque)>;
    
    using ThrottleCallback = std::function<void(const Metadata& metadata,
                                                const std::string& broker_name,
                                                int32_t broker_id,
                                                std::chrono::milliseconds throttle_time)>;
    
    using LogCallback = std::function<void(const Metadata& metadata,
                                           LogLevel level,
                                           const std::string& facility,
                                           const std::string& message)>;
    
    using StatsCallback = std::function<void(const Metadata& metadata,
                                             const std::string& json)>;
    
    // ============================================== CONSUMER =========================================================
    using OffsetCommitCallback = std::function<void(const ConsumerMetadata& metadata,
                                                    Error error,
                                                    const TopicPartitionList& topicPartitions,
                                                    const void* opaque)>;
    
    using RebalanceCallback = std::function<void(const ConsumerMetadata& metadata,
                                                 Error error,
                                                 TopicPartitionList& topicPartitions)>;
    
    using PreprocessorCallback = std::function<bool(TopicPartition hint)>;
    
    template <typename T>
    using DeserializerCallback = typename ConcreteDeserializer<T>::Callback;
    
    template <typename K, typename P>
    using ReceiverCallback = typename ConcreteReceiver<K,P>::Callback;
    
    // ============================================== PRODUCER =========================================================
    using DeliveryReportCallback = std::function<void(const ProducerMetadata& metadata,
                                                      const SentMessage& message)>;
    
    using PartitionerCallback = std::function<int32_t(const ProducerMetadata& metadata,
                                                      const Buffer& key,
                                                      int32_t partition_count)>;
    
    using QueueFullCallback = std::function<void(const ProducerMetadata& metadata,
                                                 const SentMessage& message)>;
    
    template <typename T>
    using SerializerCallback = typename ConcreteSerializer<T>::Callback;
};

}}

#endif //BLOOMBERG_COROKAFKA_CALLBACKS_H
