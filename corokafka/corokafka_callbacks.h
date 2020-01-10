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
#include <corokafka/corokafka_deserializer.h>
#include <corokafka/corokafka_receiver.h>
#include <corokafka/corokafka_consumer_metadata.h>
#include <corokafka/corokafka_producer_metadata.h>

namespace Bloomberg {
namespace corokafka {

struct Callbacks {
    // =========================================== CONNECTOR ===========================================================
    using ConnectorLogCallback = std::function<void(cppkafka::LogLevel level,
                                                    const std::string& facility,
                                                    const std::string& message)>;
    
    // ===================================== CONSUMER & PRODUCER =======================================================
    // Note: If the user registers an opaque data pointer while setting the error callback in the producer or consumer
    //       configurations, that opaque data shall **always** passed when the callback gets invoked.
    //       Otherwise the opaque data shall contain the following values:
    //       - Producer: If not NULL, it shall contain the same opaque pointer which was passed-in when calling
    //                   'send()' or 'post()'.
    //       - Consumer: If not NULL, it can be safely cast to a 'cppkafka::Message*' which contains the
    //                   raw unpacked message.
    using ErrorCallback = std::function<void(const Metadata& metadata,
                                             cppkafka::Error error,
                                             const std::string& reason,
                                             void* opaque)>;
    
    using ThrottleCallback = std::function<void(const Metadata& metadata,
                                                const std::string& broker_name,
                                                int32_t broker_id,
                                                std::chrono::milliseconds throttle_time)>;
    
    using LogCallback = std::function<void(const Metadata& metadata,
                                           cppkafka::LogLevel level,
                                           const std::string& facility,
                                           const std::string& message)>;
    
    using StatsCallback = std::function<void(const Metadata& metadata,
                                             const std::string& json)>;
    
    // ============================================== CONSUMER =========================================================
    using OffsetCommitCallback = std::function<void(const ConsumerMetadata& metadata,
                                                    cppkafka::Error error,
                                                    const cppkafka::TopicPartitionList& topicPartitions,
                                                    const std::vector<void*>& opaques)>;
    
    using RebalanceCallback = std::function<void(const ConsumerMetadata& metadata,
                                                 cppkafka::Error error,
                                                 cppkafka::TopicPartitionList& topicPartitions)>;
    
    using PreprocessorCallback = std::function<bool(const cppkafka::TopicPartition& hint)>;
    
    template <typename TOPIC>
    using ReceiverCallback = std::function<void(typename TOPIC::ReceivedMessageType)>;
    
    // ============================================== PRODUCER =========================================================
    using DeliveryReportCallback = std::function<void(const ProducerMetadata& metadata,
                                                      const SentMessage& message)>;
    
    using PartitionerCallback = std::function<int32_t(const ProducerMetadata& metadata,
                                                      const cppkafka::Buffer& key,
                                                      int32_t partition_count)>;
    
    using QueueFullCallback = std::function<void(const ProducerMetadata& metadata,
                                                 const SentMessage& message)>;
};

}}

#endif //BLOOMBERG_COROKAFKA_CALLBACKS_H
