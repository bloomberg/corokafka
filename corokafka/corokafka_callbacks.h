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
    /**
     * @brief Callback for logging connector-specific errors which are internal to CoroKafka.
     */
    using ConnectorLogCallback = std::function<void(cppkafka::LogLevel level,
                                                    const std::string& facility,
                                                    const std::string& message)>;
    
    // ===================================== CONSUMER & PRODUCER =======================================================
    /**
     * @brief Callback which is used by RdKafka to report errors. These errors can be internal to the RdKafka
     *        library or Kafka broker specific.
     * @note If the user registers an opaque data pointer while setting the error callback in the producer or consumer
     *       configurations, that opaque pointer shall **always** be passed when the callback gets invoked.
     *       Otherwise the opaque data shall contain the following values:
     *       - Producer: If not NULL, it shall contain the same opaque pointer which was passed-in when calling
     *                   'send()' or 'post()'.
     *       - Consumer: If not NULL, it can be safely cast to a 'cppkafka::Message*' which contains the
     *                   raw unpacked message.
     */
    using ErrorCallback = std::function<void(const Metadata& metadata,
                                             cppkafka::Error error,
                                             const std::string& reason,
                                             void* opaque)>;
    
    /**
     * @brief Callback which notifies the application when broker throttling occurs. This is purely
     *        informational and no action is required from the application side if
     *        'internal.consumer.auto.throttle=true' or 'internal.producer.auto.throttle=true'.
     */
    using ThrottleCallback = std::function<void(const Metadata& metadata,
                                                const std::string& brokerName,
                                                int32_t brokerId,
                                                std::chrono::milliseconds throttleTime)>;
    
    /**
     * @brief Callback used by RdKafka to log events. A difference with the ConnectorLogCallback is that this
     *        one also contains metadata pertaining to the consumer or producer which is being logged.
     */
    using LogCallback = std::function<void(const Metadata& metadata,
                                           cppkafka::LogLevel level,
                                           const std::string& facility,
                                           const std::string& message)>;
    
    /**
     * @brief Callback for publishing RdKafka message statistics. This can be enabled via 'statistics.interval.ms'.
     */
    using StatsCallback = std::function<void(const Metadata& metadata,
                                             const std::string& json)>;
    
    // ============================================== CONSUMER =========================================================
    /**
     * @brief Callback to notify the application when offsets have been committed by the broker for the specified
     *        partitions. After receiving this callback it is safe to assume that a message has been fully processed.
     */
    using OffsetCommitCallback = std::function<void(const ConsumerMetadata& metadata,
                                                    cppkafka::Error error,
                                                    const cppkafka::TopicPartitionList& topicPartitions,
                                                    const std::vector<void*>& opaques)>;
    
    /**
     * @brief Callback to be used for dynamic consumers only. This allows the application to be notified of
     *        partition re-assignments. The 'error' parameter will indicate if the partition(s) are being
     *        revoked or assigned or if there's some other error present.
     */
    using RebalanceCallback = std::function<void(const ConsumerMetadata& metadata,
                                                 cppkafka::Error error,
                                                 cppkafka::TopicPartitionList& topicPartitions)>;
    
    /**
     * @brief The application can register this callback to be notified **before** a message is actually
     *        de-serialized. This can be useful for filtering raw messages or tracking poison messages.
     *        If the callback returns 'true' the message will be processed as usual. Otherwise the message
     *        will not be de-serialized and will be flagged to be skipped (accessed via 'ReceivedMessage::skip()').
     */
    using PreprocessorCallback = std::function<bool(const cppkafka::Message& rawMessage)>;
    
    /**
     * @brief Callback for receiving messages. Registering this callback is mandatory.
     */
    template <typename TOPIC>
    using ReceiverCallback = std::function<void(typename TOPIC::ReceivedMessageType)>;
    
    // ============================================== PRODUCER =========================================================
    /**
     * @brief This callback allows a producer to determine when a message has been received and processed by
     *        the Kafka brokers. Upon receipt of this callback, the application may assume the message is
     *        safely stored on the server and ready to be picked up by consumers.
     */
    using DeliveryReportCallback = std::function<void(const ProducerMetadata& metadata,
                                                      const SentMessage& message)>;
    
    /**
     * @brief This callback allows producers to alter the behavior of the default RdKafka partitioning
     *        mechanism which is a hashing function performed over the entire length of the key (in bytes).
     *        This callback shall return the partition to use for a specific key.
     */
    using PartitionerCallback = std::function<int32_t(const ProducerMetadata& metadata,
                                                      const cppkafka::Buffer& key,
                                                      int32_t partitionCount)>;
    
    /**
     * @brief This callback tells the producer when the internal message buffer is full. At this point
     *        the producer should temporarily suspend production as there may be some delay in transmission.
     */
    using QueueFullCallback = std::function<void(const ProducerMetadata& metadata,
                                                 const SentMessage& message)>;
};

}}

#endif //BLOOMBERG_COROKAFKA_CALLBACKS_H
