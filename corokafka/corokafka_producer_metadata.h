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
#ifndef BLOOMBERG_COROKAFKA_PRODUCER_METADATA_H
#define BLOOMBERG_COROKAFKA_PRODUCER_METADATA_H

#include <corokafka/corokafka_metadata.h>
#include <corokafka/corokafka_message.h>
#ifndef COROKAFKA_DO_NOT_USE_MODIFIED_CPPKAFKA
#include <corokafka/third_party/cppkafka/buffered_producer.h>
#endif

namespace Bloomberg {
namespace corokafka {

//=============================================================================
//                            PRODUCER METADATA
//=============================================================================

class ProducerMetadata : public Metadata
{
    friend class ProducerManagerImpl;
public:
    /**
     * @sa Metadata::getType
     */
    KafkaType getType() const final { return KafkaType::Producer; }
    /**
     * @brief Get all partitions belonging to this topic.
     * @return The partition list.
     */
    const cppkafka::TopicPartitionList& getTopicPartitions() const;
    /**
     * @sa Metadata::queryOffsetWatermarks
     */
    using Metadata::queryOffsetWatermarks;
    OffsetWatermarkList queryOffsetWatermarks(std::chrono::milliseconds timeout) const final;
    /**
     * @sa Metadata::queryOffsetsAtTime
     */
    using Metadata::queryOffsetsAtTime;
    cppkafka::TopicPartitionList queryOffsetsAtTime(Timestamp timestamp,
                                                    std::chrono::milliseconds timeout) const final;
    /**
     * @brief Get the length of the outbound RdKafka queue for this producer.
     * @return The queue length.
     */
    size_t getOutboundQueueLength() const;
    /**
     * @brief Get the current length of the internal corokafka queue for outbound messages.
     * @return The buffer length.
     * @note The maximum length of this queue is set via 'internal.producer.max.queue.length'.
     * @note This queue is only used for async production.
     */
    size_t getInternalQueueLength() const;
private:
    ProducerMetadata(const std::string& topic,
                     cppkafka::BufferedProducer<ByteArray>* producer);
    ProducerMetadata(const std::string& topic,
                     const cppkafka::Topic& kafkaTopic,
                     cppkafka::BufferedProducer<ByteArray>* producer);
    
    //Members
    cppkafka::BufferedProducer<ByteArray>* _bufferedProducer;
};

}}


#endif //BLOOMBERG_COROKAFKA_PRODUCER_METADATA_H
