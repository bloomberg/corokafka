/*
** Copyright 2020 Bloomberg Finance L.P.
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
#ifndef BLOOMBERG_COROKAFKA_PRODUCER_METADATA_IMPL_H
#define BLOOMBERG_COROKAFKA_PRODUCER_METADATA_IMPL_H

#include <corokafka/interface/corokafka_iproducer_metadata.h>
#include <corokafka/impl/corokafka_metadata_impl.h>
#include <corokafka/interface/corokafka_imessage.h>
#include <cppkafka/utils/buffered_producer.h>

namespace Bloomberg {
namespace corokafka {

class ProducerMetadataImpl : public IProducerMetadata,
                             public MetadataImpl
{
public:
    ProducerMetadataImpl(const std::string& topic,
                         cppkafka::BufferedProducer<ByteArray>* producer);
        
    ProducerMetadataImpl(const std::string& topic,
                         const cppkafka::Topic& kafkaTopic,
                         cppkafka::BufferedProducer<ByteArray>* producer);

    KafkaType getType() const final;

    const cppkafka::TopicPartitionList& getTopicPartitions() const final;

    using MetadataImpl::queryOffsetWatermarks;
    OffsetWatermarkList queryOffsetWatermarks(std::chrono::milliseconds timeout) const final;

    using MetadataImpl::queryOffsetsAtTime;
    cppkafka::TopicPartitionList queryOffsetsAtTime(Timestamp timestamp,
                                                    std::chrono::milliseconds timeout) const final;
    size_t getOutboundQueueLength() const final;

    size_t getInternalQueueLength() const final;
    
private:
    //Members
    cppkafka::BufferedProducer<ByteArray>* _bufferedProducer;
};

}}

#endif //BLOOMBERG_COROKAFKA_PRODUCER_METADATA_IMPL_H
