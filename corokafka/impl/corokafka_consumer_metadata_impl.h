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
#ifndef BLOOMBERG_COROKAFKA_CONSUMER_METADATA_IMPL_H
#define BLOOMBERG_COROKAFKA_CONSUMER_METADATA_IMPL_H

#include <corokafka/interface/corokafka_iconsumer_metadata.h>
#include <corokafka/impl/corokafka_metadata_impl.h>

namespace Bloomberg {
namespace corokafka {

class ConsumerMetadataImpl : public IConsumerMetadata,
                             public MetadataImpl
{
public:
    ConsumerMetadataImpl(const std::string& topic,
                         cppkafka::Consumer* handle,
                         PartitionStrategy strategy,
                         std::chrono::milliseconds brokerTimeout);
    
    ConsumerMetadataImpl(const std::string& topic,
                         const cppkafka::Topic& kafkaTopic,
                         cppkafka::Consumer* handle,
                         PartitionStrategy strategy,
                         std::chrono::milliseconds brokerTimeout);
    
    KafkaType getType() const final;

    using MetadataImpl::queryOffsetWatermarks;
    OffsetWatermarkList queryOffsetWatermarks(std::chrono::milliseconds timeout) const final;

    OffsetWatermarkList getOffsetWatermarks() const final;

    using MetadataImpl::queryOffsetsAtTime;
    cppkafka::TopicPartitionList queryOffsetsAtTime(Timestamp timestamp,
                                                    std::chrono::milliseconds timeout) const final;

    cppkafka::TopicPartitionList queryCommittedOffsets() const final;
    
    cppkafka::TopicPartitionList queryCommittedOffsets(std::chrono::milliseconds timeout) const final;
    
    cppkafka::TopicPartitionList getOffsetPositions() const final;

    const cppkafka::TopicPartitionList& getPartitionAssignment() const final;

    cppkafka::GroupInformation getGroupInformation() const final;
    
    cppkafka::GroupInformation getGroupInformation(std::chrono::milliseconds timeout) const final;
    
    cppkafka::GroupInformationList getAllGroupsInformation() const final;
    
    cppkafka::GroupInformationList getAllGroupsInformation(std::chrono::milliseconds timeout) const final;
    
    PartitionStrategy getPartitionStrategy() const final;
    
private:
    PartitionStrategy _strategy;
};

}
}

#endif //BLOOMBERG_COROKAFKA_CONSUMER_METADATA_IMPL_H
