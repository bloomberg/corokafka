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
#include <corokafka/corokafka_consumer_metadata.h>
#include <corokafka/impl/corokafka_consumer_metadata_impl.h>
#include <corokafka/corokafka_exception.h>

namespace Bloomberg {
namespace corokafka {

//=============================================================================
//                             CONSUMER METADATA
//=============================================================================

ConsumerMetadata::ConsumerMetadata(const std::string& topic,
                                   cppkafka::Consumer* handle,
                                   PartitionStrategy strategy,
                                   std::chrono::milliseconds brokerTimeout) :
    ImplType(std::make_shared<ConsumerMetadataImpl>(topic, handle, strategy, brokerTimeout)),
    Metadata(std::static_pointer_cast<ConsumerMetadataImpl>(ImplType::impl()))
{
}

ConsumerMetadata::ConsumerMetadata(const std::string& topic,
                                   const cppkafka::Topic& kafkaTopic,
                                   cppkafka::Consumer* handle,
                                   PartitionStrategy strategy,
                                   std::chrono::milliseconds brokerTimeout) :
    ImplType(std::make_shared<ConsumerMetadataImpl>(topic, kafkaTopic, handle, strategy, brokerTimeout)),
    Metadata(std::static_pointer_cast<ConsumerMetadataImpl>(ImplType::impl()))
{
}

ConsumerMetadata::ConsumerMetadata(std::shared_ptr<IConsumerMetadata> impl) :
    ImplType(std::move(impl)),
    Metadata(std::static_pointer_cast<ConsumerMetadataImpl>(ImplType::impl()))
{
}

OffsetWatermarkList ConsumerMetadata::getOffsetWatermarks() const
{
    return ImplType::impl()->getOffsetWatermarks();
}

cppkafka::TopicPartitionList ConsumerMetadata::queryCommittedOffsets() const
{
    return ImplType::impl()->queryCommittedOffsets();
}

cppkafka::TopicPartitionList ConsumerMetadata::queryCommittedOffsets(std::chrono::milliseconds timeout) const
{
    return ImplType::impl()->queryCommittedOffsets(timeout);
}

cppkafka::TopicPartitionList ConsumerMetadata::getOffsetPositions() const
{
    return ImplType::impl()->getOffsetPositions();
}

const cppkafka::TopicPartitionList& ConsumerMetadata::getPartitionAssignment() const
{
    return ImplType::impl()->getPartitionAssignment();
}

cppkafka::GroupInformation ConsumerMetadata::getGroupInformation() const
{
    return ImplType::impl()->getGroupInformation();
}

cppkafka::GroupInformation ConsumerMetadata::getGroupInformation(std::chrono::milliseconds timeout) const
{
    return ImplType::impl()->getGroupInformation(timeout);
}

cppkafka::GroupInformationList ConsumerMetadata::getAllGroupsInformation() const
{
    return ImplType::impl()->getAllGroupsInformation();
}

cppkafka::GroupInformationList ConsumerMetadata::getAllGroupsInformation(std::chrono::milliseconds timeout) const
{
    return ImplType::impl()->getAllGroupsInformation(timeout);
}

PartitionStrategy ConsumerMetadata::getPartitionStrategy() const
{
    return ImplType::impl()->getPartitionStrategy();
}

}
}
