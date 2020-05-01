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
#include <corokafka/corokafka_metadata.h>
#include <corokafka/impl/corokafka_metadata_impl.h>
#include <corokafka/corokafka_exception.h>

namespace Bloomberg {
namespace corokafka {

//=============================================================================
//                                METADATA
//=============================================================================
// Constructor
Metadata::Metadata(const std::string& topic,
                   const cppkafka::Topic& kafkaTopic,
                   cppkafka::KafkaHandleBase* handle) :
    VirtualImpl(std::make_shared<MetadataImpl>(topic, kafkaTopic, handle))
{
}

KafkaType Metadata::getType() const
{
    return impl()->getType();
}

Metadata::operator bool() const
{
    return impl()->operator bool();
}

uint64_t Metadata::getHandle() const
{
    return impl()->getHandle();
}

const std::string& Metadata::getTopic() const
{
    return impl()->getTopic();
}

const cppkafka::Topic& Metadata::getTopicObject() const
{
    return impl()->getTopicObject();
}

OffsetWatermarkList Metadata::queryOffsetWatermarks() const
{
    return impl()->queryOffsetWatermarks();
}

OffsetWatermarkList Metadata::queryOffsetWatermarks(std::chrono::milliseconds timeout) const
{
    return impl()->queryOffsetWatermarks(timeout);
}

cppkafka::TopicPartitionList Metadata::queryOffsetsAtTime(Timestamp timestamp) const
{
    return impl()->queryOffsetsAtTime(timestamp);
}

cppkafka::TopicPartitionList Metadata::queryOffsetsAtTime(Timestamp timestamp,
                                                          std::chrono::milliseconds timeout) const
{
    return impl()->queryOffsetsAtTime(timestamp, timeout);
}

cppkafka::TopicMetadata Metadata::getTopicMetadata() const
{
    return impl()->getTopicMetadata();
}

cppkafka::TopicMetadata Metadata::getTopicMetadata(std::chrono::milliseconds timeout) const
{
    return impl()->getTopicMetadata(timeout);
}

std::string Metadata::getInternalName() const
{
    return impl()->getInternalName();
}

bool Metadata::isPartitionAvailable(int partition) const
{
    return impl()->isPartitionAvailable(partition);
}

}
}
