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
    _topic(topic),
    _handle(handle),
    _kafkaTopic(cppkafka::Topic::make_non_owning(kafkaTopic.get_handle()))
{
}

Metadata::operator bool() const
{
    return _handle != nullptr;
}

uint64_t Metadata::getHandle() const
{
    return _handle == nullptr ? 0 : (uint64_t)_handle->get_handle();
}

const std::string& Metadata::getTopic() const
{
    return _topic;
}

const cppkafka::Topic& Metadata::getTopicObject() const
{
    if (!_handle) {
        throw HandleException("Null");
    }
    if (!_kafkaTopic) {
        _kafkaTopic = _handle->get_topic(_topic);
    }
    return _kafkaTopic;
}

Metadata::OffsetWatermarkList Metadata::queryOffsetWatermarks() const
{
    if (!_handle) {
        throw HandleException("Null");
    }
    return queryOffsetWatermarks(_handle->get_timeout());
}

cppkafka::TopicPartitionList Metadata::queryOffsetsAtTime(Timestamp timestamp) const
{
    if (!_handle) {
        throw HandleException("Null");
    }
    return queryOffsetsAtTime(timestamp, _handle->get_timeout());
}

cppkafka::TopicMetadata Metadata::getTopicMetadata() const
{
    if (!_handle) {
        throw HandleException("Null");
    }
    return _handle->get_metadata(getTopicObject());
}

cppkafka::TopicMetadata Metadata::getTopicMetadata(std::chrono::milliseconds timeout) const
{
    if (!_handle) {
        throw HandleException("Null");
    }
    return _handle->get_metadata(getTopicObject(), timeout);
}

std::string Metadata::getInternalName() const
{
    if (!_handle) {
        throw HandleException("Null");
    }
    return _handle->get_name();
}

bool Metadata::isPartitionAvailable(int partition) const
{
    return getTopicObject().is_partition_available(partition);
}

}
}
