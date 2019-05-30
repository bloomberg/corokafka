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

namespace Bloomberg {
namespace corokafka {

//=============================================================================
//                                METADATA
//=============================================================================
// Constructor
Metadata::Metadata(const std::string& topic,
                   const Topic& kafkaTopic,
                   KafkaHandleBase* handle) :
    _topic(topic),
    _handle(handle),
    _kafkaTopic(Topic::make_non_owning(kafkaTopic.get_handle()))
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

const Topic& Metadata::getTopicObject() const
{
    if (!_handle) {
        throw std::runtime_error("Null handle");
    }
    if (!_kafkaTopic) {
        _kafkaTopic = _handle->get_topic(_topic);
    }
    return _kafkaTopic;
}

TopicMetadata Metadata::getTopicMetadata() const
{
    if (!_handle) {
        throw std::runtime_error("Null handle");
    }
    return _handle->get_metadata(getTopicObject());
}

std::string Metadata::getInternalName() const
{
    if (!_handle) {
        throw std::runtime_error("Null handle");
    }
    return _handle->get_name();
}

bool Metadata::isPartitionAvailable(int partition) const
{
    return getTopicObject().is_partition_available(partition);
}

}
}
