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

namespace Bloomberg {
namespace corokafka {

//=============================================================================
//                             CONSUMER METADATA
//=============================================================================

ConsumerMetadata::ConsumerMetadata(const std::string& topic,
                                   cppkafka::Consumer* handle,
                                   PartitionStrategy strategy) :
    Metadata(topic, cppkafka::Topic(), handle),
    _strategy(strategy)
{
}

ConsumerMetadata::ConsumerMetadata(const std::string& topic,
                                   const cppkafka::Topic& kafkaTopic,
                                   cppkafka::Consumer* handle,
                                   PartitionStrategy strategy) :
    Metadata(topic, kafkaTopic, handle),
    _strategy(strategy)
{
}

Metadata::OffsetWatermarkList ConsumerMetadata::queryOffsetWatermarks() const
{
    if (!_handle) {
        throw std::runtime_error("Null consumer");
    }
    OffsetWatermarkList offsets;
    for (const auto& partition : getPartitionAssignment()) {
        offsets.emplace_back(partition.get_partition(),
                             _handle->query_offsets(partition));
    }
    return offsets;
}

Metadata::OffsetWatermarkList ConsumerMetadata::getOffsetWatermarks() const
{
    if (!_handle) {
        throw std::runtime_error("Null consumer");
    }
    OffsetWatermarkList offsets;
    for (const auto& partition : getPartitionAssignment()) {
        offsets.emplace_back(partition.get_partition(),
                             static_cast<const cppkafka::Consumer*>(_handle)->get_offsets(partition));
    }
    return offsets;
}

cppkafka::TopicPartitionList ConsumerMetadata::queryOffsetsAtTime(Metadata::Timestamp timestamp) const
{
    if (!_handle) {
        throw std::runtime_error("Null consumer");
    }
    cppkafka::KafkaHandleBase::TopicPartitionsTimestampsMap timestampMap;
    std::chrono::milliseconds epochTime = timestamp.time_since_epoch();
    for (const auto& partition : getPartitionAssignment()) {
        timestampMap[partition] = epochTime;
    }
    return _handle->get_offsets_for_times(timestampMap);
}

cppkafka::TopicPartitionList ConsumerMetadata::queryCommittedOffsets() const
{
    if (!_handle) {
        throw std::runtime_error("Null consumer");
    }
    return static_cast<const cppkafka::Consumer*>(_handle)->get_offsets_committed(getPartitionAssignment());
}

cppkafka::TopicPartitionList ConsumerMetadata::getOffsetPositions() const
{
    if (!_handle) {
        throw std::runtime_error("Null consumer");
    }
    return static_cast<const cppkafka::Consumer*>(_handle)->get_offsets_position(getPartitionAssignment());
}

const cppkafka::TopicPartitionList& ConsumerMetadata::getPartitionAssignment() const
{
    if (!_handle) {
        throw std::runtime_error("Null consumer");
    }
    if (_partitions.empty()) {
        _partitions = static_cast<const cppkafka::Consumer*>(_handle)->get_assignment();
    }
    return _partitions;
}

cppkafka::GroupInformation ConsumerMetadata::getGroupInformation() const
{
    if (!_handle) {
        throw std::runtime_error("Null consumer");
    }
    return _handle->get_consumer_group(static_cast<const cppkafka::Consumer*>(_handle)->get_member_id());
}

PartitionStrategy ConsumerMetadata::getPartitionStrategy() const
{
    return _strategy;
}

}
}
