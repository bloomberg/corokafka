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
#include <corokafka/impl/corokafka_consumer_metadata_impl.h>
#include <corokafka/corokafka_exception.h>

namespace Bloomberg {
namespace corokafka {

//=============================================================================
//                             CONSUMER METADATA
//=============================================================================

ConsumerMetadataImpl::ConsumerMetadataImpl(const std::string& topic,
                                           cppkafka::Consumer* handle,
                                           PartitionStrategy strategy) :
    MetadataImpl(topic, cppkafka::Topic(), handle),
    _strategy(strategy)
{
}

ConsumerMetadataImpl::ConsumerMetadataImpl(const std::string& topic,
                                           const cppkafka::Topic& kafkaTopic,
                                           cppkafka::Consumer* handle,
                                           PartitionStrategy strategy) :
    MetadataImpl(topic, kafkaTopic, handle),
    _strategy(strategy)
{
}

KafkaType ConsumerMetadataImpl::getType() const
{
    return KafkaType::Consumer;
}

OffsetWatermarkList ConsumerMetadataImpl::queryOffsetWatermarks(std::chrono::milliseconds timeout) const
{
    if (!_handle) {
        throw HandleException("Null consumer");
    }
    OffsetWatermarkList offsets;
    for (const auto& partition : getPartitionAssignment()) {
        offsets.emplace_back(partition.get_partition(),
                             _handle->query_offsets(partition, timeout));
    }
    return offsets;
}

OffsetWatermarkList ConsumerMetadataImpl::getOffsetWatermarks() const
{
    if (!_handle) {
        throw HandleException("Null consumer");
    }
    OffsetWatermarkList offsets;
    for (const auto& partition : getPartitionAssignment()) {
        offsets.emplace_back(partition.get_partition(),
                             static_cast<const cppkafka::Consumer*>(_handle)->get_offsets(partition));
    }
    return offsets;
}

cppkafka::TopicPartitionList ConsumerMetadataImpl::queryOffsetsAtTime(IMetadata::Timestamp timestamp,
                                                                      std::chrono::milliseconds timeout) const
{
    if (!_handle) {
        throw HandleException("Null consumer");
    }
    cppkafka::KafkaHandleBase::TopicPartitionsTimestampsMap timestampMap;
    std::chrono::milliseconds epochTime = timestamp.time_since_epoch();
    for (const auto& partition : getPartitionAssignment()) {
        timestampMap[partition] = epochTime;
    }
    return _handle->get_offsets_for_times(timestampMap, timeout);
}

cppkafka::TopicPartitionList ConsumerMetadataImpl::queryCommittedOffsets() const
{
    if (!_handle) {
        throw HandleException("Null consumer");
    }
    return static_cast<const cppkafka::Consumer*>(_handle)->get_offsets_committed(getPartitionAssignment());
}

cppkafka::TopicPartitionList ConsumerMetadataImpl::queryCommittedOffsets(std::chrono::milliseconds timeout) const
{
    if (!_handle) {
        throw HandleException("Null consumer");
    }
    return static_cast<const cppkafka::Consumer*>(_handle)->get_offsets_committed(getPartitionAssignment(), timeout);
}

cppkafka::TopicPartitionList ConsumerMetadataImpl::getOffsetPositions() const
{
    if (!_handle) {
        throw HandleException("Null consumer");
    }
    return static_cast<const cppkafka::Consumer*>(_handle)->get_offsets_position(getPartitionAssignment());
}

const cppkafka::TopicPartitionList& ConsumerMetadataImpl::getPartitionAssignment() const
{
    if (!_handle) {
        throw HandleException("Null consumer");
    }
    if (_partitions.empty()) {
        _partitions = static_cast<const cppkafka::Consumer*>(_handle)->get_assignment();
    }
    return _partitions;
}

cppkafka::GroupInformation ConsumerMetadataImpl::getGroupInformation() const
{
    if (!_handle) {
        throw HandleException("Null consumer");
    }
    return _handle->get_consumer_group(static_cast<const cppkafka::Consumer*>(_handle)->get_member_id());
}

cppkafka::GroupInformation ConsumerMetadataImpl::getGroupInformation(std::chrono::milliseconds timeout) const
{
    if (!_handle) {
        throw HandleException("Null consumer");
    }
    return _handle->get_consumer_group(static_cast<const cppkafka::Consumer*>(_handle)->get_member_id(), timeout);
}

cppkafka::GroupInformationList ConsumerMetadataImpl::getAllGroupsInformation() const
{
    if (!_handle) {
        throw HandleException("Null consumer");
    }
    return _handle->get_consumer_groups();
}

cppkafka::GroupInformationList ConsumerMetadataImpl::getAllGroupsInformation(std::chrono::milliseconds timeout) const
{
    if (!_handle) {
        throw HandleException("Null consumer");
    }
    return _handle->get_consumer_groups(timeout);
}

PartitionStrategy ConsumerMetadataImpl::getPartitionStrategy() const
{
    return _strategy;
}

}
}

