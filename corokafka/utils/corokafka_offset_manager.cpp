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
#include <corokafka/utils/corokafka_offset_manager.h>
#include <corokafka/corokafka_utils.h>
#include <corokafka/corokafka_metadata.h>
#include <corokafka/corokafka_exception.h>
#include <algorithm>

namespace Bloomberg {
namespace corokafka {

OffsetManager::OffsetManager(corokafka::ConsumerManager& consumerManager) :
    OffsetManager(consumerManager, std::chrono::milliseconds((int)TimerValues::Disabled))
{
}

OffsetManager::OffsetManager(corokafka::ConsumerManager& consumerManager,
                             std::chrono::milliseconds brokerTimeout) :
    _consumerManager(consumerManager),
    _brokerTimeout(brokerTimeout)
{
    if ((_brokerTimeout <= std::chrono::milliseconds::zero()) &&
        (_brokerTimeout != std::chrono::milliseconds((int)TimerValues::Unlimited)) &&
        (_brokerTimeout != std::chrono::milliseconds((int)TimerValues::Disabled))) {
        throw InvalidArgumentException(2, "Timeout values can be [-2, -1, >0]");
    }
    //Get all managed topics
    std::vector<std::string> topics = _consumerManager.getTopics();
    for (auto&& topic : topics) {
        //Create the settings for this topic
        TopicSettings& topicSettings = _topicMap[topic];
        const ConsumerConfiguration& config = _consumerManager.getConfiguration(topic);
        //Check the offset reset if specified
        const cppkafka::ConfigurationOption* offsetReset = config.getOption("auto.offset.reset");
        if (offsetReset && StringEqualCompare()(offsetReset->get_value(), "smallest")) {
            topicSettings._autoResetAtEnd = false;
        }
        else if (offsetReset && StringEqualCompare()(offsetReset->get_value(), "earliest")) {
            topicSettings._autoResetAtEnd = false;
        }
        else if (offsetReset && StringEqualCompare()(offsetReset->get_value(), "beginning")) {
            topicSettings._autoResetAtEnd = false;
        }
        //Try to load current offsets. This may throw if brokers are not available
        queryOffsetsFromBroker(topic, topicSettings);
    }
}

void OffsetManager::queryOffsetsFromBroker(const std::string& topic,
                                           TopicSettings& settings)
{
    //Query offsets and watermarks from broker
    cppkafka::TopicPartitionList committedOffsets;
    OffsetWatermarkList watermarks;
    ConsumerMetadata metadata = _consumerManager.getMetadata(topic);
    if (_brokerTimeout == std::chrono::milliseconds((int)TimerValues::Disabled)) {
        committedOffsets = metadata.queryCommittedOffsets();
        watermarks = metadata.queryOffsetWatermarks();
    }
    else {
        committedOffsets = metadata.queryCommittedOffsets(_brokerTimeout);
        watermarks = metadata.queryOffsetWatermarks(_brokerTimeout);
    }
    //Get initial partition assignment
    const cppkafka::TopicPartitionList& assignment = metadata.getPartitionAssignment();
    for (const cppkafka::TopicPartition& toppar : assignment) {
        setStartingOffset(toppar.get_offset(),
                          settings._partitions[toppar.get_partition()],
                          findPartition(committedOffsets, toppar.get_partition()),
                          findWatermark(watermarks, toppar.get_partition()),
                          settings._autoResetAtEnd);
    }
}

const cppkafka::TopicPartition& OffsetManager::findPartition(const cppkafka::TopicPartitionList& partitions,
                                                             int partition)
{
    static cppkafka::TopicPartition notFound;
    auto it = std::find_if(partitions.begin(),
                           partitions.end(),
                           [&partition](const auto& toppar)->bool{
        return (toppar.get_partition() == partition);
    });
    return it == partitions.end() ? notFound : *it;
}

const OffsetWatermark& OffsetManager::findWatermark(const OffsetWatermarkList& watermarks,
                                                    int partition)
{
    static OffsetWatermark notFound;
    auto it = std::find_if(watermarks.begin(),
                           watermarks.end(),
                           [&partition](const auto& watermark)->bool{
        return (watermark._partition == partition);
    });
    return it == watermarks.end() ? notFound : *it;
}

void OffsetManager::setStartingOffset(int64_t offset,
                                      OffsetRanges& ranges,
                                      const cppkafka::TopicPartition& committedOffset,
                                      const OffsetWatermark& watermark,
                                      bool autoResetAtEnd)
{
    switch (offset) {
        case cppkafka::TopicPartition::Offset::OFFSET_STORED:
        case cppkafka::TopicPartition::Offset::OFFSET_INVALID:
            //populate from committed offsets if any, otherwise from watermark
            if (committedOffset.get_offset() >= 0) {
                ranges._beginOffset = ranges._currentOffset = committedOffset.get_offset();
            }
            else {
                ranges._beginOffset = ranges._currentOffset =
                        autoResetAtEnd ? watermark._watermark._high : watermark._watermark._low;
            }
            break;
        case cppkafka::TopicPartition::Offset::OFFSET_BEGINNING:
            ranges._beginOffset = ranges._currentOffset = watermark._watermark._low;
            break;
        case cppkafka::TopicPartition::Offset::OFFSET_END:
            ranges._beginOffset = ranges._currentOffset = watermark._watermark._high;
            break;
        default:
            if (offset < RD_KAFKA_OFFSET_TAIL_BASE) {
                //rewind from high watermark
                ranges._beginOffset = ranges._currentOffset =
                        watermark._watermark._high - (RD_KAFKA_OFFSET_TAIL_BASE - offset);
            }
            break;
    }
}

cppkafka::Error OffsetManager::saveOffset(const cppkafka::TopicPartition& offset) noexcept
{
    return saveOffsetImpl(offset);
}

cppkafka::Error OffsetManager::saveOffset(const cppkafka::TopicPartition& offset,
                                          ExecMode execMode) noexcept
{
    return saveOffsetImpl(offset, execMode);
}

cppkafka::TopicPartition OffsetManager::getCurrentOffset(const cppkafka::TopicPartition& partition)
{
    TopicSettings& settings = _topicMap.at(partition.get_topic());
    OffsetRanges& ranges = settings._partitions.at(partition.get_partition());
    quantum::Mutex::Guard guard(quantum::local::context(), ranges._offsetsMutex);
    return cppkafka::TopicPartition(partition.get_topic(),
                                    partition.get_partition(),
                                    ranges._currentOffset);
}

cppkafka::TopicPartition OffsetManager::getBeginOffset(const cppkafka::TopicPartition& partition)
{
    TopicSettings& settings = _topicMap.at(partition.get_topic());
    OffsetRanges& ranges = settings._partitions.at(partition.get_partition());
    //NOTE: no need to lock here as the begin offset is read-only
    return cppkafka::TopicPartition(partition.get_topic(),
                                    partition.get_partition(),
                                    ranges._beginOffset);
}

cppkafka::Error OffsetManager::forceCommit() noexcept
{
    return forceCommitImpl();
}

cppkafka::Error OffsetManager::forceCommit(ExecMode execMode) noexcept
{
    return forceCommitImpl(execMode);
}

cppkafka::Error OffsetManager::forceCommit(const cppkafka::TopicPartition& partition) noexcept
{
    return forceCommitPartitionImpl(partition);
}

cppkafka::Error OffsetManager::forceCommit(const cppkafka::TopicPartition& partition,
                                           ExecMode execMode) noexcept
{
    return forceCommitPartitionImpl(partition, execMode);
}

cppkafka::Error OffsetManager::forceCommitCurrentOffset()
{
    return forceCommitCurrentOffsetImpl();
}

cppkafka::Error OffsetManager::forceCommitCurrentOffset(ExecMode execMode)
{
    return forceCommitCurrentOffsetImpl(execMode);
}

cppkafka::Error OffsetManager::forceCommitCurrentOffset(const cppkafka::TopicPartition& partition)
{
    return saveOffset(getCurrentOffset(partition));
}

cppkafka::Error OffsetManager::forceCommitCurrentOffset(const cppkafka::TopicPartition& partition,
                                                        ExecMode execMode)
{
    return saveOffset(getCurrentOffset(partition), execMode);
}

Range<int64_t> OffsetManager::insertOffset(OffsetRanges& ranges,
                                           int64_t offset)
{
    Range<int64_t> range(-1,-1);
    auto it = ranges._offsets.insert(Point<int64_t>{offset});
    if (it.second) {
        //a range was modified
        if (it.first->first == (ranges._currentOffset + 1)) {
            //we can commit this range
            range = {it.first->first, it.first->second};
            ranges._currentOffset = it.first->second;
            //delete range from map
            ranges._offsets.erase(it.first->first);
        }
    }
    return range;
}

void OffsetManager::resetPartitionOffsets()
{
    std::vector<std::string> topics = _consumerManager.getTopics();
    for (auto&& topic : topics) {
        resetPartitionOffsets(topic);
    }
}

void OffsetManager::resetPartitionOffsets(const std::string& topic)
{
    TopicSettings& topicSettings = _topicMap[topic];
    topicSettings._partitions.clear();
    queryOffsetsFromBroker(topic, topicSettings);
}

}}

