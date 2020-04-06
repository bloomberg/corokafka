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

namespace Bloomberg {
namespace corokafka {

OffsetManager::OffsetManager(corokafka::ConsumerManager& consumerManager) :
    _consumerManager(consumerManager)
{
    //Get all managed topics
    std::vector<std::string> topics = _consumerManager.getTopics();
    for (auto&& topic : topics) {
        //Create the settings for this topic
        TopicSettings& topicSettings = _topicMap[topic];
        const ConsumerConfiguration& config = _consumerManager.getConfiguration(topic);
        //Check if we are committing synchronously
        const cppkafka::ConfigurationOption* commitExec = config.getOption(ConsumerConfiguration::Options::commitExec);
        if (commitExec && StringEqualCompare()(commitExec->get_value(), "sync")) {
            topicSettings._syncCommit = true;
        }
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
    }
}

void OffsetManager::queryOffsetsFromBroker(const std::string& topic,
                                           TopicSettings& settings)
{
    //Create the assigned partitions for tracking
    if (!settings._partitions.empty()) {
        //we have already queried the broker
        return;
    }
    //query brokers for partitions and offsets
    ConsumerMetadata metadata = _consumerManager.getMetadata(topic);
    cppkafka::TopicPartitionList committedOffsets = metadata.queryCommittedOffsets();
    Metadata::OffsetWatermarkList watermarks = metadata.queryOffsetWatermarks();
    const cppkafka::TopicPartitionList& assignment = metadata.getPartitionAssignment();
    //Get initial partition assignment
    for (const cppkafka::TopicPartition& toppar : assignment) {
        setStartingOffset(toppar.get_offset(),
                          settings._partitions[toppar.get_partition()],
                          findPartition(committedOffsets, toppar.get_partition()),
                          findWatermark(watermarks, toppar.get_partition()),
                          settings._autoResetAtEnd);
    }
}

const cppkafka::TopicPartition& OffsetManager::findPartition(const cppkafka::TopicPartitionList& partitions, int partition)
{
    static cppkafka::TopicPartition notFound;
    for (const auto& toppar : partitions) {
        if (toppar.get_partition() == partition) {
            return toppar;
        }
    }
    return notFound;
}

const OffsetWatermark& OffsetManager::findWatermark(const Metadata::OffsetWatermarkList& watermarks, int partition)
{
    static OffsetWatermark notFound;
    for (const auto& watermark : watermarks) {
        if (watermark._partition == partition) {
            return watermark;
        }
    }
    return notFound;
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
                ranges._beginOffset = ranges._currentOffset = committedOffset.get_offset() + 1;
            }
            else {
                ranges._beginOffset = ranges._currentOffset =
                        autoResetAtEnd ? watermark._watermark._high + 1 : watermark._watermark._low + 1;
            }
            break;
        case cppkafka::TopicPartition::Offset::OFFSET_BEGINNING:
            ranges._beginOffset = ranges._currentOffset = watermark._watermark._low + 1;
            break;
        case cppkafka::TopicPartition::Offset::OFFSET_END:
            ranges._beginOffset = ranges._currentOffset = watermark._watermark._high + 1;
            break;
        default:
            if (offset < RD_KAFKA_OFFSET_TAIL_BASE) {
                //rewind from high watermark
                ranges._beginOffset = ranges._currentOffset =
                        watermark._watermark._high - (RD_KAFKA_OFFSET_TAIL_BASE - offset) + 1;
            }
            break;
    }
}

cppkafka::Error OffsetManager::saveOffset(const cppkafka::TopicPartition& offset,
                                          bool forceSync) noexcept
{
    try {
        if (offset.get_offset() < 0) {
            return RD_KAFKA_RESP_ERR_OFFSET_OUT_OF_RANGE;
        }
        TopicSettings& settings = _topicMap.at(offset.get_topic());
        quantum::Mutex::Guard guard(quantum::local::context(), settings._mutex);
        queryOffsetsFromBroker(offset.get_topic(), settings);
        OffsetRanges& ranges = settings._partitions.at(offset.get_partition());
        Range<int64_t> range = insertOffset(ranges, offset.get_offset());
        //Commit range
        if (range.second != -1) {
            if (offset.get_offset() == range.second) {
                return commit(offset, settings._syncCommit || forceSync);
            }
            else {
                //End of the range is greater than the committed offset
                return commit(cppkafka::TopicPartition{offset.get_topic(), offset.get_partition(), range.second},
                              settings._syncCommit || forceSync);
            }
        }
        return {};
    }
    catch (const std::out_of_range& ex) {
        return RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION;
    }
    catch(...) {
    }
    return RD_KAFKA_RESP_ERR_UNKNOWN;
}

cppkafka::TopicPartition OffsetManager::getCurrentOffset(const cppkafka::TopicPartition& partition)
{
    TopicSettings& settings = _topicMap.at(partition.get_topic());
    quantum::Mutex::Guard guard(quantum::local::context(), settings._mutex);
    queryOffsetsFromBroker(partition.get_topic(), settings);
    return cppkafka::TopicPartition(partition.get_topic(),
                                    partition.get_partition(),
                                    settings._partitions.at(partition.get_partition())._currentOffset);
}

cppkafka::TopicPartition OffsetManager::getBeginOffset(const cppkafka::TopicPartition& partition)
{
    TopicSettings& settings = _topicMap.at(partition.get_topic());
    quantum::Mutex::Guard guard(quantum::local::context(), settings._mutex);
    queryOffsetsFromBroker(partition.get_topic(), settings);
    return cppkafka::TopicPartition(partition.get_topic(),
                                    partition.get_partition(),
                                    settings._partitions.at(partition.get_partition())._beginOffset);
}

cppkafka::Error OffsetManager::forceCommit(bool forceSync)
{
    try {
        bool isSyncCommit = false;
        cppkafka::TopicPartitionList partitions;
        for (auto& topic : _topicMap) {
            TopicSettings& settings = topic.second;
            quantum::Mutex::Guard guard(quantum::local::context(), settings._mutex);
            if (settings._syncCommit) {
                isSyncCommit = true;
            }
            for (auto& partition : settings._partitions) {
                Range<int64_t> range(-1,-1);
                OffsetRanges& ranges = partition.second;
                Iterator it = ranges._offsets.begin();
                if (it != ranges._offsets.end()) {
                    range = {it->first, it->second};
                    //bump current offset
                    ranges._currentOffset = it->second+1;
                    //delete range from map
                    ranges._offsets.erase(it);
                }
                //Commit range
                if (range.second != -1) {
                    partitions.emplace_back(topic.first, partition.first, range.second);
                }
            } //partitions
        } //topics
        //Commit all offsets
        if (!partitions.empty()) {
            return commit(partitions, isSyncCommit || forceSync);
        }
        return {};
    }
    catch(...) {
    }
    return RD_KAFKA_RESP_ERR_UNKNOWN;
}

cppkafka::Error OffsetManager::forceCommit(const cppkafka::TopicPartition& partition,
                                           bool forceSync)
{
    try {
        Range<int64_t> range(-1,-1);
        TopicSettings& settings = _topicMap.at(partition.get_topic());
        quantum::Mutex::Guard guard(quantum::local::context(), settings._mutex);
        OffsetRanges& ranges = settings._partitions.at(partition.get_partition());
        Iterator it = ranges._offsets.begin();
        if (it != ranges._offsets.end()) {
            range = {it->first, it->second};
            //bump current offset
            ranges._currentOffset = range.second+1;
            //delete range from map
            ranges._offsets.erase(it);
        }
        //Commit range
        if (range.second != -1) {
            return commit(partition, settings._syncCommit || forceSync);
        }
        return {};
    }
    catch (const std::out_of_range& ex) {
        return RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION;
    }
    catch(...) {
    }
    return RD_KAFKA_RESP_ERR_UNKNOWN;
}

cppkafka::Error OffsetManager::forceCommitCurrentOffset(const cppkafka::TopicPartition& partition,
                                                         bool forceSync)
{
    return saveOffset(getCurrentOffset(partition), forceSync);
}

cppkafka::Error OffsetManager::forceCommitCurrentOffset(bool forceSync)
{
    try {
        bool isSyncCommit = false;
        cppkafka::TopicPartitionList partitions;
        for (auto& topic : _topicMap) {
            TopicSettings& settings = topic.second;
            quantum::Mutex::Guard guard(quantum::local::context(), settings._mutex);
            for (auto& partition : settings._partitions) {
                OffsetRanges& ranges = partition.second;
                if (settings._syncCommit) {
                    isSyncCommit = true;
                }
                Range<int64_t> range = insertOffset(ranges, ranges._currentOffset);
                //Commit range
                if (range.second != -1) {
                    partitions.emplace_back(topic.first, partition.first, range.second);
                }
            } //partitions
        } //topics
        //Commit all offsets
        if (!partitions.empty()) {
            return commit(partitions, isSyncCommit || forceSync);
        }
        return {};
    }
    catch(...) {
    }
    return RD_KAFKA_RESP_ERR_UNKNOWN;
}

Range<int64_t> OffsetManager::insertOffset(OffsetRanges& ranges,
                                           int64_t offset)
{
    Range<int64_t> range(-1,-1);
    auto it = ranges._offsets.insert(Point<int64_t>{offset});
    if (it.second) {
        //a range was modified
        if (it.first->first == ranges._currentOffset) {
            //we can commit this range
            range = {it.first->first, it.first->second};
            ranges._currentOffset = it.first->second+1;
            //delete range from map
            ranges._offsets.erase(it.first->first);
        }
    }
    return range;
}

void OffsetManager::resetPartitions(const std::string& topic)
{
    TopicSettings& settings = _topicMap.at(topic);
    quantum::Mutex::Guard guard(quantum::local::context(), settings._mutex);
    settings._partitions.clear();
}

}}

