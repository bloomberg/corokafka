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

namespace Bloomberg {
namespace corokafka {

OffsetManager::OffsetManager(corokafka::ConsumerManager& consumerManager) :
    _consumerManager(consumerManager)
{
    //Get all managed topics
    std::vector<std::string> topics = consumerManager.getTopics();
    for (auto&& topic : topics) {
        bool syncCommit = false;
        PartitionMap& partitionMap = _topicMap[topic];
        const cppkafka::ConfigurationOption* commitExec =
            consumerManager.getConfiguration(topic).getOption(ConsumerConfiguration::Options::commitExec);
        if (commitExec && StringEqualCompare()(commitExec->get_value(), "sync")) {
            syncCommit = true;
        }
        cppkafka::TopicPartitionList committedOffsets = consumerManager.getMetadata(topic).queryCommittedOffsets();
        for (const cppkafka::TopicPartition partition : committedOffsets) {
            OffsetsRanges& ranges = partitionMap[partition.get_partition()];
            ranges._beginOffset = ranges._currentOffset = partition.get_offset();
            ranges._syncCommit = syncCommit;
        }
    }
}

cppkafka::Error OffsetManager::saveOffset(const cppkafka::TopicPartition& offset,
                                          bool forceSync) noexcept
{
    try {
        if (offset.get_offset() < 0) {
            return RD_KAFKA_RESP_ERR_OFFSET_OUT_OF_RANGE;
        }
        OffsetsRanges& ranges = _topicMap.at(offset.get_topic()).at(offset.get_partition());
        Range<int64_t> range = insertOffset(ranges, offset.get_offset());
        //Commit range
        if (range.second != -1) {
            if (offset.get_offset() == range.second) {
                return commit(offset, ranges._syncCommit || forceSync);
            }
            else {
                //End of the range is greater than the committed offset
                return commit(cppkafka::TopicPartition{offset.get_topic(), offset.get_partition(), range.second},
                              ranges._syncCommit || forceSync);
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

cppkafka::TopicPartition OffsetManager::getCurrentOffset(const cppkafka::TopicPartition& partition) const
{
    return cppkafka::TopicPartition(partition.get_topic(),
                                     partition.get_partition(),
                                     _topicMap.at(partition.get_topic()).at(partition.get_partition())._currentOffset);
}

cppkafka::TopicPartition OffsetManager::getBeginOffset(const cppkafka::TopicPartition& partition) const
{
    return cppkafka::TopicPartition(partition.get_topic(),
                                     partition.get_partition(),
                                     _topicMap.at(partition.get_topic()).at(partition.get_partition())._beginOffset);
}

cppkafka::Error OffsetManager::forceCommit(bool forceSync)
{
    try {
        bool isSyncCommit = false;
        cppkafka::TopicPartitionList partitions;
        for (auto& topic : _topicMap) {
            for (auto& partition : topic.second) {
                Range<int64_t> range(-1,-1);
                OffsetsRanges& ranges = partition.second;
                if (ranges._syncCommit) isSyncCommit = true;
                { //locked scope
                    quantum::Mutex::Guard guard(quantum::local::context(), ranges._mutex);
                    Iterator it = ranges._offsets.begin();
                    if (it != ranges._offsets.end()) {
                        range = {it->first, it->second};
                        //bump current offset
                        ranges._currentOffset = it->second+1;
                        //delete range from map
                        ranges._offsets.erase(it);
                    }
                } //end of locked scope
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
        OffsetsRanges& ranges = _topicMap.at(partition.get_topic()).at(partition.get_partition());
        { //locked scope
            quantum::Mutex::Guard guard(quantum::local::context(), ranges._mutex);
            Iterator it = ranges._offsets.begin();
            if (it != ranges._offsets.end()) {
                range = {it->first, it->second};
                //bump current offset
                ranges._currentOffset = range.second+1;
                //delete range from map
                ranges._offsets.erase(it);
            }
        } //end of locked scope
        //Commit range
        if (range.second != -1) {
            return commit(partition, ranges._syncCommit || forceSync);
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
            for (auto& partition : topic.second) {
                OffsetsRanges& ranges = partition.second;
                if (ranges._syncCommit) isSyncCommit = true;
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

Range<int64_t> OffsetManager::insertOffset(OffsetsRanges& ranges,
                                           int64_t offset)
{
    Range<int64_t> range(-1,-1);
    quantum::Mutex::Guard guard(quantum::local::context(), ranges._mutex);
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

}}

