//
// Created by adamian on 2/3/21.
//

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
#include <corokafka/impl/corokafka_offset_manager_impl.h>
#include <corokafka/corokafka_utils.h>
#include <corokafka/corokafka_metadata.h>
#include <corokafka/corokafka_exception.h>
#include <corokafka/utils/corokafka_json_builder.h>
#include <cppkafka/detail/callback_invoker.h>
#include <algorithm>
#include <sstream>

namespace Bloomberg {
namespace corokafka {

OffsetManagerImpl::OffsetManagerImpl(corokafka::ConsumerManager& consumerManager) :
    OffsetManagerImpl(consumerManager, std::chrono::milliseconds((int)TimerValues::Disabled))
{
}

OffsetManagerImpl::OffsetManagerImpl(corokafka::ConsumerManager& consumerManager,
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
        if (!offsetReset) {
            offsetReset = config.getTopicOption("auto.offset.reset");
        }
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

void OffsetManagerImpl::queryOffsetsFromBroker(const std::string& topic,
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
    cppkafka::CallbackInvoker<Callbacks::LogCallback>
                    logCallback("log", _consumerManager.getConfiguration(topic).getLogCallback(), nullptr);
    if (logCallback) {
        std::ostringstream oss;
        {
            JsonBuilder json(oss);
            json.startMember("topicInfo").
                tag("watermarks", watermarks).
                tag("committed", committedOffsets).
                tag("assignment", assignment).
                endMember();
        }
        logCallback(_consumerManager.getMetadata(topic),
                    cppkafka::LogLevel::LogDebug,
                    "OffsetManager::QueryBroker",
                    oss.str());
    }
}

const cppkafka::TopicPartition& OffsetManagerImpl::findPartition(const cppkafka::TopicPartitionList& partitions,
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

const OffsetWatermark& OffsetManagerImpl::findWatermark(const OffsetWatermarkList& watermarks,
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

void OffsetManagerImpl::setStartingOffset(int64_t offset,
                                          OffsetRanges& ranges,
                                          const cppkafka::TopicPartition& committedOffset,
                                          const OffsetWatermark& watermark,
                                          bool autoResetAtEnd)
{
    //Watermarks should always be valid. Otherwise it means we don't have a valid assignment.
    if (watermark._partition == RD_KAFKA_PARTITION_UA) {
        return;
    }
    switch (offset) {
        case cppkafka::TopicPartition::Offset::OFFSET_STORED:
        case cppkafka::TopicPartition::Offset::OFFSET_INVALID:
            //populate from committed offsets if any, otherwise from watermark
            if (committedOffset.get_offset() >= 0) {
                //committed offset is not a Kafka magic number
                if (committedOffset.get_offset() < watermark._watermark._low) {
                    //the topic was purged and the first message offset is now above the last commit
                    ranges._beginOffset = ranges._currentOffset = watermark._watermark._low;
                }
                else {
                    ranges._beginOffset = ranges._currentOffset = committedOffset.get_offset();
                }
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

cppkafka::Error OffsetManagerImpl::saveOffset(const cppkafka::TopicPartition& offset)
{
    return saveOffsetImpl(offset);
}

cppkafka::Error OffsetManagerImpl::saveOffset(const cppkafka::TopicPartition& offset,
                                          ExecMode execMode)
{
    return saveOffsetImpl(offset, execMode);
}

cppkafka::Error OffsetManagerImpl::saveOffset(const IMessage& message)
{
    return saveOffset({message.getTopic(), message.getPartition(), message.getOffset()+1});
}

cppkafka::Error OffsetManagerImpl::saveOffset(const IMessage& message,
                                              ExecMode execMode)
{
    return saveOffset({message.getTopic(), message.getPartition(), message.getOffset()+1}, execMode);
}

cppkafka::TopicPartition OffsetManagerImpl::getCurrentOffset(const cppkafka::TopicPartition& partition)
{
    const OffsetRanges& ranges = getOffsetRanges(getTopicSettings(partition), partition);
    quantum::Mutex::Guard guard(quantum::local::context(), ranges._offsetsMutex);
    return cppkafka::TopicPartition(partition.get_topic(),
                                    partition.get_partition(),
                                    ranges._currentOffset);
}

cppkafka::TopicPartition OffsetManagerImpl::getBeginOffset(const cppkafka::TopicPartition& partition)
{
    const OffsetRanges& ranges = getOffsetRanges(getTopicSettings(partition), partition);
    //NOTE: no need to lock here as the begin offset is read-only
    return cppkafka::TopicPartition(partition.get_topic(),
                                    partition.get_partition(),
                                    ranges._beginOffset);
}

cppkafka::Error OffsetManagerImpl::forceCommit()
{
    return forceCommitImpl();
}

cppkafka::Error OffsetManagerImpl::forceCommit(ExecMode execMode)
{
    return forceCommitImpl(execMode);
}

cppkafka::Error OffsetManagerImpl::forceCommit(const cppkafka::TopicPartition& partition)
{
    return forceCommitPartitionImpl(partition);
}

cppkafka::Error OffsetManagerImpl::forceCommit(const cppkafka::TopicPartition& partition,
                                               ExecMode execMode)
{
    return forceCommitPartitionImpl(partition, execMode);
}

cppkafka::Error OffsetManagerImpl::forceCommitCurrentOffset()
{
    return forceCommitCurrentOffsetImpl();
}

cppkafka::Error OffsetManagerImpl::forceCommitCurrentOffset(ExecMode execMode)
{
    return forceCommitCurrentOffsetImpl(execMode);
}

cppkafka::Error OffsetManagerImpl::forceCommitCurrentOffset(const cppkafka::TopicPartition& partition)
{
    return saveOffset(getCurrentOffset(partition));
}

cppkafka::Error OffsetManagerImpl::forceCommitCurrentOffset(const cppkafka::TopicPartition& partition,
                                                            ExecMode execMode)
{
    return saveOffset(getCurrentOffset(partition), execMode);
}

Range<int64_t> OffsetManagerImpl::insertOffset(OffsetRanges& ranges,
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

void OffsetManagerImpl::resetPartitionOffsets(ResetAction action)
{
    std::vector<std::string> topics = _consumerManager.getTopics();
    for (auto&& topic : topics) {
        resetPartitionOffsets(topic, action);
    }
}

void OffsetManagerImpl::resetPartitionOffsets(const std::string& topic,
                                          ResetAction action)
{
    TopicSettings& topicSettings = _topicMap[topic];
    topicSettings._partitions.clear();
    if (action == ResetAction::FetchOffsets) {
        queryOffsetsFromBroker(topic, topicSettings);
    }
}

const OffsetManagerImpl::TopicSettings&
OffsetManagerImpl::getTopicSettings(const cppkafka::TopicPartition& partition) const
{
    return const_cast<OffsetManagerImpl*>(this)->getTopicSettings(partition);
}

OffsetManagerImpl::TopicSettings&
OffsetManagerImpl::getTopicSettings(const cppkafka::TopicPartition& partition)
{
    auto it = _topicMap.find(partition.get_topic());
    if (it == _topicMap.end()) {
        std::ostringstream oss;
        oss << "Unknown topic: " << partition.get_topic();
        throw std::out_of_range(oss.str());
    }
    return it->second;
}

const OffsetManagerImpl::OffsetRanges&
OffsetManagerImpl::getOffsetRanges(const TopicSettings& settings,
                                   const cppkafka::TopicPartition& partition) const
{
    return const_cast<OffsetManagerImpl*>(this)->getOffsetRanges(const_cast<TopicSettings&>(settings), partition);
}

OffsetManagerImpl::OffsetRanges&
OffsetManagerImpl::getOffsetRanges(TopicSettings& settings,
                                   const cppkafka::TopicPartition& partition)
{
    auto it = settings._partitions.find(partition.get_partition());
    if (it == settings._partitions.end()) {
        std::ostringstream oss;
        oss << "Unknown partition: " << partition.get_partition();
        throw std::out_of_range(oss.str());
    }
    return it->second;
}

std::string OffsetManagerImpl::toString() const
{
    std::ostringstream oss;
    {
        JsonBuilder json(oss);
        json.startMember("offsetManager", JsonBuilder::Array::True);
        for (const auto &topic  : _topicMap) {
            json.rawTag(toString(topic.first));
        }
        json.endMember();
    }
    return oss.str();
}

std::string OffsetManagerImpl::toString(const std::string& topic) const
{
    const TopicSettings& settings = getTopicSettings(cppkafka::TopicPartition{topic});
    std::ostringstream oss;
    {
        JsonBuilder json(oss);
        json.startMember("topic").
            tag("name", topic).
            startMember("partitions", JsonBuilder::Array::True);
        for (const auto &partition : settings._partitions) {
            json.startMember().
                tag("partition", std::to_string(partition.first).c_str()).
                tag("begin", partition.second._beginOffset).
                tag("current", partition.second._currentOffset).
                rawTag("offsets", partition.second._offsets).
                endMember();
        }
        json.endMember(). //partitions
            tag("resetAtEnd", settings._autoResetAtEnd).
            endMember(); //topic
    }
    return oss.str();
}

void OffsetManagerImpl::enableCommitTracing(bool enable)
{
    _traceCommits = enable;
}

void OffsetManagerImpl::logOffsets(const std::string& facility,
                                   const cppkafka::TopicPartition& offset) const
{
    if (!_traceCommits) return;
    const auto& topic = offset.get_topic();
    cppkafka::CallbackInvoker<Callbacks::LogCallback>
                    logCallback("log", _consumerManager.getConfiguration(topic).getLogCallback(), nullptr);
    if (!logCallback) return;
    std::ostringstream oss;
    oss << offset;
    logCallback(_consumerManager.getMetadata(topic),
                cppkafka::LogLevel::LogDebug,
                facility,
                oss.str());
}

void OffsetManagerImpl::logOffsets(const std::string& facility,
                                   const cppkafka::TopicPartitionList& offsets) const
{
    if (!_traceCommits || offsets.empty()) return;
    const auto& topic = offsets.front().get_topic();
    cppkafka::CallbackInvoker<Callbacks::LogCallback>
                    logCallback("log", _consumerManager.getConfiguration(topic).getLogCallback(), nullptr);
    if (!logCallback) return;
    std::ostringstream oss;
    oss << offsets;
    logCallback(_consumerManager.getMetadata(topic),
                cppkafka::LogLevel::LogDebug,
                facility,
                oss.str());
}

}}

