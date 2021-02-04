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
#ifndef BLOOMBERG_COROKAFKA_OFFSET_MANAGER_IMPL_H
#define BLOOMBERG_COROKAFKA_OFFSET_MANAGER_IMPL_H

#include <corokafka/interface/corokafka_ioffset_manager.h>
#include <corokafka/interface/corokafka_impl.h>

namespace Bloomberg {
namespace corokafka {

class OffsetManagerImpl : public IOffsetManager
{
public:
    using ResetAction = IOffsetManager::ResetAction;

    //CTORs
    OffsetManagerImpl() = delete;
    OffsetManagerImpl(const OffsetManagerImpl&) = delete;
    OffsetManagerImpl(OffsetManagerImpl&&) = default;
    OffsetManagerImpl& operator=(const OffsetManagerImpl&) = delete;
    OffsetManagerImpl& operator=(OffsetManagerImpl&&) = delete;
    explicit OffsetManagerImpl(corokafka::ConsumerManager& consumerManager);
    OffsetManagerImpl(corokafka::ConsumerManager& consumerManager,
                      std::chrono::milliseconds brokerTimeout);
    
    //-------------------------------------------------------------------------------
    //                             IOffsetManager
    //-------------------------------------------------------------------------------
    cppkafka::Error saveOffset(const cppkafka::TopicPartition& offset) override;
    cppkafka::Error saveOffset(const cppkafka::TopicPartition& offset,
                               ExecMode execMode) override;
    cppkafka::Error saveOffset(const IMessage& message) override;
    cppkafka::Error saveOffset(const IMessage& message,
                               ExecMode execMode) override;
    cppkafka::TopicPartition getCurrentOffset(const cppkafka::TopicPartition& partition) override;
    cppkafka::TopicPartition getBeginOffset(const cppkafka::TopicPartition& partition) override;
    cppkafka::Error forceCommit() override;
    cppkafka::Error forceCommit(ExecMode execMode) override;
    cppkafka::Error forceCommit(const cppkafka::TopicPartition& partition) override;
    cppkafka::Error forceCommit(const cppkafka::TopicPartition& partition,
                                ExecMode execMode) override;
    cppkafka::Error forceCommitCurrentOffset() override;
    cppkafka::Error forceCommitCurrentOffset(ExecMode execMode) override;
    cppkafka::Error forceCommitCurrentOffset(const cppkafka::TopicPartition& partition) override;
    cppkafka::Error forceCommitCurrentOffset(const cppkafka::TopicPartition& partition,
                                             ExecMode execMode) override;
    void resetPartitionOffsets(ResetAction action) override;
    void resetPartitionOffsets(const std::string& topic,
                               ResetAction action) override;
    std::string toString() const override;
    std::string toString(const std::string& topic) const override;
    void enableCommitTracing(bool enable) override;
    
private:
    using OffsetMap = IntervalSet<int64_t>;
    using InsertReturnType = OffsetMap::InsertReturnType;
    using Iterator = OffsetMap::Iterator;
    struct OffsetRanges {
        int64_t                 _beginOffset{cppkafka::TopicPartition::Offset::OFFSET_INVALID};
        int64_t                 _currentOffset{cppkafka::TopicPartition::Offset::OFFSET_INVALID};
        mutable quantum::Mutex  _offsetsMutex; //protect offset map
        OffsetMap               _offsets;
    };
    using PartitionMap = std::unordered_map<int, OffsetRanges>;
    struct TopicSettings {
        bool            _autoResetAtEnd{true};
        PartitionMap    _partitions;
    };
    using TopicMap = std::unordered_map<std::string, TopicSettings>;
    static Range<int64_t>
    insertOffset(OffsetRanges& ranges,
                 int64_t offset);

    static void setStartingOffset(int64_t offset,
                                  OffsetRanges &ranges,
                                  const cppkafka::TopicPartition& committedOffset,
                                  const OffsetWatermark& watermark,
                                  bool autoResetEnd);

    static const cppkafka::TopicPartition&
    findPartition(const cppkafka::TopicPartitionList& partitions,
                  int partition);
    
    static const OffsetWatermark&
    findWatermark(const OffsetWatermarkList& watermarks,
                  int partition);
    
    void queryOffsetsFromBroker(const std::string& topic,
                                TopicSettings& settings);
    
    const TopicSettings& getTopicSettings(const cppkafka::TopicPartition& partition) const;
    TopicSettings& getTopicSettings(const cppkafka::TopicPartition& partition);
    
    const OffsetRanges& getOffsetRanges(const TopicSettings& settings,
                                        const cppkafka::TopicPartition& partition) const;
    OffsetRanges& getOffsetRanges(TopicSettings& settings,
                                  const cppkafka::TopicPartition& partition);
    
    template <typename...EXEC_MODE>
    cppkafka::Error saveOffsetImpl(const cppkafka::TopicPartition& offset,
                                   EXEC_MODE&&...execMode);
    
    template <typename...EXEC_MODE>
    cppkafka::Error forceCommitImpl(EXEC_MODE&&...execMode);
    
    template <typename...EXEC_MODE>
    cppkafka::Error forceCommitPartitionImpl(const cppkafka::TopicPartition& partition,
                                             EXEC_MODE&&...execMode);
    
    template <typename...EXEC_MODE>
    cppkafka::Error forceCommitCurrentOffsetImpl(EXEC_MODE&&...execMode);
    
    void logOffsets(const std::string& facility, const cppkafka::TopicPartition& offset) const;
    void logOffsets(const std::string& facility, const cppkafka::TopicPartitionList& offsets) const;

    // Members
    corokafka::ConsumerManager&     _consumerManager;
    std::chrono::milliseconds       _brokerTimeout;
    TopicMap                        _topicMap;
    bool                            _traceCommits{false};
};

// Implementation
template <typename...EXEC_MODE>
cppkafka::Error OffsetManagerImpl::saveOffsetImpl(const cppkafka::TopicPartition& offset,
                                                  EXEC_MODE&&...execMode)
{
    try {
        if (offset.get_offset() < 0) {
            return RD_KAFKA_RESP_ERR_OFFSET_OUT_OF_RANGE;
        }
        OffsetRanges& ranges = getOffsetRanges(getTopicSettings(offset), offset);
        Range<int64_t> range(-1,-1);
        {//locked scope
            logOffsets("OffsetManager:Insert", offset);
            quantum::Mutex::Guard guard(quantum::local::context(), ranges._offsetsMutex);
            range = insertOffset(ranges, offset.get_offset());
        }
        //Commit range
        if (range.second != -1) {
            //This is a valid range
            cppkafka::TopicPartition partition{offset.get_topic(), offset.get_partition(), range.second};
            logOffsets("OffsetManager:Commit", partition);
            return _consumerManager.commit(partition, std::forward<EXEC_MODE>(execMode)...);
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

template <typename...EXEC_MODE>
cppkafka::Error OffsetManagerImpl::forceCommitImpl(EXEC_MODE&&...execMode)
{
    try {
        bool isSyncCommit = false;
        for (auto& topic : _topicMap) {
            cppkafka::TopicPartitionList partitions;
            TopicSettings& settings = topic.second;
            for (auto& partition : settings._partitions) {
                Range<int64_t> range(-1,-1);
                OffsetRanges& ranges = partition.second;
                quantum::Mutex::Guard guard(quantum::local::context(), ranges._offsetsMutex);
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
            //Commit all offsets for this topic
            if (!partitions.empty()) {
                logOffsets("OffsetManager:Commit", partitions);
                cppkafka::Error error = _consumerManager.commit(partitions, std::forward<EXEC_MODE>(execMode)...);
                if (error) {
                    return error;
                }
            }
        } //topics
        return {};
    }
    catch(...) {
    }
    return RD_KAFKA_RESP_ERR_UNKNOWN;
}

template <typename...EXEC_MODE>
cppkafka::Error OffsetManagerImpl::forceCommitPartitionImpl(const cppkafka::TopicPartition& partition,
                                                            EXEC_MODE&&...execMode)
{
    try {
        Range<int64_t> range(-1,-1);
        OffsetRanges& ranges = getOffsetRanges(getTopicSettings(partition), partition);
        { //locked scope
            quantum::Mutex::Guard guard(quantum::local::context(), ranges._offsetsMutex);
            Iterator it = ranges._offsets.begin();
            if (it != ranges._offsets.end()) {
                range = {it->first, it->second};
                //bump current offset
                ranges._currentOffset = range.second + 1;
                //delete range from map
                ranges._offsets.erase(it);
            }
        }
        //Commit range
        if (range.second != -1) {
            logOffsets("OffsetManager:Commit", partition);
            return _consumerManager.commit(partition, std::forward<EXEC_MODE>(execMode)...);
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

template <typename...EXEC_MODE>
cppkafka::Error OffsetManagerImpl::forceCommitCurrentOffsetImpl(EXEC_MODE&&...execMode)
{
    try {
        bool isSyncCommit = false;
        for (auto& topic : _topicMap) {
            cppkafka::TopicPartitionList partitions;
            TopicSettings& settings = topic.second;
            for (auto& partition : settings._partitions) {
                OffsetRanges& ranges = partition.second;
                Range<int64_t> range(-1,-1);
                {//locked scope
                    if (_traceCommits) {
                        logOffsets("OffsetManager:Insert", {topic.first, partition.first, ranges._currentOffset});
                    }
                    quantum::Mutex::Guard guard(quantum::local::context(), ranges._offsetsMutex);
                    range = insertOffset(ranges, ranges._currentOffset);
                }
                //Commit range
                if (range.second != -1) {
                    partitions.emplace_back(topic.first, partition.first, range.second);
                }
            } //partitions
            //Commit all offsets for this topic
            if (!partitions.empty()) {
                logOffsets("OffsetManager:Commit", partitions);
                cppkafka::Error error = _consumerManager.commit(partitions, std::forward<EXEC_MODE>(execMode)...);
                if (error) {
                    return error;
                }
            }
        } //topics
        return {};
    }
    catch(...) {
    }
    return RD_KAFKA_RESP_ERR_UNKNOWN;
}


}}

#endif //BLOOMBERG_COROKAFKA_OFFSET_MANAGER_IMPL_H
