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
#ifndef BLOOMBERG_COROKAFKA_OFFSET_MANAGER_H
#define BLOOMBERG_COROKAFKA_OFFSET_MANAGER_H

#include <corokafka/utils/corokafka_interval_set.h>
#include <corokafka/corokafka_consumer_manager.h>
#include <corokafka/corokafka_received_message.h>
#include <quantum/quantum.h>
#include <unordered_map>
#include <chrono>

namespace Bloomberg {
namespace corokafka {

/// @brief The OffsetManager class helps commit offsets in a concurrent environment
///        by guaranteeing gapless ordering. When offsets are consumed on multiple
///        threads, some messages may be processed out-of-order and committing them
///        before others may result in message loss should a crash occur.
class OffsetManager
{
public:
    enum class ResetAction : int {
        FetchOffsets,       //Fetch new committed offsets and watermarks from broker
        DoNotFetchOffsets   //Don't fetch any offsets
    };
    OffsetManager() = delete;
    OffsetManager(const OffsetManager&) = delete;
    OffsetManager(OffsetManager&&) = default;
    OffsetManager& operator=(const OffsetManager&) = delete;
    OffsetManager& operator=(OffsetManager&&) = delete;
    
    /// @brief Creates an offset manager.
    /// @param consumerManager The consumer manager for which we want to manage offsets.
    /// @param timeout The max timeout for querying offsets and watermarks from brokers.
    ///                If timeout is not specified, the default consumer timeout will be used.
    ///                Set timeout to -1 to block infinitely.
    /// @note May throw if the broker queries time out.
    explicit OffsetManager(corokafka::ConsumerManager& consumerManager);
    OffsetManager(corokafka::ConsumerManager& consumerManager,
                  std::chrono::milliseconds brokerTimeout);
    
    /// @brief Saves an offset to be committed later and potentially commits a range of offsets if it became available.
    /// @param offset The partition containing the offset to be saved.
    /// @param execMode If specified, overrides 'internal.consumer.commit.exec' consumer setting.
    /// @returns Error.
    cppkafka::Error saveOffset(const cppkafka::TopicPartition& offset) noexcept;
    cppkafka::Error saveOffset(const cppkafka::TopicPartition& offset,
                               ExecMode execMode) noexcept;

    /// @brief Saves an offset to be committed later and potentially commits a range of offsets if it became available.
    /// @param message The message whose offset we want to commit.
    /// @param execMode If specified, overrides 'internal.consumer.commit.exec' consumer setting.
    /// @returns Error.
    /// @remark Note that this will actually commit ReceivedMessage::getOffset()+1
    template <typename KEY, typename PAYLOAD, typename HEADERS>
    cppkafka::Error saveOffset(const ReceivedMessage<KEY,PAYLOAD,HEADERS>& message) noexcept;
    
    template <typename KEY, typename PAYLOAD, typename HEADERS>
    cppkafka::Error saveOffset(const ReceivedMessage<KEY,PAYLOAD,HEADERS>& message,
                               ExecMode execMode) noexcept;
    
    /// @brief Returns the smallest offset which is yet to be committed.
    /// @param partition The partition where this offset is.
    /// @return The offset.
    cppkafka::TopicPartition getCurrentOffset(const cppkafka::TopicPartition& partition);
    
    /// @brief Returns the beginning offset.
    /// @param partition The partition where this offset is.
    /// @return The offset.
    /// @note [getBeginOffset(), getCurrentOffset()) gives the total committed range of offsets.
    cppkafka::TopicPartition getBeginOffset(const cppkafka::TopicPartition& partition);
    
    /// @brief Commits the first available *lowest* offset range even if there are smaller offset(s) still pending.
    /// @param partition The partition where this offset is, or all partitions if not specified.
    /// @param execMode If specified, overrides 'internal.consumer.commit.exec' consumer setting.
    /// @warning Messages may be lost if the committed offsets were not yet complete and the application crashes.
    cppkafka::Error forceCommit() noexcept;
    cppkafka::Error forceCommit(ExecMode execMode) noexcept;
    cppkafka::Error forceCommit(const cppkafka::TopicPartition& partition) noexcept;
    cppkafka::Error forceCommit(const cppkafka::TopicPartition& partition,
                                ExecMode execMode) noexcept;
    
    /// @brief Commit the lowest offset range as if by calling `saveOffset(getCurrentOffset(partition))`. This will
    ///        either commit the current offset or any range resulting by merging with the current offset.
    /// @param partition The partition where this offset is, or all partitions if not specified.
    /// @param execMode If specified, overrides 'internal.consumer.commit.exec' consumer setting.
    /// @warning Messages may be lost if the committed offsets were not yet complete and the application crashes.
    cppkafka::Error forceCommitCurrentOffset();
    cppkafka::Error forceCommitCurrentOffset(ExecMode execMode);
    cppkafka::Error forceCommitCurrentOffset(const cppkafka::TopicPartition& partition);
    cppkafka::Error forceCommitCurrentOffset(const cppkafka::TopicPartition& partition,
                                             ExecMode execMode);
    
    /// @brief Reset all partition offsets for the specified topic.
    /// @param topic The topic for which all partitions and offsets will be reset.
    ///              If topic is not present, then reset all topic partitions.
    /// @param action Determines if new offsets should be fetched or not.
    /// @note Call this when a new partition assignment has been made for this topic.
    /// @note This call **MUST** be made with exclusive access (i.e. when no other threads access
    ///       the ConsumerManager).
    /// @note May throw.
    void resetPartitionOffsets(ResetAction action = ResetAction::FetchOffsets);
    void resetPartitionOffsets(const std::string& topic,
                               ResetAction action = ResetAction::FetchOffsets);
    
    /// @brief Output the offsets contained by this manager
    /// @param topic Restrict output to a specific topic only
    std::string toString() const;
    std::string toString(const std::string& topic) const;
    
    /// @brief Enables or disables commit debug logging via the registered
    ///        log callback in the ConsumerManager
    /// @warning May be verbose
    void enableCommitTracing(bool enable);
    
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
                                   EXEC_MODE&&...execMode) noexcept;
    
    template <typename...EXEC_MODE>
    cppkafka::Error forceCommitImpl(EXEC_MODE&&...execMode) noexcept;
    
    template <typename...EXEC_MODE>
    cppkafka::Error forceCommitPartitionImpl(const cppkafka::TopicPartition& partition,
                                             EXEC_MODE&&...execMode) noexcept;
    
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
template <typename KEY, typename PAYLOAD, typename HEADERS>
cppkafka::Error OffsetManager::saveOffset(const ReceivedMessage<KEY,PAYLOAD,HEADERS>& message) noexcept {
    return saveOffset({message.getTopic(), message.getPartition(), message.getOffset()+1});
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
cppkafka::Error OffsetManager::saveOffset(const ReceivedMessage<KEY,PAYLOAD,HEADERS>& message,
                                          ExecMode execMode) noexcept {
    return saveOffset({message.getTopic(), message.getPartition(), message.getOffset()+1}, execMode);
}

template <typename...EXEC_MODE>
cppkafka::Error OffsetManager::saveOffsetImpl(const cppkafka::TopicPartition& offset,
                                              EXEC_MODE&&...execMode) noexcept
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
cppkafka::Error OffsetManager::forceCommitImpl(EXEC_MODE&&...execMode) noexcept
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
cppkafka::Error OffsetManager::forceCommitPartitionImpl(const cppkafka::TopicPartition& partition,
                                                        EXEC_MODE&&...execMode) noexcept
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
cppkafka::Error OffsetManager::forceCommitCurrentOffsetImpl(EXEC_MODE&&...execMode)
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

/// @brief Stream operator for the OffsetManager class
std::ostream& operator<<(std::ostream& output, const OffsetManager& rhs);

/// @brief RAII-type class for committing an offset within a scope.
class OffsetCommitGuard
{
public:
    /// @brief Saves an offset locally to be committed when this object goes out of scope.
    /// @param om The offset manager reference.
    /// @param offset The offset to be committed.
    /// @param execMode If specified, overrides 'internal.consumer.commit.exec' consumer setting.
    OffsetCommitGuard(OffsetManager& om,
                      cppkafka::TopicPartition offset) :
        _om(om),
        _offset(std::move(offset))
    {}
    OffsetCommitGuard(OffsetManager& om,
                      cppkafka::TopicPartition offset,
                      ExecMode execMode) :
        _om(om),
        _offset(std::move(offset)),
        _execMode(execMode),
        _useExecMode(true)
    {}

    /// @brief Saves an offset locally to be committed when this object goes out of scope.
    /// @param om The offset manager reference.
    /// @param message The message whose offset we want to commit.
    /// @param execMode If specified, overrides 'internal.consumer.commit.exec' consumer setting.
    /// @remark Note that this will actually commit ReceivedMessage::getOffset()+1
    template <typename KEY, typename PAYLOAD, typename HEADERS>
    OffsetCommitGuard(OffsetManager& om,
                      const ReceivedMessage<KEY,PAYLOAD,HEADERS>& message) :
            _om(om),
            _offset(message.getTopic(), message.getPartition(), message.getOffset()+1)
    {}
    template <typename KEY, typename PAYLOAD, typename HEADERS>
    OffsetCommitGuard(OffsetManager& om,
                      const ReceivedMessage<KEY,PAYLOAD,HEADERS>& message,
                      ExecMode execMode) :
            _om(om),
            _offset(message.getTopic(), message.getPartition(), message.getOffset()+1),
            _execMode(execMode),
            _useExecMode(true)
    {}

    /// @brief Destructor. This will attempt to commit the offset.
    ~OffsetCommitGuard()
    {
        if (_useExecMode) {
            _om.saveOffset(_offset, _execMode);
        }
        else {
            _om.saveOffset(_offset);
        }
    }
private:
    OffsetManager&                      _om;
    const cppkafka::TopicPartition      _offset;
    ExecMode                            _execMode{ExecMode::Async};
    bool                                _useExecMode{false};
};

}}

#endif //BLOOMBERG_COROKAFKA_OFFSET_MANAGER_H
