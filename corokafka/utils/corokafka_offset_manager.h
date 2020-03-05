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

namespace Bloomberg {
namespace corokafka {

/// @brief The OffsetManager class helps commit offsets in a concurrent environment
///        by guaranteeing gapless ordering. When offsets are consumed on multiple
///        threads, some messages may be processed out-of-order and committing them
///        before others may result in message loss should a crash occur.
class OffsetManager
{
public:
    OffsetManager() = delete;
    OffsetManager(const OffsetManager&) = delete;
    OffsetManager(OffsetManager&&) = default;
    OffsetManager& operator=(const OffsetManager&) = delete;
    OffsetManager& operator=(OffsetManager&&) = delete;
    
    /// @brief Creates an offset manager.
    /// @param consumerManager The consumer manager for which we want to manage offsets.
    OffsetManager(corokafka::ConsumerManager& consumerManager);
    
    /// @brief Saves an offset to be committed later and potentially commits a range of offsets if it became available.
    /// @param offset The partition containing the offset to be saved.
    /// @param forceSync Force synchronous commits regardless of 'internal.consumer.commit.exec' consumer setting.
    /// @returns Error.
    cppkafka::Error saveOffset(const cppkafka::TopicPartition& offset,
                               bool forceSync = false) noexcept;

    /// @brief Saves an offset to be committed later and potentially commits a range of offsets if it became available.
    /// @param message The message whose offset we want to commit.
    /// @param forceSync Force synchronous commits regardless of 'internal.consumer.commit.exec' consumer setting.
    /// @returns Error.
    /// @remark Note that this will actually commit ReceivedMessage::getOffset()+1
    template <typename KEY, typename PAYLOAD, typename HEADERS>
    cppkafka::Error saveOffset(const ReceivedMessage<KEY,PAYLOAD,HEADERS>& message,
                               bool forceSync = false) noexcept;
    
    /// @brief Returns the smallest offset which is yet to be committed.
    /// @param partition The partition where this offset is.
    /// @return The offset.
    cppkafka::TopicPartition getCurrentOffset(const cppkafka::TopicPartition& partition) const;
    
    /// @brief Returns the beginning offset.
    /// @param partition The partition where this offset is.
    /// @return The offset.
    /// @note [getBeginOffset(), getCurrentOffset()) gives the total committed range of offsets.
    cppkafka::TopicPartition getBeginOffset(const cppkafka::TopicPartition& partition) const;
    
    /// @brief Commits the first available *lowest* offset range even if there are smaller offset(s) still pending.
    /// @param partition The partition where this offset is.
    /// @param ctx A coroutine synchronization context if this method is called from within a coroutine.
    /// @param forceSync Force synchronous commits regardless of 'internal.consumer.commit.exec' consumer setting.
    /// @warning Messages may be lost if the committed offsets were not yet complete and the application crashes.
    cppkafka::Error forceCommit(bool forceSync = false);
    cppkafka::Error forceCommit(const cppkafka::TopicPartition& partition,
                                bool forceSync = false);
    
    /// @brief Commit the lowest offset range as if by calling `saveOffset(getCurrentOffset(partition))`. This will
    ///        either commit the current offset or any range resulting by merging with the current offset.
    /// @param partition The partition where this offset is.
    /// @param ctx A coroutine synchronization context if this method is called from within a coroutine.
    /// @param forceSync Force synchronous commits regardless of 'internal.consumer.commit.exec' consumer setting.
    /// @warning Messages may be lost if the committed offsets were not yet complete and the application crashes.
    cppkafka::Error forceCommitCurrentOffset(bool forceSync = false);
    cppkafka::Error forceCommitCurrentOffset(const cppkafka::TopicPartition& partition,
                                             bool forceSync = false);
private:
    using OffsetMap = IntervalSet<int64_t>;
    using InsertReturnType = OffsetMap::InsertReturnType;
    using Iterator = OffsetMap::Iterator;
    struct OffsetsRanges {
        quantum::Mutex  _mutex;
        int64_t         _beginOffset{-1};
        int64_t         _currentOffset{-1};
        OffsetMap       _offsets;
        bool            _syncCommit{false};
    };
    using PartitionMap = std::unordered_map<int, OffsetsRanges>;
    using TopicMap = std::unordered_map<std::string, PartitionMap>;
    
    Range<int64_t> insertOffset(OffsetsRanges& ranges,
                                int64_t offset);
    
    template <typename PARTITIONS>
    cppkafka::Error commit(const PARTITIONS& partitions,
                           bool forceSync);

    void setStartingOffset(int64_t offset,
                           OffsetsRanges &ranges,
                           const cppkafka::TopicPartition& committedOffset,
                           const OffsetWatermark& watermark,
                           bool syncCommit,
                           bool autoResetEnd);

    const cppkafka::TopicPartition& findPartition(const cppkafka::TopicPartitionList& partitions,
                                                  int partition);
    const OffsetWatermark& findWatermark(const Metadata::OffsetWatermarkList& watermarks,
                                         int partition);

    // Members
    corokafka::ConsumerManager&     _consumerManager;
    TopicMap                        _topicMap;
};

// Implementation
template <typename KEY, typename PAYLOAD, typename HEADERS>
cppkafka::Error OffsetManager::saveOffset(const ReceivedMessage<KEY,PAYLOAD,HEADERS>& message,
                                          bool forceSync) noexcept {
    return saveOffset({message.getTopic(), message.getPartition(), message.getOffset()+1}, forceSync);
}

/// @brief RAII-type class for committing an offset within a scope.
class OffsetCommitGuard
{
public:
    /// @brief Saves an offset locally to be committed when this object goes out of scope.
    /// @param om The offset manager reference.
    /// @param offset The offset to be committed.
    /// @param forceSync Force synchronous commit even though the setting for this topic is async.
    OffsetCommitGuard(OffsetManager& om,
                      cppkafka::TopicPartition offset,
                      bool forceSync = false) :
        _om(om),
        _offset(std::move(offset)),
        _forceSync(forceSync)
    {}

    /// @brief Saves an offset locally to be committed when this object goes out of scope.
    /// @param om The offset manager reference.
    /// @param message The message whose offset we want to commit.
    /// @param forceSync Force synchronous commit even though the setting for this topic is async.
    /// @remark Note that this will actually commit ReceivedMessage::getOffset()+1
    template <typename KEY, typename PAYLOAD, typename HEADERS>
    OffsetCommitGuard(OffsetManager& om,
                      const ReceivedMessage<KEY,PAYLOAD,HEADERS>& message,
                      bool forceSync = false) :
            _om(om),
            _offset(message.getTopic(), message.getPartition(), message.getOffset()+1),
            _forceSync(forceSync)
    {}

    /// @brief Destructor. This will attempt to commit the offset.
    ~OffsetCommitGuard()
    {
        _om.saveOffset(_offset, _forceSync);
    }
private:
    OffsetManager&                      _om;
    const cppkafka::TopicPartition      _offset;
    bool                                _forceSync;
};

// Implementations
template <typename PARTITIONS>
cppkafka::Error OffsetManager::commit(const PARTITIONS& partitions,
                                      bool forceSync)
{
    return _consumerManager.commit(partitions, nullptr, forceSync);
}

}}

#endif //BLOOMBERG_COROKAFKA_OFFSET_MANAGER_H
