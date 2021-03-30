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

#include <corokafka/interface/corokafka_ioffset_manager.h>
#include <corokafka/interface/corokafka_impl.h>

namespace Bloomberg {
namespace corokafka {

/// @brief The OffsetManager class helps commit offsets in a concurrent environment
///        by guaranteeing gapless ordering. When offsets are consumed on multiple
///        threads, some messages may be processed out-of-order and committing them
///        before others may result in message loss should a crash occur.
class OffsetManager : public Impl<IOffsetManager>
{
public:
    using ResetAction = IOffsetManager::ResetAction;

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
    cppkafka::Error saveOffset(const cppkafka::TopicPartition& offset) override;
    cppkafka::Error saveOffset(const cppkafka::TopicPartition& offset,
                               ExecMode execMode) override;

    /// @brief Saves an offset to be committed later and potentially commits a range of offsets if it became available.
    /// @param message The message whose offset we want to commit.
    /// @param execMode If specified, overrides 'internal.consumer.commit.exec' consumer setting.
    /// @returns Error.
    /// @remark Note that this will actually commit IMessage::getOffset()+1
    cppkafka::Error saveOffset(const IMessage& message) override;
    cppkafka::Error saveOffset(const IMessage& message,
                               ExecMode execMode) override;
    
    /// @brief Returns the smallest offset which is yet to be committed.
    /// @param partition The partition where this offset is.
    /// @return The offset.
    cppkafka::TopicPartition getCurrentOffset(const cppkafka::TopicPartition& partition) override;
    
    /// @brief Returns the beginning offset.
    /// @param partition The partition where this offset is.
    /// @return The offset.
    /// @note [getBeginOffset(), getCurrentOffset()) gives the total committed range of offsets.
    cppkafka::TopicPartition getBeginOffset(const cppkafka::TopicPartition& partition) override;
    
    /// @brief Commits the first available *lowest* offset range even if there are smaller offset(s) still pending.
    /// @param partition The partition where this offset is, or all partitions if not specified.
    /// @param execMode If specified, overrides 'internal.consumer.commit.exec' consumer setting.
    /// @warning Messages may be lost if the committed offsets were not yet complete and the application crashes.
    cppkafka::Error forceCommit() override;
    cppkafka::Error forceCommit(ExecMode execMode) override;
    cppkafka::Error forceCommit(const cppkafka::TopicPartition& partition) override;
    cppkafka::Error forceCommit(const cppkafka::TopicPartition& partition,
                                ExecMode execMode) override;
    
    /// @brief Commit the lowest offset range as if by calling `saveOffset(getCurrentOffset(partition))`. This will
    ///        either commit the current offset or any range resulting by merging with the current offset.
    /// @param partition The partition where this offset is, or all partitions if not specified.
    /// @param execMode If specified, overrides 'internal.consumer.commit.exec' consumer setting.
    /// @warning Messages may be lost if the committed offsets were not yet complete and the application crashes.
    cppkafka::Error forceCommitCurrentOffset() override;
    cppkafka::Error forceCommitCurrentOffset(ExecMode execMode) override;
    cppkafka::Error forceCommitCurrentOffset(const cppkafka::TopicPartition& partition) override;
    cppkafka::Error forceCommitCurrentOffset(const cppkafka::TopicPartition& partition,
                                             ExecMode execMode) override;
    
    /// @brief Reset all partition offsets for the specified topic.
    /// @param topic The topic for which all partitions and offsets will be reset.
    ///              If topic is not present, then reset all topic partitions.
    /// @param action Determines if new offsets should be fetched or not.
    /// @note Call this when a new partition assignment has been made for this topic.
    /// @note This call **MUST** be made with exclusive access (i.e. when no other threads access
    ///       the ConsumerManager).
    /// @note May throw.
    void resetPartitionOffsets(ResetAction action = ResetAction::FetchOffsets) override;
    void resetPartitionOffsets(const std::string& topic,
                               ResetAction action = ResetAction::FetchOffsets) override;
    
    /// @brief Output the offsets contained by this manager.
    /// @param topic Restrict output to a specific topic only.
    std::string toString() const override;
    std::string toString(const std::string& topic) const override;
    
    /// @brief Enables or disables commit logging via the registered
    ///        log callback in the ConsumerManager at the specified severity level.
    /// @warning May be verbose
    void enableCommitTracing(bool enable,
                             cppkafka::LogLevel level = cppkafka::LogLevel::LogDebug) override;
    
    /**
     * @brief For mocking only via dependency injection.
     */
    using ImplType = Impl<IOffsetManager>;
    using ImplType::ImplType;
};

/// @brief Stream operator for the OffsetManager class
std::ostream& operator<<(std::ostream& output, const OffsetManager& rhs);

}}

#include <corokafka/utils/corokafka_offset_commit_guard.h>

#endif //BLOOMBERG_COROKAFKA_OFFSET_MANAGER_H
