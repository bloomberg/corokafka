#ifndef BLOOMBERG_COROKAFKA_IOFFSET_MANAGER_H
#define BLOOMBERG_COROKAFKA_IOFFSET_MANAGER_H

#include <corokafka/utils/corokafka_interval_set.h>
#include <corokafka/corokafka_consumer_manager.h>
#include <corokafka/corokafka_received_message.h>
#include <corokafka/corokafka_utils.h>
#include <quantum/quantum.h>
#include <unordered_map>
#include <chrono>

namespace Bloomberg {
namespace corokafka {

struct IOffsetManager
{
    enum class ResetAction : int {
        FetchOffsets,       //Fetch new committed offsets and watermarks from broker
        DoNotFetchOffsets   //Don't fetch any offsets
    };
    
    virtual ~IOffsetManager() = default;
    virtual cppkafka::Error saveOffset(const cppkafka::TopicPartition& offset) = 0;
    virtual cppkafka::Error saveOffset(const cppkafka::TopicPartition& offset,
                                       ExecMode execMode) = 0;
    virtual cppkafka::Error saveOffset(const IMessage& message) = 0;
    virtual cppkafka::Error saveOffset(const IMessage& message,
                                       ExecMode execMode) = 0;
    virtual cppkafka::TopicPartition getCurrentOffset(const cppkafka::TopicPartition& partition) = 0;
    virtual cppkafka::TopicPartition getBeginOffset(const cppkafka::TopicPartition& partition) = 0;
    virtual cppkafka::Error forceCommit() = 0;
    virtual cppkafka::Error forceCommit(ExecMode execMode) = 0;
    virtual cppkafka::Error forceCommit(const cppkafka::TopicPartition& partition) = 0;
    virtual cppkafka::Error forceCommit(const cppkafka::TopicPartition& partition,
                                        ExecMode execMode) = 0;
    virtual cppkafka::Error forceCommitCurrentOffset() = 0;
    virtual cppkafka::Error forceCommitCurrentOffset(ExecMode execMode) = 0;
    virtual cppkafka::Error forceCommitCurrentOffset(const cppkafka::TopicPartition& partition) = 0;
    virtual cppkafka::Error forceCommitCurrentOffset(const cppkafka::TopicPartition& partition,
                                                     ExecMode execMode) = 0;
    virtual void resetPartitionOffsets(ResetAction action) = 0;
    virtual void resetPartitionOffsets(const std::string& topic,
                                       ResetAction action) = 0;
    virtual std::string toString() const = 0;
    virtual std::string toString(const std::string& topic) const = 0;
    virtual void enableCommitTracing(bool enable) = 0;
};

}}

#endif //BLOOMBERG_COROKAFKA_IOFFSET_MANAGER_H
