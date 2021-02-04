#ifndef BLOOMBERG_COROKAFKA_OFFSET_COMMIT_GUARD_H
#define BLOOMBERG_COROKAFKA_OFFSET_COMMIT_GUARD_H

#include <corokafka/utils/corokafka_offset_manager.h>

namespace Bloomberg {
namespace corokafka {

/// @brief RAII-type class for committing an offset within a scope.
class OffsetCommitGuard {
public:
    /// @brief Saves an offset locally to be committed when this object goes out of scope.
    /// @param om The offset manager reference.
    /// @param offset The offset to be committed.
    /// @param execMode If specified, overrides 'internal.consumer.commit.exec' consumer setting.
    OffsetCommitGuard(OffsetManager &om,
                      cppkafka::TopicPartition offset) :
            _om(om),
            _offset(std::move(offset)) {}
    
    OffsetCommitGuard(OffsetManager &om,
                      cppkafka::TopicPartition offset,
                      ExecMode execMode) :
            _om(om),
            _offset(std::move(offset)),
            _execMode(execMode),
            _useExecMode(true) {}
    
    /// @brief Saves an offset locally to be committed when this object goes out of scope.
    /// @param om The offset manager reference.
    /// @param message The message whose offset we want to commit.
    /// @param execMode If specified, overrides 'internal.consumer.commit.exec' consumer setting.
    /// @remark Note that this will actually commit ReceivedMessage::getOffset()+1
    template<typename KEY, typename PAYLOAD, typename HEADERS>
    OffsetCommitGuard(OffsetManager &om,
                      const ReceivedMessage<KEY, PAYLOAD, HEADERS> &message) :
            _om(om),
            _offset(message.getTopic(), message.getPartition(), message.getOffset() + 1) {}
    
    template<typename KEY, typename PAYLOAD, typename HEADERS>
    OffsetCommitGuard(OffsetManager &om,
                      const ReceivedMessage<KEY, PAYLOAD, HEADERS> &message,
                      ExecMode execMode) :
            _om(om),
            _offset(message.getTopic(), message.getPartition(), message.getOffset() + 1),
            _execMode(execMode),
            _useExecMode(true) {}
    
    /// @brief Destructor. This will attempt to commit the offset.
    ~OffsetCommitGuard() {
        if (_useExecMode) {
            _om.saveOffset(_offset, _execMode);
        } else {
            _om.saveOffset(_offset);
        }
    }

private:
    IOffsetManager &_om;
    const cppkafka::TopicPartition _offset;
    ExecMode _execMode{ExecMode::Async};
    bool _useExecMode{false};
};

}}

#endif //BLOOMBERG_COROKAFKA_OFFSET_COMMIT_GUARD_H
