#ifndef BLOOMBERG_COROKAFKA_OFFSET_MANAGER_MOCK_H
#define BLOOMBERG_COROKAFKA_OFFSET_MANAGER_MOCK_H

#include <corokafka/interface/corokafka_ioffset_manager.h>
#include <gmock/gmock.h>

namespace Bloomberg {
namespace corokafka {
namespace mocks {

struct OffsetManagerMock : public IOffsetManager
{
    MOCK_METHOD1(saveOffset, cppkafka::Error(const cppkafka::TopicPartition&));
    MOCK_METHOD2(saveOffset, cppkafka::Error(const cppkafka::TopicPartition&, ExecMode));
    MOCK_METHOD1(saveOffset, cppkafka::Error(const IMessage&));
    MOCK_METHOD2(saveOffset, cppkafka::Error(const IMessage&, ExecMode));
    MOCK_METHOD1(getCurrentOffset, cppkafka::TopicPartition(const cppkafka::TopicPartition&));
    MOCK_METHOD1(getBeginOffset, cppkafka::TopicPartition(const cppkafka::TopicPartition&));
    MOCK_METHOD0(forceCommit, cppkafka::Error());
    MOCK_METHOD1(forceCommit, cppkafka::Error(ExecMode));
    MOCK_METHOD1(forceCommit, cppkafka::Error(const cppkafka::TopicPartition&));
    MOCK_METHOD2(forceCommit, cppkafka::Error(const cppkafka::TopicPartition&, ExecMode));
    MOCK_METHOD0(forceCommitCurrentOffset, cppkafka::Error());
    MOCK_METHOD1(forceCommitCurrentOffset, cppkafka::Error(ExecMode));
    MOCK_METHOD1(forceCommitCurrentOffset, cppkafka::Error(const cppkafka::TopicPartition&));
    MOCK_METHOD2(forceCommitCurrentOffset, cppkafka::Error(const cppkafka::TopicPartition&, ExecMode));
    MOCK_METHOD1(resetPartitionOffsets, void(IOffsetManager::ResetAction));
    MOCK_METHOD2(resetPartitionOffsets, void(const std::string&, ResetAction));
    MOCK_CONST_METHOD0(toString, std::string());
    MOCK_CONST_METHOD1(toString, std::string(const std::string&));
    MOCK_METHOD1(enableCommitTracing, void(bool));
    MOCK_METHOD2(enableCommitTracing, void(bool, cppkafka::LogLevel));
};

}}}

#endif //BLOOMBERG_COROKAFKA_OFFSET_MANAGER_MOCK_H
