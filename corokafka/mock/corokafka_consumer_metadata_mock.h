#ifndef BLOOMBERG_COROKAFKA_CONSUMER_METADATA_MOCK_H
#define BLOOMBERG_COROKAFKA_CONSUMER_METADATA_MOCK_H

#include <corokafka/interface/corokafka_iconsumer_metadata.h>
#include <corokafka/mock/corokafka_metadata_mock.h>
#include <gtest/gtest.h>
#include <gmock/gmock.h>

namespace Bloomberg {
namespace corokafka {
namespace mocks {

struct ConsumerMetadataMock : public IConsumerMetadata,
                              public MetadataMock //mock of IMetadata
{
    ConsumerMetadataMock(cppkafka::TopicPartitionList list = cppkafka::TopicPartitionList{}) :
        _partitionList(std::move(list))
    {
        using namespace testing;
        ON_CALL(*this, getPartitionAssignment())
            .WillByDefault(ReturnRef(_partitionList));
    }
    MOCK_CONST_METHOD0(getOffsetWatermarks, OffsetWatermarkList());
    MOCK_CONST_METHOD0(queryCommittedOffsets, cppkafka::TopicPartitionList());
    MOCK_CONST_METHOD1(queryCommittedOffsets, cppkafka::TopicPartitionList(std::chrono::milliseconds));
    MOCK_CONST_METHOD0(getOffsetPositions, cppkafka::TopicPartitionList());
    MOCK_CONST_METHOD0(getPartitionAssignment, const cppkafka::TopicPartitionList&());
    MOCK_CONST_METHOD0(getGroupInformation, cppkafka::GroupInformation());
    MOCK_CONST_METHOD1(getGroupInformation, cppkafka::GroupInformation(std::chrono::milliseconds));
    MOCK_CONST_METHOD0(getAllGroupsInformation, cppkafka::GroupInformationList());
    MOCK_CONST_METHOD1(getAllGroupsInformation, cppkafka::GroupInformationList(std::chrono::milliseconds));
    MOCK_CONST_METHOD0(getPartitionStrategy, PartitionStrategy());
private:
    cppkafka::TopicPartitionList _partitionList;
};

}}}

#endif //BLOOMBERG_COROKAFKA_CONSUMER_METADATA_MOCK_H
