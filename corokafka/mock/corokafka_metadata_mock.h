/*
** Copyright 2020 Bloomberg Finance L.P.
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
#ifndef BLOOMBERG_COROKAFKA_METADATA_MOCK_H
#define BLOOMBERG_COROKAFKA_METADATA_MOCK_H

#include <corokafka/interface/corokafka_imetadata.h>
#include <gtest/gtest.h>
#include <gmock/gmock.h>

namespace Bloomberg {
namespace corokafka {
namespace mocks {

struct MetadataMock : public virtual IMetadata
{
    explicit MetadataMock(std::string topic = "MockTopic",
                          cppkafka::Topic topicObject = cppkafka::Topic{}) :
        _topic(std::move(topic)),
        _topicObject(std::move(topicObject))
    {
        using namespace testing;
        ON_CALL(*this, getType())
            .WillByDefault(Return(KafkaType::Consumer));
        ON_CALL(*this, getTopic())
            .WillByDefault(ReturnRef(_topic));
        ON_CALL(*this, getTopicObject())
            .WillByDefault(ReturnRef(_topicObject));
    }
    MOCK_CONST_METHOD0(getType, KafkaType());
    MOCK_CONST_METHOD0(queryOffsetWatermarks, OffsetWatermarkList());
    MOCK_CONST_METHOD1(queryOffsetWatermarks, OffsetWatermarkList(std::chrono::milliseconds));
    MOCK_CONST_METHOD1(queryOffsetsAtTime, cppkafka::TopicPartitionList(Timestamp));
    MOCK_CONST_METHOD2(queryOffsetsAtTime, cppkafka::TopicPartitionList(Timestamp, std::chrono::milliseconds));
    MOCK_CONST_METHOD0(operatorBool, bool());
    explicit operator bool() const final { return operatorBool(); }
    MOCK_CONST_METHOD0(getHandle, uint64_t());
    MOCK_CONST_METHOD0(getTopic, const std::string&());
    MOCK_CONST_METHOD0(getTopicObject, const cppkafka::Topic&());
    MOCK_CONST_METHOD0(getTopicMetadata, cppkafka::TopicMetadata());
    MOCK_CONST_METHOD1(getTopicMetadata, cppkafka::TopicMetadata(std::chrono::milliseconds));
    MOCK_CONST_METHOD0(getInternalName, std::string());
    MOCK_CONST_METHOD1(isPartitionAvailable, bool(int));
private:
    std::string     _topic;
    cppkafka::Topic _topicObject;
};

}}}
#endif //BLOOMBERG_COROKAFKA_METADATA_MOCK_H
