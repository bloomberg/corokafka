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
#ifndef BLOOMBERG_COROKAFKA_CONSUMER_MANGER_MOCK_H
#define BLOOMBERG_COROKAFKA_CONSUMER_MANGER_MOCK_H

#include <corokafka/interface/corokafka_iconsumer_manager.h>
#include <corokafka/mock/corokafka_consumer_manager_mock.h>
#include <corokafka/mock/corokafka_topic_mock.h>
#include <corokafka/mock/corokafka_consumer_metadata_mock.h>
#include <corokafka/corokafka_consumer_metadata.h>
#include <gmock/gmock.h>
#include <memory>

namespace Bloomberg {
namespace corokafka {
namespace mocks {

struct ConsumerManagerMock : public IConsumerManager
{
    ConsumerManagerMock() :
        _config(MockTopic{"MockTopic"}, {}, {}, ConsumerManagerMock::receiver),
        _consumerMetadataMockPtr(std::make_shared<ConsumerMetadataMock>())
    {
        //Set default actions
        using namespace testing;
        ConsumerMetadata consumerMetadata(_consumerMetadataMockPtr);
        ON_CALL(*this, getMetadata(_))
            .WillByDefault(Return(consumerMetadata));
        ON_CALL(*this, getConfiguration(_))
                .WillByDefault(ReturnRef(_config));
        ON_CALL(*this, getTopics())
                .WillByDefault(Return(std::vector<std::string>{MockTopic{"MockTopic"}.topic()}));
    }
    
    MOCK_METHOD0(pause, void());
    MOCK_METHOD1(pause, void(const std::string&));
    MOCK_METHOD0(resume, void());
    MOCK_METHOD1(resume, void(const std::string&));
    MOCK_METHOD1(subscribe, void(const cppkafka::TopicPartitionList&));
    MOCK_METHOD2(subscribe, void(const std::string&, const cppkafka::TopicPartitionList&));
    MOCK_METHOD0(unsubscribe, void());
    MOCK_METHOD1(unsubscribe, void(const std::string&));
    MOCK_METHOD2(commitPartition, cppkafka::Error(const cppkafka::TopicPartition&,
                                                  const void*));
    MOCK_METHOD3(commitPartition, cppkafka::Error(const cppkafka::TopicPartition&,
                                                  ExecMode,
                                                  const void*));
    MOCK_METHOD2(commitPartitionList, cppkafka::Error(const cppkafka::TopicPartitionList&,
                                                      const void*));
    MOCK_METHOD3(commitPartitionList, cppkafka::Error(const cppkafka::TopicPartitionList&,
                                                      ExecMode,
                                                      const void*));
    MOCK_METHOD0(shutdown, void());
    MOCK_METHOD1(getMetadata, ConsumerMetadata(const std::string&));
    MOCK_METHOD0(enablePreprocessing, void());
    MOCK_METHOD1(enablePreprocessing, void(const std::string&));
    MOCK_METHOD0(disablePreprocessing, void());
    MOCK_METHOD1(disablePreprocessing, void(const std::string &topic));
    MOCK_CONST_METHOD1(getConfiguration, const ConsumerConfiguration&(const std::string&));
    MOCK_CONST_METHOD0(getTopics, std::vector<std::string>());
    MOCK_METHOD0(poll, void());
    MOCK_METHOD0(pollEnd, void());
    
    cppkafka::Error commit(const cppkafka::TopicPartition &topicPartition,
                           const void *opaque = nullptr) {
        return commitPartition(topicPartition, opaque);
    }
    
    cppkafka::Error commit(const cppkafka::TopicPartition &topicPartition,
                           ExecMode execMode,
                           const void *opaque = nullptr) {
        return commitPartition(topicPartition, execMode, opaque);
    }
    
    cppkafka::Error commit(const cppkafka::TopicPartitionList &topicPartitions,
                           ExecMode execMode,
                           const void *opaque = nullptr) {
        return commitPartitionList(topicPartitions, execMode, opaque);
    }
    
    cppkafka::Error commit(const cppkafka::TopicPartitionList &topicPartitions,
                           const void *opaque = nullptr) {
        return commitPartitionList(topicPartitions, opaque);
    }
    
    static void receiver(MockTopic::ReceivedMessageType) {}
    
    ConsumerMetadataMock& consumerMetadataMock() {
        return *_consumerMetadataMockPtr;
    }

private:
    ConsumerConfiguration _config;
    std::shared_ptr<ConsumerMetadataMock> _consumerMetadataMockPtr;
};

}}}

#endif //BLOOMBERG_COROKAFKA_CONSUMER_MANGER_MOCK_H
