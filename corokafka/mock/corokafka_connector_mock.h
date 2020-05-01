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
#ifndef BLOOMBERG_COROKAFKA_CONNECTOR_MOCK_H
#define BLOOMBERG_COROKAFKA_CONNECTOR_MOCK_H

#include <corokafka/interface/corokafka_iconnector.h>
#include <corokafka/mock/corokafka_producer_manager_mock.h>
#include <corokafka/mock/corokafka_consumer_manager_mock.h>
#include <gmock/gmock.h>
#include <memory>

namespace Bloomberg {
namespace corokafka {
namespace mocks {

struct ConnectorMock : public IConnector
{
    ConnectorMock() :
        _producerMockPtr(std::make_shared<ProducerManagerMock>()),
        _consumerMockPtr(std::make_shared<ConsumerManagerMock>()),
        _producer(_producerMockPtr),
        _consumer(_consumerMockPtr)
    {
        using namespace testing;
        ON_CALL(*this, producer())
            .WillByDefault(ReturnRef(_producer));
        ON_CALL(*this, consumer())
            .WillByDefault(ReturnRef(_consumer));
    }
    
    MOCK_METHOD0(producer, ProducerManager&());
    MOCK_METHOD0(consumer, ConsumerManager&());
    MOCK_METHOD0(shutdown, void());
    MOCK_METHOD1(shutdown, void(std::chrono::milliseconds));
    
    ProducerManagerMock& producerMock() {
        return *_producerMockPtr;
    }
    
    ConsumerManagerMock& consumerMock() {
        return *_consumerMockPtr;
    }
    
private:
    std::shared_ptr<ProducerManagerMock>    _producerMockPtr;
    std::shared_ptr<ConsumerManagerMock>    _consumerMockPtr;
    ProducerManager        _producer;
    ConsumerManager        _consumer;
};

}}}

#endif //BLOOMBERG_COROKAFKA_CONNECTOR_MOCK_H
