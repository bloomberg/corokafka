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
#ifndef BLOOMBERG_COROKAFKA_PRODUCER_MANAGER_MOCK_H
#define BLOOMBERG_COROKAFKA_PRODUCER_MANAGER_MOCK_H

#include <corokafka/interface/corokafka_iproducer_manager.h>
#include <corokafka/mock/corokafka_topic_mock.h>
#include <corokafka/mock/corokafka_producer_metadata_mock.h>
#include <corokafka/corokafka_producer_metadata.h>
#include <gmock/gmock.h>
#include <memory>

namespace Bloomberg {
namespace corokafka{
namespace mocks {

struct ProducerManagerMock : public IProducerManager
{
    explicit ProducerManagerMock(ProducerConfiguration config = ProducerConfiguration(MockTopic{"MockTopic"}, {}, {})) :
        _config(std::move(config)),
        _producerMetadataMockPtr(std::make_shared<ProducerMetadataMock>())
    {
        //Set default actions
        using namespace testing;
        ProducerMetadata producerMetadata(_producerMetadataMockPtr);
        ON_CALL(*this, getMetadata(_))
            .WillByDefault(Return(producerMetadata));
        ON_CALL(*this, getConfiguration(_))
                .WillByDefault(ReturnRef(_config));
        ON_CALL(*this, getTopics())
                .WillByDefault(Return(std::vector<std::string>{MockTopic{"MockTopic"}.topic()}));
    }
    
    MOCK_METHOD0(send, DeliveryReport());
    MOCK_METHOD0(post, quantum::GenericFuture<DeliveryReport>());
    MOCK_METHOD1(waitForAcks, bool(const std::string&));
    MOCK_METHOD2(waitForAcks, bool(const std::string&, std::chrono::milliseconds));
    MOCK_METHOD0(shutdown, void());
    MOCK_METHOD1(getMetadata, ProducerMetadata(const std::string&));
    MOCK_CONST_METHOD1(getConfiguration, const ProducerConfiguration&(const std::string&));
    MOCK_CONST_METHOD0(getTopics, std::vector<std::string>());
    MOCK_METHOD1(resetQueueFullTrigger, void(const std::string& topic));
    MOCK_METHOD0(poll, void());
    MOCK_METHOD0(pollEnd, void());
    
    ProducerMetadataMock& producerMetadataMock() {
        return *_producerMetadataMockPtr;
    }
    
    static void setDefaultDeliveryReport(DeliveryReport dr) {
        s_deliveryReport = std::move(dr);
        //This returns the default DeliveryReport on each call to send()
        testing::DefaultValue<DeliveryReport>::Set(s_deliveryReport);
        
        //This will create a new promise/future pair on each call to post()
        testing::DefaultValue<quantum::GenericFuture<DeliveryReport>>::SetFactory(
            []()->quantum::GenericFuture<DeliveryReport> {
                s_promisePtr = std::make_unique<quantum::Promise<DeliveryReport>>();
                s_promisePtr->set(s_deliveryReport); //set the promise
                return s_promisePtr->getIThreadFuture();
            });
    }
    
    static void clearDefaultDeliveryReport() {
        s_deliveryReport = {};
        testing::DefaultValue<DeliveryReport>::Clear();
        testing::DefaultValue<quantum::GenericFuture<DeliveryReport>>::Clear();
    }
    
private:
    ProducerConfiguration   _config;
    std::shared_ptr<ProducerMetadataMock> _producerMetadataMockPtr;
    static DeliveryReport   s_deliveryReport;
    static std::unique_ptr<quantum::Promise<DeliveryReport>> s_promisePtr;
};

inline DeliveryReport ProducerManagerMock::s_deliveryReport;
inline std::unique_ptr<quantum::Promise<DeliveryReport>> ProducerManagerMock::s_promisePtr;

} //namespace mocks

class ProducerManagerProxy
{
public:
    ProducerManagerProxy(ProducerManager *consumer) :
            _concrete(consumer) {}
    
    ProducerManagerProxy(mocks::ProducerManagerMock *mock) :
            _mock(mock) {}
            
    template <typename TOPIC, typename K, typename P, typename ...H>
    DeliveryReport send(const TOPIC& topic,
                        const void* opaque,
                        const K& key,
                        const P& payload,
                        const H&...headers) {
        if (_concrete) {
            return _concrete->send(topic, opaque, key, payload, headers...);
        }
        else if (_mock) {
            return _mock->send();
        }
        assert(false);
    }
    
    template <typename TOPIC, typename K, typename P, typename ...H>
    quantum::GenericFuture<DeliveryReport>
    post(const TOPIC& topic,
         const void* opaque,
         K&& key,
         P&& payload,
         H&&...headers)
    {
        if (_concrete) {
            return _concrete->post(topic, opaque, std::forward<K>(key), std::forward<P>(payload), std::forward<H>(headers)...);
        }
        else if (_mock) {
            return _mock->post();
        }
        assert(false);
    }
    
    bool waitForAcks(const std::string& topic)
    {
        if (_concrete) {
            return _concrete->waitForAcks(topic);
        }
        else if (_mock) {
            return _mock->waitForAcks(topic);
        }
        assert(false);
    }
    
    bool waitForAcks(const std::string& topic,
                     std::chrono::milliseconds timeout)
    {
        if (_concrete) {
            return _concrete->waitForAcks(topic, timeout);
        }
        else if (_mock) {
            return _mock->waitForAcks(topic, timeout);
        }
        assert(false);
    }
    
    void shutdown()
    {
        if (_concrete) {
            _concrete->shutdown();
        }
        else if (_mock) {
            _mock->shutdown();
        }
        assert(false);
    }
    
    ProducerMetadata getMetadata(const std::string& topic)
    {
        if (_concrete) {
            return _concrete->getMetadata(topic);
        }
        else if (_mock) {
            return _mock->getMetadata(topic);
        }
        assert(false);
    }
    
    const ProducerConfiguration& getConfiguration(const std::string& topic) const
    {
        if (_concrete) {
            return _concrete->getConfiguration(topic);
        }
        else if (_mock) {
            return _mock->getConfiguration(topic);
        }
        assert(false);
    }
    
    std::vector<std::string> getTopics() const
    {
        if (_concrete) {
            return _concrete->getTopics();
        }
        else if (_mock) {
            return _mock->getTopics();
        }
        assert(false);
    }
    
    void resetQueueFullTrigger(const std::string& topic)
    {
        if (_concrete) {
            _concrete->resetQueueFullTrigger(topic);
        }
        else if (_mock) {
            _mock->resetQueueFullTrigger(topic);
        }
        assert(false);
    }
    
private:
    ProducerManager*                _concrete{nullptr};
    mocks::ProducerManagerMock*     _mock{nullptr};
};

}
}

#endif //BLOOMBERG_COROKAFKA_PRODUCER_MANAGER_MOCK_H
