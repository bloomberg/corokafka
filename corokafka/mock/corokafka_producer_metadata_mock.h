#ifndef BLOOMBERG_COROKAFKA_PRODUCER_METADATA_MOCK_H
#define BLOOMBERG_COROKAFKA_PRODUCER_METADATA_MOCK_H

#include <corokafka/interface/corokafka_iproducer_metadata.h>
#include <corokafka/mock/corokafka_metadata_mock.h>
#include <gmock/gmock.h>

namespace Bloomberg {
namespace corokafka {
namespace mocks {

struct ProducerMetadataMock : public IProducerMetadata,
                              public MetadataMock //mock of IMetadata
{
    explicit ProducerMetadataMock(cppkafka::TopicPartitionList topics = cppkafka::TopicPartitionList{}) :
        _topics(std::move(topics))
    {
        //Set default actions
        using namespace testing;
        ON_CALL(*this, getTopicPartitions())
                .WillByDefault(ReturnRef(_topics));
    }
    MOCK_CONST_METHOD0(getTopicPartitions, const cppkafka::TopicPartitionList&());
    MOCK_CONST_METHOD0(getOutboundQueueLength, size_t());
    MOCK_CONST_METHOD0(getInternalQueueLength, size_t());
    
private:
    cppkafka::TopicPartitionList _topics;
};

}}}

#endif //BLOOMBERG_COROKAFKA_PRODUCER_METADATA_MOCK_H
