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
    MOCK_CONST_METHOD0(getTopicPartitions, const cppkafka::TopicPartitionList&());
    MOCK_CONST_METHOD0(getOutboundQueueLength, size_t());
    MOCK_CONST_METHOD0(getInternalQueueLength, size_t());
};

}}}

#endif //BLOOMBERG_COROKAFKA_PRODUCER_METADATA_MOCK_H
