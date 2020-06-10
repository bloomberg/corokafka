#ifndef BLOOMBERG_COROKAFKA_IPRODUCER_METADATA_H
#define BLOOMBERG_COROKAFKA_IPRODUCER_METADATA_H

#include <corokafka/interface/corokafka_imetadata.h>
#include <cppkafka/topic_partition_list.h>

namespace Bloomberg {
namespace corokafka {

struct IProducerMetadata : public virtual IMetadata
{
    virtual ~IProducerMetadata() = default;
    virtual const cppkafka::TopicPartitionList& getTopicPartitions() const = 0;
    virtual size_t getOutboundQueueLength() const = 0;
    virtual size_t getInternalQueueLength() const = 0;
};

}
}

#endif //BLOOMBERG_COROKAFKA_IPRODUCER_METADATA_H
