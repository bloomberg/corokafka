#ifndef BLOOMBERG_COROKAFKA_ICONSUMER_METADATA_H
#define BLOOMBERG_COROKAFKA_ICONSUMER_METADATA_H

#include <corokafka/corokafka_utils.h>
#include <corokafka/interface/corokafka_imetadata.h>
#include <corokafka/corokafka_offset_watermark.h>
#include <cppkafka/topic_partition.h>
#include <cppkafka/topic_partition_list.h>
#include <cppkafka/group_information.h>

namespace Bloomberg {
namespace corokafka {

struct IConsumerMetadata : public virtual IMetadata
{
    virtual ~IConsumerMetadata() = default;
    virtual OffsetWatermarkList getOffsetWatermarks() const = 0;
    virtual cppkafka::TopicPartitionList queryCommittedOffsets() const = 0;
    virtual cppkafka::TopicPartitionList queryCommittedOffsets(std::chrono::milliseconds timeout) const = 0;
    virtual cppkafka::TopicPartitionList getOffsetPositions() const = 0;
    virtual const cppkafka::TopicPartitionList& getPartitionAssignment() const = 0;
    virtual cppkafka::GroupInformation getGroupInformation() const = 0;
    virtual cppkafka::GroupInformation getGroupInformation(std::chrono::milliseconds timeout) const = 0;
    virtual cppkafka::GroupInformationList getAllGroupsInformation() const = 0;
    virtual cppkafka::GroupInformationList getAllGroupsInformation(std::chrono::milliseconds timeout) const = 0;
    virtual PartitionStrategy getPartitionStrategy() const = 0;
};

}
}

#endif //BLOOMBERG_COROKAFKA_ICONSUMER_METADATA_H
