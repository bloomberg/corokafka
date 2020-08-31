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
#ifndef BLOOMBERG_COROKAFKA_METADATA_IMPL_H
#define BLOOMBERG_COROKAFKA_METADATA_IMPL_H

#include <corokafka/corokafka_utils.h>
#include <corokafka/interface/corokafka_imetadata.h>

namespace Bloomberg {
namespace corokafka {

class MetadataImpl : public virtual IMetadata {
public:
    /**
     * @brief Constructors and assignment operators
     */
    MetadataImpl(const std::string& topic,
                 const cppkafka::Topic& kafkaTopic,
                 cppkafka::KafkaHandleBase* handle,
                 std::chrono::milliseconds brokerTimeout);
    MetadataImpl(const MetadataImpl&) = delete;
    MetadataImpl(MetadataImpl&&) = default;
    MetadataImpl& operator=(const MetadataImpl&) = delete;
    MetadataImpl& operator=(MetadataImpl&&) = default;
    
    KafkaType getType() const override;
    
    OffsetWatermarkList queryOffsetWatermarks() const final;
    
    OffsetWatermarkList queryOffsetWatermarks(std::chrono::milliseconds timeout) const override;
    
    cppkafka::TopicPartitionList queryOffsetsAtTime(Timestamp timestamp) const final;
    
    cppkafka::TopicPartitionList queryOffsetsAtTime(Timestamp timestamp,
                                                    std::chrono::milliseconds timeout) const override;
    explicit operator bool() const final;
    
    uint64_t getHandle() const final;
    
    const std::string& getTopic() const final;
    
    cppkafka::TopicMetadata getTopicMetadata() const final;
    
    cppkafka::TopicMetadata getTopicMetadata(std::chrono::milliseconds timeout) const final;
    
    std::string getInternalName() const final;
    
    bool isPartitionAvailable(int partition) const final;
    
    const cppkafka::Topic& getTopicObject() const final;
    
protected:
    std::chrono::milliseconds brokerTimeout() const;
    
    const std::string*                      _topic;
    cppkafka::KafkaHandleBase*              _handle;
    mutable cppkafka::Topic                 _kafkaTopic;
    mutable cppkafka::TopicPartitionList    _partitions;
    std::chrono::milliseconds               _brokerTimeout;
};

}}

#endif //BLOOMBERG_COROKAFKA_METADATA_IMPL_H
