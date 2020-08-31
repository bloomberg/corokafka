/*
** Copyright 2019 Bloomberg Finance L.P.
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
#ifndef BLOOMBERG_COROKAFKA_METADATA_H
#define BLOOMBERG_COROKAFKA_METADATA_H

#include <corokafka/corokafka_utils.h>
#include <corokafka/interface/corokafka_impl.h>
#include <corokafka/interface/corokafka_imetadata.h>

namespace Bloomberg {
namespace corokafka {

//=============================================================================
//                                 METADATA
//=============================================================================
class Metadata : public VirtualImpl<IMetadata>
{
public:
    /**
     * @brief Get the metadata type.
     * @return The type.
     */
    KafkaType getType() const final;
    
    /**
     * @brief Query the remote broker for all offset watermarks.
     * @return A list of offset watermarks belonging to this consumer or producer.
     * @remark This method blocks until offset data is received. It is preferable *not* to call this
     *         method from within a callback.
     */
    OffsetWatermarkList queryOffsetWatermarks() const final;
    OffsetWatermarkList queryOffsetWatermarks(std::chrono::milliseconds) const final;
    
    /**
     * @brief Query the remote broker for all offsets newer than timestamp.
     * @param timestamp The timestamp.
     * @return A partition list containing all offsets.
     * @remark This method blocks until offset data is received. It is preferable *not* to call this
     *         method from within a callback.
     */
    cppkafka::TopicPartitionList queryOffsetsAtTime(Timestamp timestamp) const final;
    cppkafka::TopicPartitionList queryOffsetsAtTime(Timestamp timestamp,
                                                    std::chrono::milliseconds) const final;
    
    /**
     * @brief Indicates if the rdkafka consumer/producer handle is valid and set
     * @return True if it's valid, False otherwise.
     */
    explicit operator bool() const final;
    
    /**
     * @brief Get the underlying rdkafka producer/consumer handle.
     * @return The handle. If the handle is invalid, 0 is returned.
     */
    uint64_t getHandle() const final;
    
    /**
     * @brief Get the topic name.
     * @return The name.
     */
    const std::string& getTopic() const final;
    
    /**
     * @brief Get the topic-specific metadata.
     * @return The metadata.
     */
    cppkafka::TopicMetadata getTopicMetadata() const final;
    cppkafka::TopicMetadata getTopicMetadata(std::chrono::milliseconds timeout) const final;
    
    /**
     * @brief Get the RdKafka internal name for this consumer or producer.
     * @return The name
     */
    std::string getInternalName() const final;
    
    /**
     * @brief Checks if this partition is available for this topic.
     * @param partition The partition id.
     * @return True if available, False otherwise.
     */
    bool isPartitionAvailable(int partition) const final;
    
protected:
    using ImplType = Impl<IMetadata>;
    // Constructor
    Metadata(const std::string& topic,
             const cppkafka::Topic& kafkaTopic,
             cppkafka::KafkaHandleBase* handle,
             std::chrono::milliseconds brokerTimeout = std::chrono::milliseconds{EnumValue(TimerValues::Disabled)});
    
    using VirtualImpl<IMetadata>::VirtualImpl;
    
    const cppkafka::Topic& getTopicObject() const final;
};

}
}

#endif //BLOOMBERG_COROKAFKA_METADATA_H
