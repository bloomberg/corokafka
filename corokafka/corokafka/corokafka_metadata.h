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

#include <string>
#include <corokafka/corokafka_utils.h>

namespace Bloomberg {
namespace corokafka {

struct OffsetWatermark {
    OffsetWatermark(int partition,
                    KafkaHandleBase::OffsetTuple watermark) :
        _partition(partition),
       _watermark{std::get<0>(watermark), std::get<1>(watermark)}
    {}
    int _partition;
    struct {
        int64_t _low;
        int64_t _high;
    } _watermark;
};

//=============================================================================
//                                 METADATA
//=============================================================================
class Metadata {
public:
    using OffsetWatermarkList = std::vector<OffsetWatermark>;
    using Timestamp = std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds>;
    
    /**
     * @brief Constructors and assignment operators
     */
    Metadata(const Metadata&) = delete;
    Metadata(Metadata&&) = default;
    Metadata& operator=(const Metadata&) = delete;
    Metadata& operator=(Metadata&&) = default;
    virtual ~Metadata() = default;
    
    /**
     * @brief Get the metadata type.
     * @return The type.
     */
    virtual KafkaType getType() const = 0;
    
    /**
     * @brief Query the remote broker for all offset watermarks.
     * @return A list of offset watermarks belonging to this consumer or producer.
     * @remark This method blocks until offset data is received. It is preferable *not* to call this
     *         method from within a callback.
     */
    virtual OffsetWatermarkList queryOffsetWatermarks() const = 0;
    
    /**
     * @brief Query the remote broker for all offsets newer than timestamp.
     * @param timestamp The timestamp.
     * @return A partition list containing all offsets.
     * @remark This method blocks until offset data is received. It is preferable *not* to call this
     *         method from within a callback.
     */
    virtual TopicPartitionList queryOffsetsAtTime(Timestamp timestamp) const = 0;
    
    /**
     * @brief Get the topic name.
     * @return The name.
     */
    const std::string& getTopic() const;
    
    /**
     * @brief Get the topic-specific metadata.
     * @return The metadata.
     */
    TopicMetadata getTopicMetadata() const;
    
    /**
     * @brief Get the librdkafka internal name for this consumer or producer.
     * @return The name
     */
    std::string getInternalName() const;
    
    /**
     * @brief Checks if this partition is available for this topic.
     * @param partition The partition id.
     * @return True if available, False otherwise.
     */
    bool isPartitionAvailable(int partition) const;
    
protected:
    // Constructor
    Metadata(const std::string& topic,
             const Topic& kafkaTopic,
             KafkaHandleBase& handle);
    
    const Topic& getTopicObject() const;
    
    const std::string&            _topic;
    KafkaHandleBase&              _handle;
    mutable Topic                 _kafkaTopic;
    mutable TopicPartitionList    _partitions;
};

}
}

#endif //BLOOMBERG_COROKAFKA_METADATA_H
