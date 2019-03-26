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
#ifndef BLOOMBERG_COROKAFKA_CONSUMER_METADATA_H
#define BLOOMBERG_COROKAFKA_CONSUMER_METADATA_H

#include <corokafka/corokafka_metadata.h>

namespace Bloomberg {
namespace corokafka {

//=============================================================================
//                             CONSUMER METADATA
//=============================================================================

class ConsumerMetadata : public Metadata
{
    friend class ConsumerManagerImpl;
public:
    /**
     * @sa Metadata::getType
     */
    KafkaType getType() const final { return KafkaType::Consumer; }
    /**
     * @sa Metadata::queryOffsetWatermarks
     */
    OffsetWatermarkList queryOffsetWatermarks() const final;
    /**
     * @brief Similar to queryOffsetWatermarks but only gets the locally known assigned partitions offsets.
     * @return The offset list.
     * @remark Since this method is local, it *could* be used in a callback.
     */
    OffsetWatermarkList getOffsetWatermarks() const;
    /**
     * @sa Metadata::queryOffsetsAtTime
     */
    TopicPartitionList queryOffsetsAtTime(Timestamp timestamp) const final;
    /**
     * @brief Query the remote broker for the committed offsets.
     * @return The committed offsets list.
     * @remark This method blocks until offset data is received. It is preferable *not* to call this
     *         method from within a callback.
     */
    TopicPartitionList queryCommittedOffsets() const;
    /**
     * @brief Gets the last offset positions locally known.
     * @return The offset list.
     * @remark The offset positions represent the offsets of the last message(s) delivered to the application
     *         but not yet committed.
     */
    TopicPartitionList getOffsetPositions() const;
    /**
     * @brief Get the current partition assignment for this consumer.
     * @return The partition list.
     */
    const TopicPartitionList& getPartitionAssignment() const;
    /**
     * @brief Get information about the group this consumer belongs to (if any).
     * @return The group info.
     */
    GroupInformation getGroupInformation() const;
    
private:
    ConsumerMetadata(const std::string& topic,
                     Consumer& handle);
    ConsumerMetadata(const std::string& topic,
                     const Topic& kafkaTopic,
                     Consumer& handle);
};

}
}


#endif //BLOOMBERG_COROKAFKA_CONSUMER_METADATA_H
