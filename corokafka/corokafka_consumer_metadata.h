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
#include <corokafka/interface/corokafka_iconsumer_metadata.h>
#include <memory>

namespace Bloomberg {
namespace corokafka {

namespace mocks {
    struct ConsumerManagerMock;
}

//=============================================================================
//                             CONSUMER METADATA
//=============================================================================

class ConsumerMetadata : public Impl<IConsumerMetadata>,
                         public Metadata
{
public:
    /**
     * @brief Similar to queryOffsetWatermarks but only gets the locally known assigned partitions offsets.
     * @return The offset list.
     * @remark Since this method is local, it *could* be used in a callback.
     */
    OffsetWatermarkList getOffsetWatermarks() const final;
    /**
     * @brief Query the remote broker for the committed offsets.
     * @return The committed offsets list.
     * @remark This method blocks until offset data is received. It is preferable *not* to call this
     *         method from within a callback.
     */
    cppkafka::TopicPartitionList queryCommittedOffsets() const final;
    cppkafka::TopicPartitionList queryCommittedOffsets(std::chrono::milliseconds timeout) const final;
    /**
     * @brief Gets the last offset positions locally known.
     * @return The offset list.
     * @remark The offset positions represent the offsets of the last message(s) delivered to the application
     *         but not yet committed.
     */
    cppkafka::TopicPartitionList getOffsetPositions() const final;
    /**
     * @brief Get the current partition assignment for this consumer.
     * @return The partition list.
     */
    const cppkafka::TopicPartitionList& getPartitionAssignment() const final;
    /**
     * @brief Get information about the group this consumer belongs to (if any).
     * @return The group info.
     */
    cppkafka::GroupInformation getGroupInformation() const final;
    cppkafka::GroupInformation getGroupInformation(std::chrono::milliseconds timeout) const final;
    
    /**
     * @brief Get information about all consumer groups
     * @return A list of group information objects
     */
    cppkafka::GroupInformationList getAllGroupsInformation() const final;
    cppkafka::GroupInformationList getAllGroupsInformation(std::chrono::milliseconds timeout) const final;
    
    /**
     * @brief Get the current partition strategy.
     * @return The strategy.
     */
    PartitionStrategy getPartitionStrategy() const final;
    
private:
    using ImplType = Impl<IConsumerMetadata>;
    friend class ConsumerManagerImpl;
    friend struct mocks::ConsumerManagerMock;
    
    ConsumerMetadata(const std::string& topic,
                     cppkafka::Consumer* handle,
                     PartitionStrategy strategy,
                     std::chrono::milliseconds brokerTimeout = std::chrono::milliseconds{EnumValue(TimerValues::Disabled)});
    
    ConsumerMetadata(const std::string& topic,
                     const cppkafka::Topic& kafkaTopic,
                     cppkafka::Consumer* handle,
                     PartitionStrategy strategy,
                     std::chrono::milliseconds brokerTimeout = std::chrono::milliseconds{EnumValue(TimerValues::Disabled)});
    
    //For mocking only
    explicit ConsumerMetadata(std::shared_ptr<IConsumerMetadata> impl);
};

}
}


#endif //BLOOMBERG_COROKAFKA_CONSUMER_METADATA_H
