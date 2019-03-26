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
#ifndef BLOOMBERG_COROKAFKA_MESSAGE_H
#define BLOOMBERG_COROKAFKA_MESSAGE_H

#include <memory>
#include <tuple>
#include <corokafka/corokafka_utils.h>
#include <corokafka/corokafka_offset_map.h>
#include <corokafka/corokafka_deserializer.h>
#include <boost/optional.hpp>

namespace Bloomberg {
namespace corokafka {

class IMessage
{
public:
    using HeaderType = Message::HeaderType;
    using HeaderListType = Message::HeaderListType;
    /**
     * @brief Destructor
     */
    virtual ~IMessage() = default;
    /**
     * @brief Returns the message handle.
     * @return The handle.
     * @remark This handle should be treated as opaque by the application. It serves to identify a unique
     *         message instance in the system.
     */
    virtual uint64_t getHandle() const = 0;
    /**
     * @brief Get the raw Kafka buffer containing key data.
     * @return A non-owning Buffer.
     * @warning This buffer is owned by the rdkafka library and is valid until this object is deleted.
     */
    virtual const Buffer& getKeyBuffer() const = 0;
    /**
     * @brief Get the raw Kafka header list.
     * @return A header list composed of non-owning Buffers.
     */
    virtual const HeaderListType& getHeaderList() const = 0;
    /**
     * @brief Get the raw Kafka buffer containing payload data.
     * @return A non-owning Buffer.
     * @warning This buffer is owned by the rdkafka library and is valid until this object is deleted.
     */
    virtual const Buffer& getPayloadBuffer() const = 0;
    /**
     * @brief Returns the message error if any.
     * @return The Error
     */
    virtual Error getError() const = 0;
    /**
     * @brief Get the topic name.
     * @return The name.
     */
    virtual std::string getTopic() const = 0;
    /**
     * @brief Get the partition associated with this message.
     * @return The partition id.
     */
    virtual int getPartition() const = 0;
    /**
     * @brief Get the offset of the message.
     * @return The offset.
     */
    virtual int64_t getOffset() const = 0;
    /**
     * @brief Gets the message timestamp.
     * @return The timestamp.
     * @note If the timestamp was created with a 'time_point', the duration represents the number of
     *       milliseconds since epoch.
     */
    virtual std::chrono::milliseconds getTimestamp() const = 0;
    /**
     * @brief Indicate if this message is valid or not.
     * @return True if valid, False otherwise.
     */
    virtual explicit operator bool() const = 0;
};

}
}

#endif //BLOOMBERG_COROKAFKA_MESSAGE_H
