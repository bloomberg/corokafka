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
#ifndef BLOOMBERG_COROKAFKA_DELIVERY_REPORT_H
#define BLOOMBERG_COROKAFKA_DELIVERY_REPORT_H

#include <corokafka/corokafka_utils.h>

namespace Bloomberg {
namespace corokafka {

/**
 * @brief Represents the result of a successful or failed delivery
 *        of a message
 */
class DeliveryReport
{
public:
    DeliveryReport() = default;
    /**
     * @brief Gets the message topic partition
     */
    const cppkafka::TopicPartition& getTopicPartition() const;
    /**
     * @brief Gets the number of bytes written if successful
     */
    size_t getNumBytesWritten() const;
    /**
     * @brief Gets the message delivery error
     */    
    const cppkafka::Error& getError() const;
    /**
     * @brief Gets the opaque data associated with the message
     */
    void* getOpaque() const;
    
#if (RD_KAFKA_VERSION >= RD_KAFKA_MESSAGE_STATUS_SUPPORT_VERSION)
    /**
     * @brief Gets the message persistence status.
     * @note Only available if SentMessage was build with a Message type.
     */
    rd_kafka_msg_status_t getStatus() const;
#endif
    
#if RD_KAFKA_VERSION >= RD_KAFKA_MESSAGE_LATENCY_SUPPORT_VERSION
    /**
     * @brief Gets the message latency in microseconds as measured from the produce() call.
     * @return The latency in microseconds
     */
    std::chrono::microseconds getLatency() const;
#endif
    
    /**
     * @brief Get a string representation of this object
     */
    std::string toString() const;

private:
    friend class ProducerManagerImpl;
    
    //Setters
    DeliveryReport& topicPartition(cppkafka::TopicPartition topicPartition);
    DeliveryReport& numBytesWritten(size_t numBytes);
    DeliveryReport& error(cppkafka::Error error);
#if (RD_KAFKA_VERSION >= RD_KAFKA_MESSAGE_STATUS_SUPPORT_VERSION)
    DeliveryReport& status(rd_kafka_msg_status_t status);
#endif
#if RD_KAFKA_VERSION >= RD_KAFKA_MESSAGE_LATENCY_SUPPORT_VERSION
    DeliveryReport& latency(std::chrono::microseconds latency);
#endif
    DeliveryReport& opaque(const void* opaque);
    
    static const char* kafkaStatusToString(rd_kafka_msg_status_t status);
    
    //Members
    cppkafka::TopicPartition    _topicPartition;
    size_t                      _numBytes{0};
    cppkafka::Error             _error{RD_KAFKA_RESP_ERR__FAIL};
#if (RD_KAFKA_VERSION >= RD_KAFKA_MESSAGE_STATUS_SUPPORT_VERSION)
    rd_kafka_msg_status_t       _status{RD_KAFKA_MSG_STATUS_NOT_PERSISTED};
#endif
#if RD_KAFKA_VERSION >= RD_KAFKA_MESSAGE_LATENCY_SUPPORT_VERSION
    std::chrono::microseconds   _latency;
#endif
    const void*                 _opaque{nullptr};
};

/**
 * @brief Print to stream
 */
std::ostream& operator<<(std::ostream& output, const DeliveryReport& dr);
 
}}

#endif // BLOOMBERG_COROKAFKA_DELIVERY_REPORT_H
