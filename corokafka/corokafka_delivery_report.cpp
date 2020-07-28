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
#include <corokafka/corokafka_delivery_report.h>
#include <corokafka/utils/corokafka_json_builder.h>
#include <sstream>

namespace Bloomberg {
namespace corokafka {

//====================================================================================
//                               DELIVERY REPORT
//====================================================================================
const cppkafka::TopicPartition& DeliveryReport::getTopicPartition() const
{
    return _topicPartition;
}

DeliveryReport& DeliveryReport::topicPartition(cppkafka::TopicPartition topicPartition)
{
    _topicPartition = std::move(topicPartition);
    return *this;
}

size_t DeliveryReport::getNumBytesWritten() const
{
    return _numBytes;
}

DeliveryReport& DeliveryReport::numBytesWritten(size_t bytes)
{
    _numBytes = bytes;
    return *this;
}

const cppkafka::Error& DeliveryReport::getError() const
{
    return _error;
}

DeliveryReport& DeliveryReport::error(cppkafka::Error error)
{
    _error = std::move(error);
    return *this;
}

void* DeliveryReport::getOpaque() const
{
    return const_cast<void*>(_opaque);
}

DeliveryReport& DeliveryReport::opaque(const void* opaque)
{
    _opaque = opaque;
    return *this;
}

#if (RD_KAFKA_VERSION >= RD_KAFKA_MESSAGE_STATUS_SUPPORT_VERSION)
rd_kafka_msg_status_t DeliveryReport::getStatus() const
{
    return _status;
}

DeliveryReport& DeliveryReport::status(rd_kafka_msg_status_t status)
{
    _status = status;
    return *this;
}
#endif

#if RD_KAFKA_VERSION >= RD_KAFKA_MESSAGE_LATENCY_SUPPORT_VERSION
std::chrono::microseconds DeliveryReport::getLatency() const
{
    return _latency;
}

DeliveryReport& DeliveryReport::latency(std::chrono::microseconds latency)
{
    _latency = std::move(latency);
    return *this;
}
#endif

std::ostream& operator<<(std::ostream& output, const DeliveryReport& dr)
{
    output << dr.toString();
    return output;
}

std::string DeliveryReport::toString() const
{
    std::ostringstream oss;
    {
        JsonBuilder json(oss);
        json.startMember("deliveryReport").tag("destination", _topicPartition);
        if (_error) {
            json.tag("error", _error);
        } else {
            json.tag("numBytes", _numBytes);
        }
#if (RD_KAFKA_VERSION >= RD_KAFKA_MESSAGE_STATUS_SUPPORT_VERSION)
        json.tag("status", kafkaStatusToString(_status));
#endif
#if RD_KAFKA_VERSION >= RD_KAFKA_MESSAGE_LATENCY_SUPPORT_VERSION
        json.tag("latencyUs", _latency.count());
#endif
        if (_opaque) {
            json.tag("opaque", _opaque);
        }
        json.endMember();
    }
    return oss.str();
}

const char* DeliveryReport::kafkaStatusToString(rd_kafka_msg_status_t status)
{
    switch (status) {
        case RD_KAFKA_MSG_STATUS_NOT_PERSISTED:
            return "NotPersisted";
        case RD_KAFKA_MSG_STATUS_POSSIBLY_PERSISTED:
            return "PossiblyPersisted";
        case RD_KAFKA_MSG_STATUS_PERSISTED:
            return "Persisted";
        default:
            return "Unknown";
    }
}
  
}
}
