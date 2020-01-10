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

DeliveryReport::DeliveryReport(cppkafka::TopicPartition topicPartition,
                               size_t numBytes,
                               cppkafka::Error error,
                               const void * opaque) :
    _topicPartition(std::move(topicPartition)),
    _numBytes(numBytes),
    _error(std::move(error)),
    _opaque(opaque)
{
}

const cppkafka::TopicPartition& DeliveryReport::getTopicPartition() const
{
    return _topicPartition;
}

size_t DeliveryReport::getNumBytesWritten() const
{
    return _numBytes;
}

const cppkafka::Error& DeliveryReport::getError() const
{
    return _error;
}

void* DeliveryReport::getOpaque() const
{
    return const_cast<void*>(_opaque);
}

std::string DeliveryReport::toString() const
{
    std::ostringstream oss;
    JsonBuilder json(oss);
    json.startMember("deliveryReport").tag("destination", _topicPartition);
    if (_error) {
        json.tag("error", _error);
    }
    else {
        json.tag("numBytes", _numBytes);
    }
    if (_opaque) {
        json.tag("opaque", _opaque);
    }
    json.endMember().end();
    return oss.str();
}

std::ostream& operator<<(std::ostream& output, const DeliveryReport& dr)
{
    output << dr.toString();
    return output;
}
  
}
}
