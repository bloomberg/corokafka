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
#ifndef BLOOMBERG_COROKAFKA_IMETADATA_H
#define BLOOMBERG_COROKAFKA_IMETADATA_H

#include <corokafka/corokafka_utils.h>
#include <corokafka/corokafka_offset_watermark.h>
#include <cppkafka/metadata.h>
#include <vector>
#include <chrono>
#include <string>

namespace Bloomberg {
namespace corokafka {

struct IMetadata
{
    using Timestamp = std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds>;
    
    virtual ~IMetadata() = default;
    virtual KafkaType getType() const = 0;
    virtual OffsetWatermarkList queryOffsetWatermarks() const = 0;
    virtual OffsetWatermarkList queryOffsetWatermarks(std::chrono::milliseconds timeout) const = 0;
    virtual cppkafka::TopicPartitionList queryOffsetsAtTime(Timestamp timestamp) const = 0;
    virtual cppkafka::TopicPartitionList queryOffsetsAtTime(Timestamp timestamp,
                                                            std::chrono::milliseconds timeout) const = 0;
    virtual explicit operator bool() const = 0;
    virtual uint64_t getHandle() const = 0;
    virtual const std::string& getTopic() const = 0;
    virtual const cppkafka::Topic& getTopicObject() const = 0;
    virtual cppkafka::TopicMetadata getTopicMetadata() const = 0;
    virtual cppkafka::TopicMetadata getTopicMetadata(std::chrono::milliseconds timeout) const = 0;
    virtual std::string getInternalName() const = 0;
    virtual bool isPartitionAvailable(int partition) const = 0;
};

}}

#endif //BLOOMBERG_COROKAFKA_IMETADATA_H
