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
#ifndef COROKAFKA_COROKAFKA_OFFSET_WATERMARK_H
#define COROKAFKA_COROKAFKA_OFFSET_WATERMARK_H

#include <cppkafka/kafka_handle_base.h>
#include <cppkafka/topic_partition.h>
#include <librdkafka/rdkafka.h>
#include <vector>

namespace Bloomberg {
namespace corokafka {

struct OffsetWatermark
{
    OffsetWatermark() = default;
    OffsetWatermark(int partition,
                    cppkafka::KafkaHandleBase::OffsetTuple watermark) :
            _partition(partition),
            _watermark{std::get<0>(watermark), std::get<1>(watermark)} {}
    
    int _partition{RD_KAFKA_PARTITION_UA};
    struct {
        int64_t _low{cppkafka::TopicPartition::OFFSET_INVALID};
        int64_t _high{cppkafka::TopicPartition::OFFSET_INVALID};
    } _watermark;
};

using OffsetWatermarkList = std::vector<OffsetWatermark>;

}}

#endif //COROKAFKA_COROKAFKA_OFFSET_WATERMARK_H
