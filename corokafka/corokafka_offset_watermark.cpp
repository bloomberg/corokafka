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
#include <corokafka/corokafka_offset_watermark.h>
#include <corokafka/utils/corokafka_json_builder.h>
#include <cppkafka/topic_partition.h>
#include <string>
#include <algorithm>
#include <sstream>

namespace Bloomberg {
namespace corokafka {

std::string OffsetWatermark::toString() const
{
    std::ostringstream oss;
    {
        JsonBuilder json(oss);
        json.startMember("watermark").
            tag("partition", _partition).
            startMember("offsets").
            tag("low", (_watermark._low == cppkafka::TopicPartition::OFFSET_INVALID ?
                        "#" : std::to_string(_watermark._low))).
            tag("high", (_watermark._high == cppkafka::TopicPartition::OFFSET_INVALID ?
                         "#" : std::to_string(_watermark._high))).
            endMember(). //offsets
            endMember(); //watermark
    }
    return oss.str();
}

std::ostream& operator<<(std::ostream& output, const OffsetWatermark& watermark)
{
    return output << watermark.toString();
}

std::ostream& operator<<(std::ostream& output, const OffsetWatermarkList& watermarks)
{
    {
        JsonBuilder json(output);
        json.startMember("watermarks", JsonBuilder::Array::True);
        for (const auto &watermark : watermarks) {
            json.rawTag(watermark);
        }
        json.endMember();
    }
    return output;
}

}}


