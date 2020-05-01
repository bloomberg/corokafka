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
#ifndef BLOOMBERG_COROKAFKA_TOPIC_MOCK_H
#define BLOOMBERG_COROKAFKA_TOPIC_MOCK_H

#include <corokafka/corokafka_topic.h>

namespace Bloomberg {
namespace corokafka {
namespace mocks {

struct MockEmpty{};
using MockTopic = corokafka::Topic<MockEmpty, MockEmpty>; //key, payload

} //namespace mocks

//serializers and deserializers
template <>
struct Serialize<mocks::MockEmpty>
{
    std::vector<uint8_t> operator()(const mocks::MockEmpty& m) { return {reinterpret_cast<const uint8_t*>(&m),
                                                                         reinterpret_cast<const uint8_t*>(&m)+sizeof(m)}; };
};

template <>
struct Deserialize<mocks::MockEmpty>
{
    mocks::MockEmpty operator()(const cppkafka::TopicPartition&, const cppkafka::Buffer&) { return {}; }
};

}
}

#endif //BLOOMBERG_COROKAFKA_TOPIC_MOCK_H
