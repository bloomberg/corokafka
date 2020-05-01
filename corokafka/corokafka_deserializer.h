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
#ifndef BLOOMBERG_COROKAFKA_DESERIALIZER_H
#define BLOOMBERG_COROKAFKA_DESERIALIZER_H

#include <corokafka/interface/corokafka_imessage.h>
#include <corokafka/corokafka_utils.h>
#include <corokafka/corokafka_header_pack.h>
#include <boost/any.hpp>
#include <bitset>

namespace Bloomberg {
namespace corokafka {

struct DeserializerError
{
    enum class Source : uint8_t {
        Kafka = 0,
        Key,
        Payload,
        Header,
        Preprocessor,
        Max
    };
    bool hasError() const {
        return _source.any();
    }
    bool hasError(Source source) const {
        return _source.test(EnumValue(source));
    }
    void setError(Source source) {
        _source.set(EnumValue(source));
    }
    //members
    cppkafka::Error                     _error{RD_KAFKA_RESP_ERR_NO_ERROR};
    std::bitset<EnumValue(Source::Max)> _source;
    int                                 _headerNum{-1};
};

struct Deserializer
{
    using ResultType = boost::any;
    using result_type = ResultType; //cppkafka compatibility for callback invoker
    virtual ~Deserializer() = default;
    virtual ResultType operator()(const cppkafka::TopicPartition&, const cppkafka::Buffer&) const { return {}; };
    explicit operator bool() const { return true; };
};

template <typename T>
class ConcreteDeserializer : public Deserializer
{
public:
    using ResultType = Deserializer::ResultType;
    using Callback = std::function<T(const cppkafka::TopicPartition&, const cppkafka::Buffer& buffer)>;
    
    ResultType operator()(const cppkafka::TopicPartition& toppar,
                          const cppkafka::Buffer& buffer) const final {
        return Deserialize<T>{}(toppar, buffer);
    }
};

}
}

#endif //BLOOMBERG_COROKAFKA_DESERIALIZER_H
