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
#ifndef BLOOMBERG_COROKAFKA_SERIALIZER_H
#define BLOOMBERG_COROKAFKA_SERIALIZER_H

#include <corokafka/corokafka_message.h>
#include <corokafka/corokafka_utils.h>
#include <corokafka/corokafka_header_pack.h>

namespace Bloomberg {
namespace corokafka {

struct Serializer
{
    using ResultType = ByteArray;
    using result_type = ResultType; //cppkafka compatibility for callback invoker
    virtual ~Serializer() = default;
    virtual ResultType operator()(const void*) const { return {}; };
    virtual ResultType operator()(const HeaderPack*, const void*) const { return {}; };
    virtual explicit operator bool() const = 0;
};

template <typename T>
class ConcreteSerializer : public Serializer
{
public:
    using ResultType = Serializer::ResultType;
    using Callback = std::function<ResultType(const T&)>;
    
    //Ctor
    ConcreteSerializer(Callback callback) :
        _func(std::move(callback))
    {}
    
    const Callback& getCallback() const { return _func; }
    
    ResultType operator()(const void* unpacked) const final {
        if (unpacked == nullptr) {
            return {};
        }
        return _func(*static_cast<const T*>(unpacked));
    }
    
    explicit operator bool() const final { return (bool)_func; }
private:
    Callback _func;
};

template <typename T>
class ConcreteSerializerWithHeaders : public Serializer
{
public:
    using ResultType = Serializer::ResultType;
    using Callback = std::function<ResultType(const HeaderPack&, const T&)>;
    
    //Ctor
    ConcreteSerializerWithHeaders(Callback callback) :
        _func(std::move(callback))
    {}
    
    const Callback& getCallback() const { return _func; }
    
    ResultType operator()(const HeaderPack* headers, const void* unpacked) const final {
        if ((headers == nullptr) || (unpacked == nullptr)) {
            return {};
        }
        return _func(*headers, *static_cast<const T*>(unpacked));
    }
    
    explicit operator bool() const final { return (bool)_func; }
private:
    Callback _func;
};

}
}

#endif //BLOOMBERG_COROKAFKA_SERIALIZER_H
