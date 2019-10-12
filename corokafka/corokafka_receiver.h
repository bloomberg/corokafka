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
#ifndef BLOOMBERG_COROKAFKA_RECEIVER_H
#define BLOOMBERG_COROKAFKA_RECEIVER_H

#include <memory>
#include <corokafka/corokafka_received_message.h>
#include <corokafka/corokafka_header_pack.h>
#include <corokafka/corokafka_utils.h>
#include <boost/any.hpp>

namespace Bloomberg {
namespace corokafka {

struct Receiver
{
    using ResultType = void;
    using result_type = ResultType; //cppkafka compatibility for callback invoker
    virtual ~Receiver() = default;
    virtual ResultType operator()(cppkafka::BackoffCommitter& committer,
                                  OffsetMap& offsets,
                                  cppkafka::Message&& kafkaMessage,
                                  boost::any&& key,
                                  boost::any&& payload,
                                  HeaderPack&& headers,
                                  DeserializerError&& error,
                                  const OffsetPersistSettings& offsetSettings) const = 0;
    virtual explicit operator bool() const = 0;
};

template <typename TOPIC>
class ConcreteReceiver : public Receiver
{
public:
    using ResultType = Receiver::ResultType;
    using Key = typename TOPIC::KeyType;
    using Payload = typename TOPIC::PayloadType;
    using HeadersType = typename TOPIC::HeadersType;
    using ReceivedMessageType = ReceivedMessage<Key,Payload,HeadersType>;
    using Callback = std::function<ResultType(ReceivedMessageType)>;
    
    //Ctor
    ConcreteReceiver(Callback callback) :
        _func(std::move(callback))
    {}
    
    const Callback& getCallback() const { return _func; }
    
    void operator()(cppkafka::BackoffCommitter& committer,
                    OffsetMap& offsets,
                    cppkafka::Message&& kafkaMessage,
                    boost::any&& key,
                    boost::any&& payload,
                    HeaderPack&& headers,
                    DeserializerError&& error,
                    const OffsetPersistSettings& offsetSettings) const final {
        _func(ReceivedMessageType(committer,
                               offsets,
                               std::move(kafkaMessage),
                               std::move(key),
                               std::move(payload),
                               std::move(headers),
                               std::move(error),
                               offsetSettings));
    }
    
    explicit operator bool() const final { return (bool)_func; }
    
private:
    Callback _func;
};

}
}

#endif //BLOOMBERG_COROKAFKA_RECEIVER_H
