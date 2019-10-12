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
#include <corokafka/corokafka_sent_message.h>

namespace Bloomberg {
namespace corokafka {

//====================================================================================
//                               SENT MESSAGE
//====================================================================================
SentMessage::SentMessage(const cppkafka::Message& kafkaMessage, void* opaque) :
    _message(&kafkaMessage),
    _builder(nullptr),
    _error(kafkaMessage.get_error()),
    _opaque(opaque)
{
}

SentMessage::SentMessage(const cppkafka::MessageBuilder& builder, cppkafka::Error error, void* opaque) :
    _message(nullptr),
    _builder(&builder),
    _error(error),
    _opaque(opaque)
{
}

uint64_t SentMessage::getHandle() const
{
    return _message ? (uint64_t)_message->get_handle() : 0;
}

const cppkafka::Buffer& SentMessage::getKeyBuffer() const
{
    return _message ? _message->get_key() : _builder->key();
}

const IMessage::HeaderListType& SentMessage::getHeaderList() const
{
    return _message ? _message->get_header_list() : _builder->header_list();
}

const cppkafka::Buffer& SentMessage::getPayloadBuffer() const
{
    return _message ? _message->get_payload() : _builder->payload();
}

cppkafka::Error SentMessage::getError() const
{
    return _error;
}

std::string SentMessage::getTopic() const
{
    return _message ? _message->get_topic() : _builder->topic();
}

int SentMessage::getPartition() const
{
    return _message ? _message->get_partition() : _builder->partition();
}

int64_t SentMessage::getOffset() const
{
    return _message ? _message->get_offset() : (int64_t)cppkafka::TopicPartition::OFFSET_INVALID;
}

std::chrono::milliseconds SentMessage::getTimestamp() const
{
    if (_message) {
        boost::optional<cppkafka::MessageTimestamp> timestamp = _message->get_timestamp();
        if (!timestamp) {
            throw std::runtime_error("Timestamp not set");
        }
        return timestamp.get().get_timestamp();
    }
    return _builder->timestamp();
}

SentMessage::operator bool() const
{
    return _message ? (bool)*_message : true;
}

void* SentMessage::getOpaque() const
{
    return _opaque;
}

#if (RD_KAFKA_VERSION >= RD_KAFKA_MESSAGE_STATUS_SUPPORT_VERSION)
rd_kafka_msg_status_t SentMessage::getStatus() const
{
    if (!_message) {
        throw std::runtime_error("Status not available");
    }
    return _message->get_status();
}
#endif

}
}
