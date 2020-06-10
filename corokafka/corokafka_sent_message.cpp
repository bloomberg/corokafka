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
#include <corokafka/corokafka_exception.h>
#include <corokafka/impl/corokafka_sent_message_impl.h>

namespace Bloomberg {
namespace corokafka {

//====================================================================================
//                               SENT MESSAGE
//====================================================================================
SentMessage::SentMessage(const cppkafka::Message& kafkaMessage,
                         const void* opaque) :
    ImplType(std::make_shared<SentMessageImpl>(kafkaMessage, opaque))
{
}

SentMessage::SentMessage(const cppkafka::MessageBuilder& builder,
                         cppkafka::Error error,
                         const void* opaque) :
    ImplType(std::make_shared<SentMessageImpl>(builder, error, opaque))
{
}

uint64_t SentMessage::getHandle() const
{
    return impl()->getHandle();
}

const cppkafka::Buffer& SentMessage::getKeyBuffer() const
{
    return impl()->getKeyBuffer();
}

const cppkafka::Message::HeaderListType& SentMessage::getHeaderList() const
{
    return impl()->getHeaderList();
}

const cppkafka::Buffer& SentMessage::getPayloadBuffer() const
{
    return impl()->getPayloadBuffer();
}

cppkafka::Error SentMessage::getError() const
{
    return impl()->getError();
}

std::string SentMessage::getTopic() const
{
    return impl()->getTopic();
}

int SentMessage::getPartition() const
{
    return impl()->getPartition();
}

int64_t SentMessage::getOffset() const
{
    return impl()->getOffset();
}

std::chrono::milliseconds SentMessage::getTimestamp() const
{
    return impl()->getTimestamp();
}

SentMessage::operator bool() const
{
    return impl()->operator bool();
}

void* SentMessage::getOpaque() const
{
    return impl()->getOpaque();
}

#if (RD_KAFKA_VERSION >= RD_KAFKA_MESSAGE_STATUS_SUPPORT_VERSION)
rd_kafka_msg_status_t SentMessage::getStatus() const
{
    return impl()->getStatus();
}
#endif

#if RD_KAFKA_VERSION >= RD_KAFKA_MESSAGE_LATENCY_SUPPORT_VERSION
std::chrono::microseconds SentMessage::getLatency() const
{
    return impl()->getLatency();
}
#endif

}
}
