#include <corokafka/impl/corokafka_sent_message_impl.h>
#include <corokafka/corokafka_exception.h>

namespace Bloomberg {
namespace corokafka {

SentMessageImpl::SentMessageImpl(const cppkafka::Message& kafkaMessage,
                                 const void* opaque) :
    _message(&kafkaMessage),
    _builder(nullptr),
    _error(kafkaMessage.get_error()),
    _opaque(opaque)
{
}

SentMessageImpl::SentMessageImpl(const cppkafka::MessageBuilder& builder,
                                 cppkafka::Error error,
                                 const void* opaque) :
    _message(nullptr),
    _builder(&builder),
    _error(error),
    _opaque(opaque)
{
}

uint64_t SentMessageImpl::getHandle() const
{
    return _message ? reinterpret_cast<uint64_t>(_message->get_handle()) : 0;
}

const cppkafka::Buffer& SentMessageImpl::getKeyBuffer() const
{
    return _message ? _message->get_key() : _builder->key();
}

const cppkafka::Message::HeaderListType& SentMessageImpl::getHeaderList() const
{
    return _message ? _message->get_header_list() : _builder->header_list();
}

const cppkafka::Buffer& SentMessageImpl::getPayloadBuffer() const
{
    return _message ? _message->get_payload() : _builder->payload();
}

cppkafka::Error SentMessageImpl::getError() const
{
    return _error;
}

std::string SentMessageImpl::getTopic() const
{
    return _message ? _message->get_topic() : _builder->topic();
}

int SentMessageImpl::getPartition() const
{
    return _message ? _message->get_partition() : _builder->partition();
}

int64_t SentMessageImpl::getOffset() const
{
    return _message ? _message->get_offset() : static_cast<int64_t>(cppkafka::TopicPartition::OFFSET_INVALID);
}

std::chrono::milliseconds SentMessageImpl::getTimestamp() const
{
    if (_message) {
        boost::optional<cppkafka::MessageTimestamp> timestamp = _message->get_timestamp();
        if (!timestamp) {
            throw MessageException("Timestamp not set");
        }
        return timestamp.get().get_timestamp();
    }
    return _builder->timestamp();
}

SentMessageImpl::operator bool() const
{
    return _message ? (bool)*_message : true;
}

void* SentMessageImpl::getOpaque() const
{
    return const_cast<void*>(_opaque);
}

#if (RD_KAFKA_VERSION >= RD_KAFKA_MESSAGE_STATUS_SUPPORT_VERSION)
rd_kafka_msg_status_t SentMessageImpl::getStatus() const
{
    if (!_message) {
        throw MessageException("Status not available");
    }
    return _message->get_status();
}
#endif

#if RD_KAFKA_VERSION >= RD_KAFKA_MESSAGE_LATENCY_SUPPORT_VERSION
std::chrono::microseconds SentMessageImpl::getLatency() const
{
    if (!_message) {
        throw MessageException("Latency not available");
    }
    return _message->get_latency();
}
#endif

}}

