#ifndef COROKAFKA_COROKAFKA_SENT_MESSAGE_IMPL_H
#define COROKAFKA_COROKAFKA_SENT_MESSAGE_IMPL_H

#include <corokafka/interface/corokafka_isent_message.h>

namespace Bloomberg {
namespace corokafka {

class SentMessageImpl : public ISentMessage
{
public:
    SentMessageImpl(const cppkafka::Message& kafkaMessage,
                    const void* opaque);
    SentMessageImpl(const cppkafka::MessageBuilder& builder,
                    cppkafka::Error error,
                    const void* opaque);
    
    const cppkafka::Buffer& getKeyBuffer() const final;
    const cppkafka::Message::HeaderListType& getHeaderList() const final;
    const cppkafka::Buffer& getPayloadBuffer() const final;
    uint64_t getHandle() const final;
    cppkafka::Error getError() const final;
    std::string getTopic() const final;
    int getPartition() const final;
    int64_t getOffset() const final;
    std::chrono::milliseconds getTimestamp() const final;
    explicit operator bool() const final;
    void* getOpaque() const final;
    
#if (RD_KAFKA_VERSION >= RD_KAFKA_MESSAGE_STATUS_SUPPORT_VERSION)
    rd_kafka_msg_status_t getStatus() const final;
#endif

#if RD_KAFKA_VERSION >= RD_KAFKA_MESSAGE_LATENCY_SUPPORT_VERSION
    std::chrono::microseconds getLatency() const final;
#endif

private:
    const cppkafka::Message*          _message;
    const cppkafka::MessageBuilder*   _builder;
    cppkafka::Error                   _error;
    const void*                       _opaque;
};

}}

#endif //COROKAFKA_COROKAFKA_SENT_MESSAGE_IMPL_H
