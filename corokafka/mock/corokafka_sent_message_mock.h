#ifndef BLOOMBERG_COROKAFKA_SENT_MESSAGE_MOCK_H
#define BLOOMBERG_COROKAFKA_SENT_MESSAGE_MOCK_H

#include <corokafka/interface/corokafka_isent_message.h>
#include <corokafka/mock/corokafka_message_mock.h>

namespace Bloomberg {
namespace corokafka {
namespace mocks {

struct SentMessageMock : public ISentMessage,
                         public MessageMock
{
    MOCK_CONST_METHOD0(getOpaque, void*());
    
#if (RD_KAFKA_VERSION >= RD_KAFKA_MESSAGE_STATUS_SUPPORT_VERSION)
    MOCK_CONST_METHOD0(getStatus, rd_kafka_msg_status_t());
#endif

#if RD_KAFKA_VERSION >= RD_KAFKA_MESSAGE_LATENCY_SUPPORT_VERSION
    MOCK_CONST_METHOD0(getLatency, std::chrono::microseconds());
#endif
};

}}}

#endif //BLOOMBERG_COROKAFKA_SENT_MESSAGE_MOCK_H
