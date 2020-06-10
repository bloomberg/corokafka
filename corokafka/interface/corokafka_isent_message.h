#ifndef BLOOMBERG_COROKAFKA_ISENT_MESSAGE_H
#define BLOOMBERG_COROKAFKA_ISENT_MESSAGE_H

#include <corokafka/interface/corokafka_imessage.h>
#include <corokafka/corokafka_utils.h>
#include <chrono>

namespace Bloomberg {
namespace corokafka {

struct ISentMessage : public virtual IMessage
{
    /**
     * @brief Return the opaque application-specific pointer set when send() or sendAsync() were called.
     * @return The opaque pointer.
     */
    virtual void* getOpaque() const = 0;
    
#if (RD_KAFKA_VERSION >= RD_KAFKA_MESSAGE_STATUS_SUPPORT_VERSION)
    /**
     * @brief Gets the message persistence status.
     * @note Only available if SentMessage was build with a Message type.
     */
    virtual rd_kafka_msg_status_t getStatus() const = 0;
#endif

#if RD_KAFKA_VERSION >= RD_KAFKA_MESSAGE_LATENCY_SUPPORT_VERSION
    /**
     * @brief Gets the message latency in microseconds as measured from the produce() call.
     * @return The latency in microseconds
     */
    virtual std::chrono::microseconds getLatency() const = 0;
#endif
};

}}

#endif //BLOOMBERG_COROKAFKA_ISENT_MESSAGE_H
