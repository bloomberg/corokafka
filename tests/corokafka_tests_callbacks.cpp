#include <corokafka_tests_callbacks.h>
#include <corokafka_tests_utils.h>

namespace Bloomberg {
namespace corokafka {
namespace tests {

//callback implementations
void Callbacks::handleKafkaError(const Metadata& metadata,
                                 cppkafka::Error error,
                                 const std::string &reason,
                                 void *opaque)
{
    callbackCounters()._error++;
    callbackCounters()._opaque = opaque;
}

void Callbacks::handleThrottling(const Metadata& metadata,
                                 const std::string &brokerName,
                                 int32_t brokerId,
                                 std::chrono::milliseconds throttleTime)
{
    callbackCounters()._throttle++;
}

void Callbacks::kafkaLogger(const Metadata& metadata,
                            cppkafka::LogLevel level,
                            const std::string &facility,
                            const std::string &message)
{
    callbackCounters()._logger++;
}

void Callbacks::connectorLogger(cppkafka::LogLevel level,
                                const std::string &facility,
                                const std::string &message)
{
    callbackCounters()._connectorLogger++;
}

void Callbacks::handleStats(const Metadata &metadata,
                            const std::string &json)
{
    callbackCounters()._stats++;
}

bool Callbacks::messagePreprocessor(cppkafka::TopicPartition toppar)
{
    callbackCounters()._preprocessor++;
    callbackCounters()._preprocessorIoThread = quantum::local::context() == nullptr;
    return callbackCounters()._forceSkip;
}

void Callbacks::handleDeliveryReport(const ProducerMetadata &metadata,
                                     const SentMessage &msg)
{
    callbackCounters()._deliveryReport++;
    callbackCounters()._opaque = msg.getOpaque();
}

int partition(const cppkafka::Buffer& key,
              int32_t partitionCount)
{
    //FNV-1a hash
    static const long long prime = 1099511628211;
    static const long long offset = 0xcbf29ce484222325;
    unsigned long long result = offset;
    for (auto it = key.begin(); it != key.end(); ++it) {
        result = result ^ *it;
        result = result * prime;
    }
    return result % partitionCount;
}

int32_t Callbacks::partitioner(const ProducerMetadata& metadata,
                               const cppkafka::Buffer& key,
                               int32_t partitionCount)
{
    callbackCounters()._partitioner++;
    return partition(key, partitionCount);
}

void Callbacks::handleQueueFull(const ProducerMetadata &metadata,
                                const SentMessage &message)
{
    callbackCounters()._queueFull++;
}

void Callbacks::handleOffsetCommit(const ConsumerMetadata &metadata,
                                   cppkafka::Error error,
                                   const cppkafka::TopicPartitionList &topicPartitions,
                                   const std::vector<void*>& opaques)
{
    callbackCounters()._offsetCommit++;
    for (auto&& part : topicPartitions) {
        if (part.get_offset() != (int)OffsetPoint::Invalid) {
            callbackCounters()._offsetCommitPartitions[part] = part.get_offset()-1;
        }
    }
    if (!opaques.empty()) {
        callbackCounters()._opaque = opaques.front();
    }
}

void processMessages(TopicWithHeaders::ReceivedMessageType&& message)
{
    if (message.isHeaderValidAt<0>() && message.isHeaderValidAt<1>()) {
        MessageTracker::Info info{(SenderId)message.getHeaderAt<0>()._senderId,
                                      message.getHeaderAt<0>(),
                                      message.getHeaderAt<1>(),
                                      message.getPayload()};
        info._partition = message.getPartition();
        info._offset = message.getOffset();
        consumerMessageTracker().add(std::move(info));
    }
    else if (message.isHeaderValidAt<0>()) {
        MessageTracker::Info info{(SenderId)SenderId::SyncSecondHeaderMissing,
                                      message.getHeaderAt<0>(),
                                      message.getPayload()};
        info._partition = message.getPartition();
        info._offset = message.getOffset();
        consumerMessageTracker().add(std::move(info));
    }
    else if (message.isHeaderValidAt<1>()) {
        MessageTracker::Info info{(SenderId)SenderId::SyncFirstHeaderMissing,
                                      message.getHeaderAt<1>(),
                                      message.getPayload()};
        info._partition = message.getPartition();
        info._offset = message.getOffset();
        consumerMessageTracker().add(std::move(info));
    }
    else {
        //only have payload
        MessageTracker::Info info{(SenderId)SenderId::SyncBothHeadersMissing,
                                      message.getPayload()};
        info._partition = message.getPartition();
        info._offset = message.getOffset();
        consumerMessageTracker().add(std::move(info));
    }
}

void Callbacks::messageReceiverWithHeaders(TopicWithHeaders::ReceivedMessageType message)
{
    callbackCounters()._receiverIoThread = quantum::local::context() == nullptr;
    callbackCounters()._receiver++;
    if (!message) {
        callbackCounters()._messageErrors++;
        return;
    }
    if (message.isEof()) {
        callbackCounters()._eof++;
        return;
    }
    if (message.skip()) {
        callbackCounters()._skip++;
        return;
    }
    processMessages(std::move(message));
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
}

void Callbacks::messageReceiverWithHeadersManualCommit(TopicWithHeaders::ReceivedMessageType message)
{
    message.commit();
    messageReceiverWithHeaders(std::move(message));
}

void Callbacks::messageReceiverWithoutHeaders(TopicWithoutHeaders::ReceivedMessageType message)
{
    callbackCounters()._receiverIoThread = quantum::local::context() == nullptr;
    callbackCounters()._receiver++;
    if (!message) {
        callbackCounters()._messageErrors++;
        return;
    }
    if (message.isEof()) {
        int offset = message.getOffset();
        callbackCounters()._eof++;
        return;
    }
    //valid message
    MessageTracker::Info info{SenderId::SyncWithoutHeaders, std::move(message).getPayload()};
    info._partition = message.getPartition();
    info._offset = message.getOffset();
    consumerMessageWithoutHeadersTracker().add(std::move(info));
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
}

void Callbacks::handleRebalance(const ConsumerMetadata& metadata,
                                cppkafka::Error error,
                                cppkafka::TopicPartitionList& topicPartitions)
{
    if (error.get_error() == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS) {
        callbackCounters()._assign++;
    }
    else if (error.get_error() == RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS) {
        callbackCounters()._revoke++;
    }
    else {
        callbackCounters()._rebalance++;
    }
}

}}}
