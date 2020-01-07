#include <corokafka_tests_callbacks.h>

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
    return false;
}

void Callbacks::handleDeliveryReport(const ProducerMetadata &metadata,
                                     const SentMessage &msg)
{
    callbackCounters()._deliveryReport++;
}

int32_t Callbacks::partitioner(const ProducerMetadata& metadata,
                               const cppkafka::Buffer& key,
                               int32_t partition_count)
{
    callbackCounters()._partitioner++;
    //FNV-1a hash
    static const long long prime = 1099511628211;
    static const long long offset = 0xcbf29ce484222325;
    unsigned long long result = offset;
    for (auto it = key.begin(); it != key.end(); ++it) {
        result = result ^ *it;
        result = result * prime;
    }
    return result % partition_count;
}

void Callbacks::handleQueueFull(const ProducerMetadata &metadata,
                                const SentMessage &message)
{
    callbackCounters()._queueFull++;
}

void Callbacks::handleOffsetCommit(const ConsumerMetadata &metadata,
                                   cppkafka::Error error,
                                   const cppkafka::TopicPartitionList &topicPartitions,
                                   const std::vector<const void *>&)
{
    callbackCounters()._offsetCommit++;
}

void Callbacks::messageReceiverWithHeaders(TopicWithHeaders::ReceivedMessageType message)
{

}

void Callbacks::messageReceiverWithoutHeaders(TopicWithoutHeaders::ReceivedMessageType message)
{

}

}}
}
