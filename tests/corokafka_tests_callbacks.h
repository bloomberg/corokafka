#ifndef BLOOMBERGLP_COROKAFKA_TESTS_CALLBACKS_H
#define BLOOMBERGLP_COROKAFKA_TESTS_CALLBACKS_H

#include <corokafka/corokafka.h>
#include <corokafka_tests_topics.h>
#include <map>

namespace Bloomberg {
namespace corokafka {
namespace tests {

struct CallbackCounters
{
    void reset()
    { *this = CallbackCounters(); }
    
    int _error{0};
    int _throttle{0};
    int _logger{0};
    int _connectorLogger{0};
    int _stats{0};
    int _preprocessor{0};
    int _deliveryReport{0};
    int _partitioner{0};
    int _queueFull{0};
    int _offsetCommit{0};
    std::map<uint16_t, int> _receiver; //indexed by producer id
    void* _opaque{nullptr};
};

inline
CallbackCounters& callbackCounters()
{
    static CallbackCounters _counters;
    return _counters;
}

struct Callbacks
{
    static void handleKafkaError(const Metadata &metadata,
                                 cppkafka::Error error,
                                 const std::string &reason,
                                 void *opaque);
    
    static void handleThrottling(const Metadata &metadata,
                                 const std::string &broker_name,
                                 int32_t brokerId,
                                 std::chrono::milliseconds throttleTime);
    
    static void kafkaLogger(const Metadata &metadata,
                            cppkafka::LogLevel level,
                            const std::string &facility,
                            const std::string &message);
    
    static void connectorLogger(cppkafka::LogLevel level,
                                const std::string &facility,
                                const std::string &message);
    
    static void handleStats(const Metadata &metadata,
                            const std::string &json);
    
    static bool messagePreprocessor(cppkafka::TopicPartition hint);
    
    // ============================================== PRODUCER =========================================
    static void handleDeliveryReport(const ProducerMetadata &metadata,
                                     const SentMessage &msg);
    
    static int32_t partitioner(const ProducerMetadata &metadata,
                               const cppkafka::Buffer &key,
                               int32_t partition_count);
    
    static void handleQueueFull(const ProducerMetadata &metadata,
                                const SentMessage &message);
    
    // ============================================== CONSUMER =========================================
    static void handleOffsetCommit(const ConsumerMetadata &metadata,
                                   cppkafka::Error error,
                                   const cppkafka::TopicPartitionList &topicPartitions,
                                   const std::vector<void*> &opaques);
    
    static void messageReceiverWithHeaders(TopicWithHeaders::ReceivedMessageType message);
    
    static void messageReceiverWithoutHeaders(TopicWithoutHeaders::ReceivedMessageType message);
};

}}}

#endif //BLOOMBERGLP_COROKAFKA_TESTS_CALLBACKS_H
