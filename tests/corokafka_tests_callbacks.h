#ifndef BLOOMBERGLP_COROKAFKA_TESTS_CALLBACKS_H
#define BLOOMBERGLP_COROKAFKA_TESTS_CALLBACKS_H

#include <corokafka/corokafka.h>
#include <map>

namespace Bloomberg {
namespace corokafka {
namespace tests {

struct CallbackCounters
{
    void reset() { *this = CallbackCounters(); }
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
};

inline
CallbackCounters& callbackCounters() { static CallbackCounters _counters; return counters; }

struct Callbacks
{
    static void handleKafkaError(const ck::Metadata &metadata,
                                 cppkafka::Error error,
                                 const std::string &reason,
                                 void *opaque);
    
    static void handleThrottling(const ck::Metadata &metadata,
                                 const std::string &broker_name,
                                 int32_t brokerId,
                                 std::chrono::milliseconds throttleTime);
    
    static void kafkaLogger(const ck::Metadata &metadata,
                            cppkafka::LogLevel level,
                            const std::string &facility,
                            const std::string &message);
    
    static void connectorLogger(cppkafka::LogLevel level,
                                const std::string &facility,
                                const std::string &message);
    
    static void handleStats(const ck::Metadata &metadata,
                            const std::string &json);
    
    static bool messagePreprocessor(cppkafka::TopicPartition hint);
    
    // ============================================== PRODUCER =========================================
    static void handleDeliveryReport(const ck::ProducerMetadata &metadata,
                                     const ck::SentMessage &msg);
    
    static int32_t partitioner(const ck::ProducerMetadata &metadata,
                               const cppkafka::Buffer &key,
                               int32_t partition_count);
    
    static void handleQueueFull(const ck::ProducerMetadata &metadata,
                                const ck::SentMessage &message);
    
    // ============================================== CONSUMER =========================================
    static void handleOffsetCommit(const ck::ConsumerMetadata &metadata,
                                   cppkafka::Error error,
                                   const cppkafka::TopicPartitionList &topicPartitions,
                                   const std::vector<const void *> &opaques);
    
    static void messageReceiver(MessageWithHeaders message);
    static void messageReceiver(MessageWithoutHeaders message);
}}}

#endif //BLOOMBERGLP_COROKAFKA_TESTS_CALLBACKS_H
