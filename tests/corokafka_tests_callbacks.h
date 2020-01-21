#ifndef BLOOMBERGLP_COROKAFKA_TESTS_CALLBACKS_H
#define BLOOMBERGLP_COROKAFKA_TESTS_CALLBACKS_H

#include <corokafka/corokafka.h>
#include <corokafka_tests_topics.h>
#include <map>
#include <atomic>

namespace Bloomberg {
namespace corokafka {
namespace tests {

struct CallbackCounters
{
    void reset()
    {
        _error = 0;
        _throttle = 0;
        _logger = 0;
        _connectorLogger = 0;
        _stats = 0;
        _deliveryReport = 0;
        _partitioner = 0;
        _queueFull = 0;
        _offsetCommit = 0;
        _assign = 0;
        _revoke = 0;
        _rebalance = 0;
        _offsetCommitPartitions.clear();
        _receiver = 0;
        _eof = 0;
        _skip = 0;
        _messageErrors = 0;
        _preprocessor = 0;
        _opaque = nullptr;
        _receiverIoThread = true;
        _preprocessorIoThread = true;
        _forceSkip = false;
    }
    
    //callback stats (single threaded since called from polling thread)
    int _error{0};
    int _throttle{0};
    int _logger{0};
    int _connectorLogger{0};
    int _stats{0};
    int _deliveryReport{0};
    int _partitioner{0};
    int _queueFull{0};
    int _offsetCommit{0};
    std::map<cppkafka::TopicPartition, int> _offsetCommitPartitions;
    int _assign{0};
    int _revoke{0};
    int _rebalance{0};
    
    //receiver and preprocessor stats (multi-threaded since called from io or coroutine threads)
    std::atomic_int _receiver{0};
    std::atomic_int _eof{0};
    std::atomic_int _skip{0};
    std::atomic_int _messageErrors{0};
    std::atomic_int _preprocessor{0};
    
    void* _opaque{nullptr};
    bool _receiverIoThread{true};
    bool _preprocessorIoThread{true};
    bool _forceSkip{false};
};

inline
quantum::Dispatcher& dispatcher()
{
    static quantum::Dispatcher _dispatcher({});
    return _dispatcher;
}

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
                               int32_t partitionCount);
    
    static void handleQueueFull(const ProducerMetadata &metadata,
                                const SentMessage &message);
    
    // ============================================== CONSUMER =========================================
    static void handleOffsetCommit(const ConsumerMetadata &metadata,
                                   cppkafka::Error error,
                                   const cppkafka::TopicPartitionList &topicPartitions,
                                   const std::vector<void*> &opaques);
    
    static void messageReceiverWithHeaders(TopicWithHeaders::ReceivedMessageType message);
    
    static void messageReceiverWithHeadersManualCommit(TopicWithHeaders::ReceivedMessageType message);
    
    static void messageReceiverWithoutHeaders(TopicWithoutHeaders::ReceivedMessageType message);
    
    static void handleRebalance(const ConsumerMetadata& metadata,
                                cppkafka::Error error,
                                cppkafka::TopicPartitionList& topicPartitions);
};

}}}

#endif //BLOOMBERGLP_COROKAFKA_TESTS_CALLBACKS_H
