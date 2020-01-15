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
#ifndef BLOOMBERG_COROKAFKA_CONSUMER_MANAGER_IMPL_H
#define BLOOMBERG_COROKAFKA_CONSUMER_MANAGER_IMPL_H

#include <unordered_map>
#include <atomic>
#include <list>
#include <quantum/quantum.h>
#include <corokafka/corokafka_metadata.h>
#include <corokafka/corokafka_configuration_builder.h>
#include <corokafka/corokafka_callbacks.h>
#include <corokafka/corokafka_consumer_topic_entry.h>
#include <corokafka/corokafka_utils.h>
#include <boost/any.hpp>

namespace Bloomberg {
namespace corokafka {

using MessageContainer = quantum::Buffer<cppkafka::Message>;

class ConsumerManagerImpl : public Interruptible
{
    friend class ConsumerManager;
public:
    ~ConsumerManagerImpl();
    
private:
    using ConfigMap = ConfigurationBuilder::ConfigMap<ConsumerConfiguration>;
    using DeserializedMessage = std::tuple<boost::any, boost::any, HeaderPack, DeserializerError>;
    using MessageTuple = std::tuple<cppkafka::Message, quantum::CoroContext<DeserializedMessage>::Ptr>;
    using ReceivedBatch = std::vector<std::tuple<cppkafka::Message, DeserializedMessage>>;
    
    ConsumerManagerImpl(quantum::Dispatcher& dispatcher,
                        const ConnectorConfiguration& connectorConfiguration,
                        const ConfigMap& configs);
    ConsumerManagerImpl(quantum::Dispatcher& dispatcher,
                        const ConnectorConfiguration& connectorConfiguration,
                        ConfigMap&& configs);
    
    ConsumerMetadata getMetadata(const std::string& topic);
    
    void preprocess(bool enable, const std::string& topic);
    
    void pause(const std::string& topic);
    
    void resume(const std::string& topic);
    
    void subscribe(const std::string& topic,
                   cppkafka::TopicPartitionList partitionList);
    
    void unsubscribe(const std::string& topic);
    
    cppkafka::Error commit(const cppkafka::TopicPartition& topicPartition,
                 const void* opaque,
                 bool forceSync);
    
    cppkafka::Error commit(const cppkafka::TopicPartitionList& topicPartitions,
                 const void* opaque,
                 bool forceSync);
    
    cppkafka::Error commitImpl(ConsumerTopicEntry& topicEntry,
                     const cppkafka::TopicPartitionList& topicPartitions,
                     const void* opaque,
                     bool forceSync);
    
    void shutdown();
    
    void poll();
    
    void setConsumerBatchSize(size_t size);
    
    size_t getConsumerBatchSize() const;
    
    const ConsumerConfiguration& getConfiguration(const std::string& topic) const;
    
    std::vector<std::string> getTopics() const;
    
    //Callbacks
    static void errorCallback2(ConsumerTopicEntry& topicEntry,
                               cppkafka::KafkaHandleBase& handle,
                               int error,
                               const std::string& reason);
    static void errorCallback(ConsumerTopicEntry& topicEntry,
                              cppkafka::KafkaHandleBase& handle,
                              int error,
                              const std::string& reason,
                              const cppkafka::Message* opaque);
    static void throttleCallback(ConsumerTopicEntry& topicEntry,
                                 cppkafka::KafkaHandleBase& handle,
                                 const std::string& brokerName,
                                 int32_t brokerId,
                                 std::chrono::milliseconds throttleDuration);
    static void logCallback(ConsumerTopicEntry& topicEntry,
                            cppkafka::KafkaHandleBase& handle,
                            int level,
                            const std::string& facility,
                            const std::string& message);
    static void statsCallback(ConsumerTopicEntry& topicEntry,
                              cppkafka::KafkaHandleBase& handle,
                              const std::string& json);
    static void offsetCommitCallback(ConsumerTopicEntry& topicEntry,
                                     cppkafka::Consumer& consumer,
                                     cppkafka::Error error,
                                     const cppkafka::TopicPartitionList& topicPartitions);
    static bool offsetCommitErrorCallback(ConsumerTopicEntry& topicEntry,
                                          cppkafka::Error error);
    static bool preprocessorCallback(ConsumerTopicEntry& topicEntry,
                                     cppkafka::TopicPartition hint);
    static void assignmentCallback(ConsumerTopicEntry& topicEntry,
                                   cppkafka::TopicPartitionList& topicPartitions);
    static void revocationCallback(ConsumerTopicEntry& topicEntry,
                                   const cppkafka::TopicPartitionList& topicPartitions);
    static void rebalanceErrorCallback(ConsumerTopicEntry& topicEntry,
                                       cppkafka::Error error);
    //log + error callback wrapper
    static void report(ConsumerTopicEntry& topicEntry,
                       cppkafka::LogLevel level,
                       int error,
                       const std::string& reason,
                       const cppkafka::Message* message);
    
    void adjustThrottling(ConsumerTopicEntry& topicEntry,
                          const std::chrono::steady_clock::time_point& now);
    
    //Coroutines and async IO
    static std::vector<cppkafka::Message> messageBatchReceiveTask(ConsumerTopicEntry& entry);
    static int messageRoundRobinReceiveTask(quantum::ThreadPromise<MessageContainer>::Ptr promise,
                                            ConsumerTopicEntry& entry);
    static DeserializedMessage deserializeCoro(
                                quantum::VoidContextPtr ctx,
                                ConsumerTopicEntry& entry,
                                const cppkafka::Message& kafkaMessage);
    static std::vector<bool> executePreprocessorCallbacks(
                                  quantum::VoidContextPtr ctx,
                                  ConsumerTopicEntry& entry,
                                  const std::vector<cppkafka::Message>& messages);
    static std::vector<DeserializedMessage> deserializeBatchCoro(
                                    quantum::VoidContextPtr ctx,
                                    ConsumerTopicEntry& entry,
                                    const std::vector<cppkafka::Message>& messages);
    static std::deque<MessageTuple> pollCoro(
                        quantum::VoidContextPtr ctx,
                        ConsumerTopicEntry& entry);
    static int processorCoro(quantum::VoidContextPtr ctx,
                             ConsumerTopicEntry& entry);
    static int invokeReceiver(ConsumerTopicEntry& entry,
                              cppkafka::Message&& kafkaMessage,
                              DeserializedMessage&& deserializedMessage);
    static int receiverTask(ConsumerTopicEntry& entry,
                            cppkafka::Message&& kafkaMessage,
                            DeserializedMessage&& deserializedMessage);

    // Batch processing coroutines and callbacks
    static void processMessageBatchOnIoThreads(quantum::VoidContextPtr ctx,
                                               ConsumerTopicEntry& entry,
                                               std::vector<cppkafka::Message>&& raw,
                                               std::vector<DeserializedMessage>&& deserializedMessages);
    static int pollBatchCoro(quantum::VoidContextPtr ctx,
                             ConsumerTopicEntry& entry);
    static int receiverMultipleBatchesTask(ConsumerTopicEntry& entry,
                                           ReceivedBatch&& messageBatch);
    static int invokeSingleBatchReceiver(ConsumerTopicEntry& entry,
                                     std::vector<cppkafka::Message>&& rawMessages,
                                     std::vector<DeserializedMessage>&& deserializedMessages);
    static int receiverSingleBatchTask(ConsumerTopicEntry& entry,
                                       std::vector<cppkafka::Message>&& rawMessages,
                                       std::vector<DeserializedMessage>&& deserializedMessages);
    static bool preprocessorTask(ConsumerTopicEntry& entry,
                                 const cppkafka::Message& kafkaMessage);
    //Misc methods
    void setup(const std::string& topic, ConsumerTopicEntry& topicEntry);
    static void exceptionHandler(const std::exception& ex,
                                 const ConsumerTopicEntry& topicEntry);
    static ConsumerMetadata makeMetadata(const ConsumerTopicEntry& topicEntry);
    static int mapPartitionToQueue(int partition,
                                     const std::pair<int,int>& range);
    static DeserializedMessage
    deserializeMessage(ConsumerTopicEntry& entry,
                       const cppkafka::Message& kafkaMessage);
    
    static OffsetPersistSettings makeOffsetPersistSettings(const ConsumerTopicEntry& topicEntry);
    
    using Consumers = std::unordered_map<std::string,
                                         ConsumerTopicEntry,
                                         std::hash<std::string>,
                                         StringEqualCompare>; //index by topic
                                         
    Consumers::iterator findConsumer(const std::string& topic);
    Consumers::const_iterator findConsumer(const std::string& topic) const;
    
    // Members
    quantum::Dispatcher&        _dispatcher;
    Consumers                   _consumers;
    size_t                      _batchSize;
    std::atomic_flag            _shutdownInitiated{0};
};

}}

#endif //BLOOMBERG_COROKAFKA_CONSUMER_MANAGER_IMPL_H
