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

#include <corokafka/interface/corokafka_iconnector.h>
#include <corokafka/corokafka_configuration_builder.h>
#include <corokafka/corokafka_callbacks.h>
#include <corokafka/corokafka_consumer_topic_entry.h>
#include <quantum/quantum.h>
#include <boost/any.hpp>
#include <unordered_map>
#include <atomic>
#include <list>

namespace Bloomberg {
namespace corokafka {

using MessageContainer = quantum::Buffer<cppkafka::Message>;

class ConsumerManagerImpl : public IConsumerManager
{
public:
    using ConfigMap = ConfigurationBuilder::ConfigMap<ConsumerConfiguration>;
    using ReceivedBatch = std::vector<std::tuple<cppkafka::Message, DeserializedMessage>>;
    
    ConsumerManagerImpl(quantum::Dispatcher& dispatcher,
                        const ConnectorConfiguration& connectorConfiguration,
                        const ConfigMap& configs,
                        std::atomic_bool& interrupt);
    
    ConsumerManagerImpl(quantum::Dispatcher& dispatcher,
                        const ConnectorConfiguration& connectorConfiguration,
                        ConfigMap&& configs,
                        std::atomic_bool& interrupt);
    
    ~ConsumerManagerImpl();
    
    ConsumerMetadata getMetadata(const std::string& topic) final;
    
    void enablePreprocessing() final;
    
    void enablePreprocessing(const std::string& topic) final;
    
    void disablePreprocessing() final;
    
    void disablePreprocessing(const std::string& topic) final;
    
    void pause() final;
    
    void pause(const std::string& topic) final;
    
    void resume() final;
    
    void resume(const std::string& topic) final;
    
    void subscribe(const cppkafka::TopicPartitionList& partitionList) final;
    
    void subscribe(const std::string& topic,
                   const cppkafka::TopicPartitionList& partitionList) final;
    
    void unsubscribe() final;
    
    void unsubscribe(const std::string& topic) final;
    
    cppkafka::Error commit(const cppkafka::TopicPartition& topicPartition,
                           const void* opaque) final;
    
    cppkafka::Error commit(const cppkafka::TopicPartition& topicPartition,
                           ExecMode execMode,
                           const void* opaque) final;
    
    cppkafka::Error commit(const cppkafka::TopicPartitionList& topicPartitions,
                           const void* opaque) final;
    
    cppkafka::Error commit(const cppkafka::TopicPartitionList& topicPartitions,
                           ExecMode execMode,
                           const void* opaque) final;
    
    void shutdown() final;
    
    const ConsumerConfiguration& getConfiguration(const std::string& topic) const final;
    
    std::vector<std::string> getTopics() const;
    
    void poll();
    
    void pollEnd();
    
    //Callbacks
    static void errorCallbackInternal(ConsumerTopicEntry& topicEntry,
                                      cppkafka::KafkaHandleBase& handle,
                                      int error,
                                      const std::string& reason);
    static void errorCallback(ConsumerTopicEntry& topicEntry,
                              cppkafka::KafkaHandleBase& handle,
                              cppkafka::Error error,
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
                                     const cppkafka::Message& rawMessage);
    static void assignmentCallback(ConsumerTopicEntry& topicEntry,
                                   cppkafka::TopicPartitionList& topicPartitions);
    static void assignmentCallbackImpl(ConsumerTopicEntry& topicEntry,
                                       cppkafka::TopicPartitionList& topicPartitions);
    static void revocationCallback(ConsumerTopicEntry& topicEntry,
                                   const cppkafka::TopicPartitionList& topicPartitions);
    static void revocationCallbackImpl(ConsumerTopicEntry& topicEntry,
                                       const cppkafka::TopicPartitionList& topicPartitions);
    static void rebalanceErrorCallback(ConsumerTopicEntry& topicEntry,
                                       cppkafka::Error error);
    static void rebalanceErrorCallbackImpl(ConsumerTopicEntry& topicEntry,
                                           cppkafka::Error error);
    //log + error callback wrapper
    static void report(ConsumerTopicEntry& topicEntry,
                       cppkafka::LogLevel level,
                       cppkafka::Error error,
                       const std::string& reason,
                       const cppkafka::Message* message);
    
    void adjustThrottling(ConsumerTopicEntry& topicEntry,
                          const std::chrono::steady_clock::time_point& now);
    
    //Coroutines and async IO
    static MessageBatch messageBatchReceiveTask(ConsumerTopicEntry& entry, IoTracker);

    template <typename POLLER>
    static int messageReceiveTask(quantum::ThreadPromise<MessageContainer>::Ptr promise,
                                  POLLER& poller,
                                  ConsumerTopicEntry& entry,
                                  IoTracker);
    static int pollCoro(quantum::VoidContextPtr ctx,
                        ConsumerTopicEntry& entry,
                        IoTracker);
    static int processMessage(quantum::VoidContextPtr ctx,
                              ConsumerTopicEntry& entry,
                              cppkafka::Message&& kafkaMessage);
    static int invokeReceiver(ConsumerTopicEntry& entry,
                              cppkafka::Message&& kafkaMessage,
                              IoTracker);

    // Batch processing coroutines and callbacks
    static void processMessageBatch(quantum::VoidContextPtr ctx,
                                    ConsumerTopicEntry& entry,
                                    MessageBatch&& kafkaMessages);
    static int pollBatchCoro(quantum::VoidContextPtr ctx,
                             ConsumerTopicEntry& entry,
                             IoTracker);
    static int receiveMessageBatch(ConsumerTopicEntry& entry,
                                   MessageBatch&& rawMessages,
                                   IoTracker);
    //Misc methods
    void setup(const std::string& topic, ConsumerTopicEntry& topicEntry);
    static void exceptionHandler(const std::exception& ex,
                                 const ConsumerTopicEntry& topicEntry);
    static ConsumerMetadata makeMetadata(const ConsumerTopicEntry& topicEntry);
    static int mapPartitionToQueue(int partition,
                                   const ConsumerTopicEntry& topicEntry);
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
    bool hasNewMessages(ConsumerTopicEntry& entry) const;
    
    struct DeserializeVisitor : boost::static_visitor<DeserializedMessage> {
       DeserializedMessage operator()(DeserializedMessage& msg) const {
           return std::move(msg);
       }
       DeserializedMessage operator()(quantum::CoroContext<DeserializedMessage>::Ptr& future) const {
           return future ? future->get(quantum::local::context()) : DeserializedMessage{};
       }
    };
    
private:
    using ConsumerFunc = void(ConsumerType::*)();
    
    void pause(bool pause, ConsumerFunc);
    
    static void pauseImpl(ConsumerTopicEntry& topicEntry, bool pause, ConsumerFunc);
    
    static void subscribeImpl(ConsumerTopicEntry& topicEntry,
                              const cppkafka::TopicPartitionList& partitionList);
    
    static void unsubscribeImpl(ConsumerTopicEntry& topicEntry);
    
    cppkafka::Error commitImpl(const cppkafka::TopicPartitionList& topicPartitions,
                               ExecMode* execMode,
                               const void* opaque);
    
    static cppkafka::Error commitImpl(ConsumerTopicEntry& entry,
                                      const cppkafka::TopicPartitionList& topicPartitions,
                                      ExecMode execMode,
                                      const void* opaque);
    
    // Members
    quantum::Dispatcher&            _dispatcher;
    const ConnectorConfiguration&   _connectorConfiguration;
    Consumers                       _consumers;
    std::atomic_flag                _shutdownInitiated{false};
    std::chrono::milliseconds       _shutdownIoWaitTimeoutMs{2000};
};

template <typename POLLER>
int ConsumerManagerImpl::messageReceiveTask(quantum::ThreadPromise<MessageContainer>::Ptr promise,
                                            POLLER& poller,
                                            ConsumerTopicEntry& entry,
                                            IoTracker)
{
    try {
        int readSize = entry._readSize;
        auto endTime = std::chrono::steady_clock::now() + entry._pollTimeout;
        auto timeout = (entry._minPollInterval.count() == EnumValue(TimerValues::Disabled)) ?
                entry._pollTimeout : entry._minPollInterval;
        
        //Get messages until the batch is filled or until the timeout expires
        while (((entry._readSize == EnumValue(SizeLimits::Unlimited)) || (readSize > 0)) &&
               ((entry._pollTimeout.count() == EnumValue(TimerValues::Unlimited)) || (std::chrono::steady_clock::now() < endTime)) &&
               !entry._interrupt) {
            cppkafka::Message message = poller.poll(timeout);
            if (message) {
                --readSize;
                //We have a valid message
                promise->push(std::move(message));
            }
        }
    }
    catch (const std::exception& ex) {
        exceptionHandler(ex, entry);
    }
    return promise->closeBuffer();
}

}}

#endif //BLOOMBERG_COROKAFKA_CONSUMER_MANAGER_IMPL_H
