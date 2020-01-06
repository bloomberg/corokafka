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
#ifndef BLOOMBERG_COROKAFKA_CONSUMER_CONFIGURATION_H
#define BLOOMBERG_COROKAFKA_CONSUMER_CONFIGURATION_H

#include <corokafka/corokafka_callbacks.h>
#include <corokafka/corokafka_utils.h>
#include <corokafka/corokafka_topic_configuration.h>
#include <corokafka/corokafka_type_erased_deserializer.h>

namespace Bloomberg {
namespace corokafka {

//========================================================================
//                       CONSUMER CONFIGURATION
//========================================================================
/**
 * @brief The ConsumerConfiguration is a builder class which contains
 *        configuration information for a specific topic. This configuration consists
 *        of both RdKafka and CoroKafka configuration options as per documentation
 *        (see CONFIGURATION.md in the respective projects).
 *        At a minimum, the user should supply a 'metadata.broker.list' in the constructor 'options'.
 */
class ConsumerConfiguration : public TopicConfiguration
{
    friend class TopicConfiguration;
    friend class ConsumerManagerImpl;
public:
    /**
     * @brief Internal CoroKafka-specific options for the consumer. They are used to control this
     *        library's behavior for consumers and are complementary to the RdKafka consumer options.
     *        For more details please read CONFIGURATION.md document.
     */
    struct Options
    {
        static constexpr const char* autoOffsetPersist =                "internal.consumer.auto.offset.persist";
        static constexpr const char* autoOffsetPersistOnException =     "internal.consumer.auto.offset.persist.on.exception";
        static constexpr const char* autoThrottle =                     "internal.consumer.auto.throttle";
        static constexpr const char* autoThrottleMultiplier =           "internal.consumer.auto.throttle.multiplier";
        static constexpr const char* batchPrefetch =                    "internal.consumer.batch.prefetch";
        static constexpr const char* commitBackoffStrategy =            "internal.consumer.commit.backoff.strategy";
        static constexpr const char* commitBackoffIntervalMs =          "internal.consumer.commit.backoff.interval.ms";
        static constexpr const char* commitExec =                       "internal.consumer.commit.exec";
        static constexpr const char* commitMaxBackoffMs =               "internal.consumer.commit.max.backoff.ms";
        static constexpr const char* commitNumRetries =                 "internal.consumer.commit.num.retries";
        static constexpr const char* logLevel =                         "internal.consumer.log.level";
        static constexpr const char* offsetPersistStrategy =            "internal.consumer.offset.persist.strategy";
        static constexpr const char* pauseOnStart =                     "internal.consumer.pause.on.start";
        static constexpr const char* pollStrategy =                     "internal.consumer.poll.strategy";
        static constexpr const char* pollTimeoutMs =                    "internal.consumer.poll.timeout.ms";
        static constexpr const char* preprocessMessages =               "internal.consumer.preprocess.messages";
        static constexpr const char* preprocessInvokeThread =           "internal.consumer.preprocess.invoke.thread";
        static constexpr const char* readSize =                         "internal.consumer.read.size";
        static constexpr const char* receiveCallbackExec =              "internal.consumer.receive.callback.exec";
        static constexpr const char* receiveCallbackThreadRangeLow =    "internal.consumer.receive.callback.thread.range.low";
        static constexpr const char* receiveCallbackThreadRangeHigh =   "internal.consumer.receive.callback.thread.range.high";
        static constexpr const char* receiveInvokeThread =              "internal.consumer.receive.invoke.thread";
        static constexpr const char* skipUnknownHeaders =               "internal.consumer.skip.unknown.headers";
        static constexpr const char* timeoutMs =                        "internal.consumer.timeout.ms";
    };
    
    /**
     * @brief Create a consumer configuration.
     * @tparam TOPIC Type Topic<KEY,PAYLOAD,HEADERS> which represents this consumer.
     * @param topic The topic object to which this configuration applies.
     * @param options The consumer configuration options (for both RdKafka and CoroKafka).
     * @param topicOptions The topic configuration options (for both RdKafka and CoroKafka).
     * @param receiver The receiver function on which all messages are delivered.
     * @note 'metadata.broker.list' must be supplied in 'options'.
     */
    template <typename TOPIC>
    ConsumerConfiguration(const TOPIC& topic,
                          Configuration::OptionList options,
                          Configuration::OptionList topicOptions,
                          Callbacks::ReceiverCallback<TOPIC> receiver);
    template <typename TOPIC>
    ConsumerConfiguration(const TOPIC& topic,
                          std::initializer_list<cppkafka::ConfigurationOption> options,
                          std::initializer_list<cppkafka::ConfigurationOption> topicOptions,
                          Callbacks::ReceiverCallback<TOPIC> receiver);
    
    /**
     * @brief Create a consumer configuration.
     * @param topic The topic name to which this configuration applies.
     * @param options The consumer configuration options (for both RdKafka and CoroKafka).
     * @param topicOptions The topic configuration options (for both RdKafka and CoroKafka).
     * @note When using this constructor, the application must call 'setReceiverCallback()' below.
     * @note 'metadata.broker.list' must be supplied in 'options'.
     */
    ConsumerConfiguration(const std::string& topic,
                          Configuration::OptionList options,
                          Configuration::OptionList topicOptions);
    ConsumerConfiguration(const std::string& topic,
                          std::initializer_list<cppkafka::ConfigurationOption> options,
                          std::initializer_list<cppkafka::ConfigurationOption> topicOptions);
    
    /**
     * @brief Assign partitions and offsets on startup for this consumer.
     * @param strategy The strategy to use for this consumer.
     * @param partitions The partition list.
     * @remark When 'strategy == static', the partitions provided will be used in a call to rdkafka::rd_kafka_assign().
     *         When 'strategy == dynamic', the partition list *must* contain all partitions for this topic in order
     *         to cover any possible partition combinations assigned by Kafka. This setting will result in a call
     *         to rdkafka::rd_kafka_subscribe().
     */
    void assignInitialPartitions(PartitionStrategy strategy,
                                 cppkafka::TopicPartitionList partitions);
    
    /**
     * @brief Get the partition strategy used by this consumer.
     * @return The strategy.
     * @remark The default strategy is 'Dynamic' unless set otherwise via assignInitialPartitions().
     */
    PartitionStrategy getPartitionStrategy() const;
    
    /**
     * @brief Get the initial partition assignment.
     * @return The partition assignment.
     */
    const cppkafka::TopicPartitionList& getInitialPartitionAssignment() const;
    
    /**
     * @brief Set the offset commit callback.
     * @param callback The callback.
     */
    void setOffsetCommitCallback(Callbacks::OffsetCommitCallback callback);
    
    /**
     * @brief Get the offset commit callback.
     * @return The callback.
     */
    const Callbacks::OffsetCommitCallback& getOffsetCommitCallback() const;
    
    /**
     * @brief Set the rebalance callback.
     * @param callback The callback.
     * @remark This library handles all partition assignments and revocations internally.
     *         As such, setting this callback is entirely optional and discretionary.
     */
    void setRebalanceCallback(Callbacks::RebalanceCallback callback);
    
    /**
     * @brief Get the rebalance callback.
     * @return The callback.
     */
    const Callbacks::RebalanceCallback& getRebalanceCallback() const;
    
    /**
     * @brief Set the preprocessor callback. This will be called before a message is de-serialized.
     * @param callback The callback.
     * @note The callback should return 'true' if the message should be skipped.
     */
    void setPreprocessorCallback(Callbacks::PreprocessorCallback callback);
    
    /**
     * @brief Get the preprocessor callback.
     * @return The callback.
     */
    const Callbacks::PreprocessorCallback& getPreprocessorCallback() const;
    
    /**
     * @brief Set the receiver function.
     * @tparam TOPIC Type Topic<KEY,PAYLOAD,HEADERS> which represents this consumer.
     * @param callback The receiver function on which all messages are delivered.
     * @remark Setting a receiver callback is mandatory.
     */
    template <typename TOPIC>
    void setReceiverCallback(const TOPIC& topic, Callbacks::ReceiverCallback<TOPIC> receiver);
    
    /**
     * @brief Get the receiver callback.
     * @tparam TOPIC Type Topic<KEY,PAYLOAD,HEADERS>
     * @return The callback.
     */
    template <typename TOPIC>
    const Callbacks::ReceiverCallback<TOPIC>& getReceiverCallback() const;
    
private:
    const TypeErasedDeserializer& getTypeErasedDeserializer() const;
    const Receiver& getTypeErasedReceiver() const;
    
    Callbacks::OffsetCommitCallback         _offsetCommitCallback;
    Callbacks::RebalanceCallback            _rebalanceCallback;
    Callbacks::PreprocessorCallback         _preprocessorCallback;
    TypeErasedDeserializer                  _typeErasedDeserializer;
    std::shared_ptr<Receiver>               _receiver;
    cppkafka::TopicPartitionList            _initialPartitionList;
    PartitionStrategy                       _strategy{PartitionStrategy::Dynamic};
    static const OptionSet                  s_internalOptions;
    static const OptionSet                  s_internalTopicOptions;
    static const std::string                s_internalOptionsPrefix;
};

}
}

#include <corokafka/impl/corokafka_consumer_configuration_impl.h>

#endif //BLOOMBERG_COROKAFKA_CONSUMER_CONFIGURATION_H
