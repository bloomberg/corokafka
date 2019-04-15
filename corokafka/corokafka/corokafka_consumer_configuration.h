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
#include <corokafka/corokafka_configuration.h>

namespace Bloomberg {
namespace corokafka {

//========================================================================
//                       CONSUMER CONFIGURATION
//========================================================================
/*
 * Kafka-connector specific configs
 * -----------------------------------------------------------------------
 * Description: These configuration options are additional to the rdkafka ones. Options
 *              pertaining to a consumer start with 'internal.consumer' and for topics start with 'internal.topic'.
 *              Default values are marked with a * or are in {}.
 *
 * internal.consumer.pause.on.start: {'true',*'false'}. The consumer starts in paused state.
 * User needs to call 'resume' to consume.
 *
 * internal.consumer.timeout.ms: {1000} Sets the timeout on any operations requiring a timeout such as poll,
 * query offsets, etc.
 *
 * internal.consumer.poll.timeout.ms: {0} Override the 'internal.consumer.timeout.ms' default setting for polling only.
 * Set to -1 to disable, in which case 'internal.consumer.timeout.ms' will take precedence. Note that in the case where
 * 'internal.consumer.poll.strategy=roundrobin', the actual timeout per message will be
 * 'internal.consumer.poll.timeout.ms/internal.consumer.read.size'. When value is 0, there is no timeout.
 *
 * internal.consumer.auto.offset.persist: {*'true','false'}. Enables auto-commit/auto-store inside the corokafka
 * once each ReceivedMessage has been destroyed.
 *
 * internal.consumer.offset.persist.strategy: {'commit',*'store'}. Determines if offsets are committed or stored
 * locally. Some rdkafka settings will be changed according to Note 2 below.
 *
 * internal.consumer.commit.exec: {'sync',*'async'}. Dictates if offset commits should be synchronous or asynchronous.
 *
 * internal.consumer.commit.num.retries: {MAX_UINT} Sets the number of times to retry committing an offset before giving up.
 *
 * internal.consumer.commit.backoff.strategy: {*'linear','exponential'} Backoff strategy when initial commit fails.
 *
 * internal.consumer.commit.backoff.interval.ms: {50} Time in ms between retries.
 *
 * internal.consumer.commit.max.backoff.ms: {1000} Maximum backoff time for retries. If set, this has higher precedence
 * than 'internal.consumer.commit.num.retries'.
 *
 * internal.consumer.poll.strategy: {'batch',*'roundrobin'}. Determines how messages are read from rdkafka queues.
 * The 'batch' strategy is to read the entire batch of messages from the main consumer queue. If 'roundrobin'
 * strategy is selected, messages are read in round-robin fashion from each partition at a time.
 *
 * internal.consumer.read.size: {100} Number of messages to read on each poll interval.
 * See ConnectorConfiguration::setPollInterval() for more details.
 *
 * internal.consumer.batch.prefetch: {'true',*'false'} If 'internal.consumer.poll.strategy=batch', start pre-fetching
 * the next batch while processing the current batch. This increases performance but may cause additional burden
 * on the broker.
 *
 * internal.consumer.receive.callback.thread.range.low: {0} Specifies the lowest thread id on which receive callbacks
 * will be called. See note 1 below for more details.
 *
 * internal.consumer.receive.callback.thread.range.high: {total IO threads} Specifies the highest thread id on which
 * receive callbacks will be called. See note 1 below for more details. This setting is only valid if
 * 'internal.consumer.receive.invoke.thread=io'.
 *
 * internal.consumer.receive.callback.exec: {'sync',*'async'}. Call the receiver callback asynchronously.
 * When set to 'async' all received messages from a poll interval (see 'internal.consumer.read.size' above) are
 * enqueued on the IO threads into the specified thread range (see above) and the receiver callback is called asynchronously,
 * which means that another poll operation may start immediately. When set to 'sync', the corokafka will deliver each
 * message synchronously into the specified thread range (see above) and will wait until the entire batch is delivered
 * to the application, before making another poll operation. In the synchronous case, it's recommended not to do too much
 * processing inside the receiver callback since it delays the delivery of other messages and to dispatch the message
 * to some other work queue as soon as possible (see Note 1 for more details).
 *
 * internal.consumer.receive.invoke.thread: {*'io','coro'}. Determines where the receiver callback will be invoked from.
 * In the coroutine case, the execution mode will be forced to sync (i.e. 'internal.consumer.receive.callback.exec=sync')
 * since 'async' execution has the risk of delivering messages from two consecutive poll intervals out of order.
 * Receiving messages inside a coroutine could increase performance if the callbacks are non-blocking (or if the block
 * for very short intervals). When invoking on coroutines, the receiver callback will always be called serially for all
 * messages inside a particular poll batch.
 *
 * internal.consumer.log.level: {'emergency','alert','critical','error','warning','notice',*'info','debug'}.
 * Sets the log level for this consumer. Note that if the rdkafka 'debug' property is set, internal.producer.log.level
 * will be automatically adjusted to 'debug'.
 *
 * internal.consumer.skip.unknown.headers: {*'true','false'}. If unknown headers are encountered (i.e. for which there
 * is no registered deserializer), they will be skipped. If set to false, an error will be thrown.
 *
 * internal.consumer.auto.throttle: {'true',*'false'}. When enabled, the consumers will be automatically paused/resumed
 * when throttled by the 'throttle time x multiplier'.
 *
 * internal.consumer.auto.throttle.multiplier: {1}. Change this value to pause the consumer by
 * 'throttle time x multiplier' instead. This only works if 'internal.consumer.auto.throttle=true'.
 *
 * internal.consumer.preprocess.messages: {*'true','false'}. Enable the preprocessor callback if it has been registered.
 * Otherwise it can be enabled/disabled via ConsumerManager::preprocess().
 *
 * internal.consumer.preprocess.invoke.thread: {*'io','coro'}. If 'io', run the preprocessor callback on an IO thread.
 * If 'coro', run it directly on the same coroutine threads where the messages are deserialized. The advantage of invoking
 * the preprocessor on an IO thread is that any blocking operation can be performed inside the callback. On the other
 * hand, if no IO operations are needed (or if they are very fast), using coroutine threads will result in better
 * performance.
 *
 * NOTE 1:
 * -------
 *      Mapping partitions unto IO threads for the receive callback allows for a maximum amount of parallelization
 *      while maintaining arrival order. These two variables allow to manually specify how to distribute 'P' partitions
 *      belonging to a topic unto 'T' IO threads when the receive callback is invoked. The default [low,high] values are
 *      [0,T-1] where T is set to 'numIoThreads' value used in the dispatcher constructor, and the distribution model
 *      is 'partition id % num threads' within the [low,high] range.
 *      This ensures that processing order for all messages is preserved. If a partition reassignment happens,
 *      the range remains unchanged and the new partitions will follow the same distribution mechanism.
 *
 *      Ex: There are 8 IO threads and 20 partitions assigned to this consumer. We set [low,high] range to
 *      [4,7] which means that the 20 partitions will be mapped unto 4 thread ids {4,5,6,7}. When message from
 *      partition 10 arrives, the thread id on which the receive callback will be invoked can be calculated as
 *      follows: 10 % (7-4+1) + 4 = 6.
 *
 * NOTE 2:
 * -------
 *      Commit strategy:
 *          'enable.auto.commit=false'
 *          'enable.auto.offset.store=false'
 *          'auto.commit.interval.ms=0'
 *      Store strategy:
 *          'enable.auto.commit=true'
 *          'enable.auto.offset.store=false'
 */
class ConsumerConfiguration : public Configuration
{
    friend class Configuration;
public:
    /**
     * @brief Create a consumer configuration.
     * @param topic The topic to which this configuration applies.
     * @param config The producer configuration options.
     * @param topicConfig The topic configuration options.
     */
    ConsumerConfiguration(const std::string& topic,
                          Options config,
                          Options topicConfig);
    
    /**
     * @brief Create a consumer configuration.
     * @param topic The topic to which this configuration applies.
     * @param config The producer configuration options.
     * @param topicConfig The topic configuration options.
     */
    ConsumerConfiguration(const std::string& topic,
                          std::initializer_list<ConfigurationOption> config,
                          std::initializer_list<ConfigurationOption> topicConfig);
    
    using Configuration::setCallback;
    
    /**
     * @brief Assign partitions and offsets on startup for this consumer.
     * @param strategy The strategy to use for this consumer.
     * @param partitions The partition list.
     * @remark When 'strategy == static', the partitions provided will be used in a call to librdkafka::rd_kafka_assign().
     *         When 'strategy == dynamic', the partition list *must* contain all partitions for this topic in order
     *         to cover any possible partition combinations assigned by Kafka. This setting will result in a call
     *         to librdkafka::rd_kafka_subscribe().
     */
    void assignInitialPartitions(PartitionStrategy strategy,
                                 TopicPartitionList partitions);
    
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
    const TopicPartitionList& getInitialPartitionAssignment() const;
    
    /**
     * @brief Set the offset commit callback.
     * @param callback The callback.
     */
    void setCallback(Callbacks::OffsetCommitCallback callback);
    
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
    void setCallback(Callbacks::RebalanceCallback callback);
    
    /**
     * @brief Get the rebalance callback.
     * @return The callback.
     */
    const Callbacks::RebalanceCallback& getRebalanceCallback() const;
    
    /**
     * @brief Set the preprocessor callback. This will be called before a message is deserialized.
     * @param callback The callback.
     * @note The callback should return 'true' if the message should be skipped.
     */
    void setCallback(Callbacks::PreprocessorCallback callback);
    
    /**
     * @brief Get the preprocessor callback.
     * @return The callback.
     */
    const Callbacks::PreprocessorCallback& getPreprocessorCallback() const;
    
    /**
     * @brief Set the receiver callback.
     * @tparam K The Key type.
     * @tparam P The Payload type.
     * @param callback The callback.
     * @remark Setting a receiver callback is mandatory.
     */
    template <typename K, typename P>
    void setCallback(Callbacks::ReceiverCallback<K,P> callback)
    {
        _receiver.reset(new ConcreteReceiver<K,P>(std::move(callback)));
    }
    
    /**
     * @brief Get the receiver callback.
     * @tparam K The Key type.
     * @tparam P The Payload type.
     * @return The callback.
     */
    template <typename K, typename P>
    const Callbacks::ReceiverCallback<K,P>& getReceiverCallback() const
    {
        return std::static_pointer_cast<ConcreteReceiver<K,P>>(_receiver)->getCallback();
    }
    
    /**
     * @brief Get the Receiver functor.
     * @return The Receiver.
     */
    const Receiver& getReceiver() const;
    
    /**
     * @brief Set the key deserializer callback.
     * @tparam T The Key type.
     * @param callback The callback.
     * @remark Setting a key deserializer callback is mandatory.
     */
    template <typename T>
    void setKeyCallback(Callbacks::DeserializerCallback<T> callback)
    {
        _keyDeserializer.reset(new ConcreteDeserializer<T>(std::move(callback)));
    }
    
    /**
     * @brief Set the payload deserializer callback.
     * @tparam T The Payload type.
     * @param callback The callback.
     * @remark Setting a payload deserializer callback is mandatory.
     */
    template <typename T>
    void setPayloadCallback(Callbacks::DeserializerCallback<T> callback)
    {
        _payloadDeserializer.reset(new ConcreteDeserializer<T>(std::move(callback)));
    }
    
    /**
     * @brief Set the payload deserializer callback.
     * @tparam T The Payload type.
     * @param name The header name.
     * @param callback The callback.
     * @remark Setting a payload deserializer callback is mandatory.
     */
    template <typename T>
    void setHeaderCallback(const std::string& name, Callbacks::DeserializerCallback<T> callback)
    {
        _headerDeserializers[name].reset(new ConcreteDeserializer<T>(std::move(callback)));
    }
    
    /**
     * @brief Get the key deserializer callback.
     * @tparam T The Key type.
     * @return The callback.
     */
    template <typename T>
    const Callbacks::DeserializerCallback<T>& getKeyCallback() const
    {
        return std::static_pointer_cast<ConcreteDeserializer<T>>(_keyDeserializer)->getCallback();
    }
    
    /**
     * @brief Get the payload deserializer callback.
     * @tparam T The Key type.
     * @return The callback.
     */
    template <typename T>
    const Callbacks::DeserializerCallback<T>& getPayloadCallback() const
    {
        return std::static_pointer_cast<ConcreteDeserializer<T>>(_payloadDeserializer)->getCallback();
    }
    
    /**
     * @brief Get the specific header deserializer callback.
     * @tparam T The Header type.
     * @return The callback.
     */
    template <typename T>
    const Callbacks::DeserializerCallback<T>& getHeaderCallback(const std::string& name) const
    {
        return std::static_pointer_cast<ConcreteDeserializer<T>>(_headerDeserializers.at(name))->getCallback();
    }
    
    /**
     * @brief Get the Deserializer functors.
     * @return The functor.
     */
    const Deserializer& getKeyDeserializer() const;
    const Deserializer& getPayloadDeserializer() const;
    const Deserializer& getHeaderDeserializer(const std::string& name) const;
    
private:
    using DeserializerPtr = std::shared_ptr<Deserializer>;
    
    Callbacks::OffsetCommitCallback         _offsetCommitCallback;
    Callbacks::RebalanceCallback            _rebalanceCallback;
    Callbacks::PreprocessorCallback         _preprocessorCallback;
    DeserializerPtr                         _keyDeserializer;
    DeserializerPtr                         _payloadDeserializer;
    std::map<std::string, DeserializerPtr>  _headerDeserializers;
    std::shared_ptr<Receiver>               _receiver;
    TopicPartitionList                      _initialPartitionList;
    PartitionStrategy                       _strategy{PartitionStrategy::Dynamic};
    static const OptionSet                  s_internalOptions;
    static const OptionSet                  s_internalTopicOptions;
    static const std::string                s_internalOptionsPrefix;
};

}
}


#endif //BLOOMBERG_COROKAFKA_CONSUMER_CONFIGURATION_H
