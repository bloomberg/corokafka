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
#include <corokafka/impl/corokafka_consumer_manager_impl.h>
#include <corokafka/corokafka_exception.h>
#include <cmath>
#include <tuple>
#include <algorithm>

using namespace std::placeholders;

namespace Bloomberg {
namespace corokafka {

//===========================================================================================
//                               class ConsumerManagerImpl
//===========================================================================================
ConsumerManagerImpl::ConsumerManagerImpl(quantum::Dispatcher& dispatcher,
                                         const ConnectorConfiguration& connectorConfiguration,
                                         const ConfigMap& configs) :
    _dispatcher(dispatcher),
    _shutdownIoWaitTimeoutMs(connectorConfiguration.getShutdownIoWaitTimeout())
{
    //Create a consumer for each topic and apply the appropriate configuration
    for (const auto& entry : configs) {
        // Process each configuration
        auto it = _consumers.emplace(entry.first, ConsumerTopicEntry(nullptr,
                                                                     connectorConfiguration,
                                                                     entry.second,
                                                                     dispatcher.getNumIoThreads(),
                                                                     dispatcher.getCoroQueueIdRangeForAny()));
        try {
            setup(entry.first, it.first->second);
        }
        catch (const cppkafka::ConfigException& ex) {
            throw InvalidOptionException(entry.first, "RdKafka", ex.what());
        }
        catch (const cppkafka::Exception& ex) {
            throw TopicException(entry.first, ex.what());
        }
    }
}

ConsumerManagerImpl::ConsumerManagerImpl(quantum::Dispatcher& dispatcher,
                                         const ConnectorConfiguration& connectorConfiguration,
                                         ConfigMap&& configs) :
    _dispatcher(dispatcher),
    _shutdownIoWaitTimeoutMs(connectorConfiguration.getShutdownIoWaitTimeout())
{
    //Create a consumer for each topic and apply the appropriate configuration
    for (auto&& entry : configs) {
        // Process each configuration
        auto it = _consumers.emplace(entry.first, ConsumerTopicEntry(nullptr,
                                                                     connectorConfiguration,
                                                                     std::move(entry.second),
                                                                     dispatcher.getNumIoThreads(),
                                                                     dispatcher.getCoroQueueIdRangeForAny()));
        try {
            setup(entry.first, it.first->second);
        }
        catch (const cppkafka::ConfigException& ex) {
            throw InvalidOptionException(entry.first, "RdKafka", ex.what());
        }
        catch (const std::exception& ex) {
            throw TopicException(entry.first, ex.what());
        }
    }
}

ConsumerManagerImpl::~ConsumerManagerImpl()
{
    shutdown();
}

void ConsumerManagerImpl::setup(const std::string& topic, ConsumerTopicEntry& topicEntry)
{
    const Configuration::OptionList &rdKafkaOptions = topicEntry._configuration
                                                                .getOptions(Configuration::OptionType::RdKafka);
    const Configuration::OptionList &rdKafkaTopicOptions = topicEntry._configuration
                                                                     .getTopicOptions(Configuration::OptionType::RdKafka);
    const Configuration::OptionList &internalOptions = topicEntry._configuration
                                                                 .getOptions(Configuration::OptionType::Internal);
    
    auto extract = [&topic, &internalOptions](const std::string &name, auto &value) -> bool
    {
        return ConsumerConfiguration::extract(name)(topic, Configuration::findOption(name, internalOptions), &value);
    };
    
    //Validate config
    std::string brokerList;
    if (!topicEntry._configuration.getOption(Configuration::RdKafkaOptions::metadataBrokerList)) {
        throw InvalidOptionException(topic, Configuration::RdKafkaOptions::metadataBrokerList, "Missing");
    }
    
    if (!topicEntry._configuration.getOption(Configuration::RdKafkaOptions::groupId)) {
        throw InvalidOptionException(topic, Configuration::RdKafkaOptions::groupId, "Missing");
    }
    
    //Check if the receiver is set (will throw if not set)
    topicEntry._configuration.getTypeErasedReceiver();
    
    //Set the rdkafka configuration options
    cppkafka::Configuration kafkaConfig(rdKafkaOptions);
    kafkaConfig.set_default_topic_configuration(cppkafka::TopicConfiguration(rdKafkaTopicOptions));
    
    extract(ConsumerConfiguration::Options::autoThrottle, topicEntry._throttleControl.autoThrottle());
    extract(ConsumerConfiguration::Options::autoThrottleMultiplier, topicEntry._throttleControl.throttleMultiplier());
    
    //Set the global callbacks
    if (topicEntry._configuration.getErrorCallback()) {
        auto errorFunc = std::bind(errorCallback2, std::ref(topicEntry), _1, _2, _3);
        kafkaConfig.set_error_callback(std::move(errorFunc));
    }
    
    if (topicEntry._configuration.getThrottleCallback() || topicEntry._throttleControl.autoThrottle()) {
        auto throttleFunc = std::bind(throttleCallback, std::ref(topicEntry), _1, _2, _3, _4);
        kafkaConfig.set_throttle_callback(std::move(throttleFunc));
    }
    
    if (topicEntry._configuration.getLogCallback()) {
        auto logFunc = std::bind(logCallback, std::ref(topicEntry), _1, _2, _3, _4);
        kafkaConfig.set_log_callback(std::move(logFunc));
    }
    
    if (topicEntry._configuration.getStatsCallback()) {
        auto statsFunc = std::bind(statsCallback, std::ref(topicEntry), _1, _2);
        kafkaConfig.set_stats_callback(std::move(statsFunc));
    }
    
    if (topicEntry._configuration.getOffsetCommitCallback()) {
        auto offsetCommitFunc = std::bind(offsetCommitCallback, std::ref(topicEntry), _1, _2, _3);
        kafkaConfig.set_offset_commit_callback(std::move(offsetCommitFunc));
    }
    
    if (topicEntry._configuration.getPreprocessorCallback()) {
        topicEntry._preprocessorCallback = std::bind(preprocessorCallback, std::ref(topicEntry), _1);
    }
    
    extract(ConsumerConfiguration::Options::autoOffsetPersist, topicEntry._autoOffsetPersist);
    
    const cppkafka::ConfigurationOption* persistStrategy =
        Configuration::findOption(ConsumerConfiguration::Options::offsetPersistStrategy, internalOptions);
    if (persistStrategy) {
        if (StringEqualCompare()(persistStrategy->get_value(), "commit")) {
            topicEntry._autoOffsetPersistStrategy = OffsetPersistStrategy::Commit;
        }
        else if (StringEqualCompare()(persistStrategy->get_value(), "store")) {
#if (RD_KAFKA_VERSION < RD_KAFKA_STORE_OFFSETS_SUPPORT_VERSION)
            std::ostringstream oss;
            oss << std::hex << "Current RdKafka version " << RD_KAFKA_VERSION
                << " does not support this functionality. Must be greater than "
                << RD_KAFKA_STORE_OFFSETS_SUPPORT_VERSION;
            throw FeatureNotSupportedException(topic, ConsumerConfiguration::Options::offsetPersistStrategy, oss.str());
#else
            topicEntry._autoOffsetPersistStrategy = OffsetPersistStrategy::Store;
#endif
        }
        else {
            throw InvalidOptionException(topic, ConsumerConfiguration::Options::offsetPersistStrategy, persistStrategy->get_value());
        }
    }
    
    extract(ConsumerConfiguration::Options::autoOffsetPersistOnException, topicEntry._autoOffsetPersistOnException);
    
    extract(ConsumerConfiguration::Options::commitExec, topicEntry._autoCommitExec);
    
    // Set underlying rdkafka options
    if (topicEntry._autoOffsetPersist) {
        kafkaConfig.set(Configuration::RdKafkaOptions::enableAutoOffsetStore, false);
        if (topicEntry._autoOffsetPersistStrategy == OffsetPersistStrategy::Commit) {
            kafkaConfig.set(Configuration::RdKafkaOptions::enableAutoCommit, false);
            kafkaConfig.set(Configuration::RdKafkaOptions::autoCommitIntervalMs, 0);
        }
        else {
            kafkaConfig.set(Configuration::RdKafkaOptions::enableAutoCommit, true);
        }
    }
    
    //=======================================================================================
    //DO NOT UPDATE ANY KAFKA CONFIG OPTIONS BELOW THIS POINT SINCE THE CONSUMER MAKES A COPY
    //=======================================================================================
    
    //Create a consumer
    topicEntry._consumer.reset(new cppkafka::Consumer(kafkaConfig));
    topicEntry._committer.reset(new cppkafka::BackoffCommitter(*topicEntry._consumer));
    
    //Get events queue for polling
    topicEntry._eventQueue = topicEntry._consumer->get_main_queue();
    
    auto offsetCommitErrorFunc = std::bind(&offsetCommitErrorCallback, std::ref(topicEntry), _1);
    topicEntry._committer->set_error_callback(offsetCommitErrorFunc);
    
    //Set internal config options
    extract(ConsumerConfiguration::Options::skipUnknownHeaders, topicEntry._skipUnknownHeaders);
    
    if (extract(ConsumerConfiguration::Options::logLevel, topicEntry._logLevel)) {
        topicEntry._consumer->set_log_level(topicEntry._logLevel);
    }
    
    size_t commitNumRetries;
    if (extract(ConsumerConfiguration::Options::commitNumRetries, commitNumRetries)) {
        topicEntry._committer->set_maximum_retries(commitNumRetries);
    }
    
    cppkafka::BackoffPerformer::BackoffPolicy backoffStrategy;
    if (extract(ConsumerConfiguration::Options::commitBackoffStrategy, backoffStrategy)) {
        topicEntry._committer->set_backoff_policy(backoffStrategy);
    }
    
    std::chrono::milliseconds commitBackoffStep{100};
    if (extract(ConsumerConfiguration::Options::commitBackoffIntervalMs, commitBackoffStep)) {
        topicEntry._committer->set_initial_backoff(commitBackoffStep);
        topicEntry._committer->set_backoff_step(commitBackoffStep);
    }
    
    std::chrono::milliseconds commitMaxBackoff;
    if (extract(ConsumerConfiguration::Options::commitMaxBackoffMs, commitMaxBackoff)) {
        if (commitMaxBackoff < commitBackoffStep) {
            throw InvalidOptionException(topic, ConsumerConfiguration::Options::commitMaxBackoffMs, std::to_string(commitMaxBackoff.count()));
        }
    }
    
    extract(ConsumerConfiguration::Options::readSize, topicEntry._readSize);
    if (topicEntry._readSize <= 0) {
        throw InvalidOptionException(topic, ConsumerConfiguration::Options::readSize, "Read size must be > 0");
    }
    
    std::pair<int, int> prev = topicEntry._receiveCallbackThreadRange;
    extract(ConsumerConfiguration::Options::receiveCallbackThreadRangeLow, topicEntry._receiveCallbackThreadRange.first);
    if ((topicEntry._receiveCallbackThreadRange.first < prev.first) ||
        (topicEntry._receiveCallbackThreadRange.first > prev.second)) {
        throw InvalidOptionException(topic, ConsumerConfiguration::Options::receiveCallbackThreadRangeLow, std::to_string(topicEntry._receiveCallbackThreadRange.first));
    }
    extract(ConsumerConfiguration::Options::receiveCallbackThreadRangeHigh, topicEntry._receiveCallbackThreadRange.second);
    if ((topicEntry._receiveCallbackThreadRange.second < topicEntry._receiveCallbackThreadRange.first) ||
        (topicEntry._receiveCallbackThreadRange.first > prev.second)) {
        throw InvalidOptionException(topic, ConsumerConfiguration::Options::receiveCallbackThreadRangeHigh, std::to_string(topicEntry._receiveCallbackThreadRange.second));
    }
    
    extract(ConsumerConfiguration::Options::receiveCallbackExec, topicEntry._receiveCallbackExec);
    
    extract(ConsumerConfiguration::Options::receiveInvokeThread, topicEntry._receiverThread);

    extract(ConsumerConfiguration::Options::batchPrefetch, topicEntry._batchPrefetch);
    
    extract(ConsumerConfiguration::Options::preprocessMessages, topicEntry._preprocess);
    
    extract(ConsumerConfiguration::Options::preserveMessageOrder, topicEntry._preserveMessageOrder);
    
    extract(ConsumerConfiguration::Options::pollIoThreadId, topicEntry._pollIoThreadId);
    if ((int)topicEntry._pollIoThreadId >= _dispatcher.getNumIoThreads()) {
        throw InvalidOptionException(topic, ConsumerConfiguration::Options::pollIoThreadId, "Value is out of bounds");
    }
    
    extract(ConsumerConfiguration::Options::processCoroThreadId, topicEntry._processCoroThreadId);
    if ((int)topicEntry._processCoroThreadId >= _dispatcher.getNumCoroutineThreads()) {
        throw InvalidOptionException(topic, ConsumerConfiguration::Options::processCoroThreadId, "Value is out of bounds");
    }
    
    // Set the consumer callbacks
    if (topicEntry._configuration.getRebalanceCallback() ||
        ((topicEntry._configuration.getPartitionStrategy() == PartitionStrategy::Dynamic) &&
         !topicEntry._configuration.getInitialPartitionAssignment().empty())) {
        auto assignmentFunc = std::bind(&ConsumerManagerImpl::assignmentCallback, std::ref(topicEntry), _1);
        topicEntry._consumer->set_assignment_callback(std::move(assignmentFunc));
    }
    if (topicEntry._configuration.getRebalanceCallback()) {
        auto revocationFunc = std::bind(&ConsumerManagerImpl::revocationCallback, std::ref(topicEntry), _1);
        topicEntry._consumer->set_revocation_callback(std::move(revocationFunc));
        
        auto rebalanceErrorFunc = std::bind(&ConsumerManagerImpl::rebalanceErrorCallback, std::ref(topicEntry), _1);
        topicEntry._consumer->set_rebalance_error_callback(std::move(rebalanceErrorFunc));
    }
    
    if (extract(ConsumerConfiguration::Options::pauseOnStart, topicEntry._isPaused)) {
        if (topicEntry._isPaused) {
            topicEntry._consumer->pause(topic);
        }
    }
    
    extract(ConsumerConfiguration::Options::pollStrategy, topicEntry._pollStrategy);
    if (topicEntry._pollStrategy == PollStrategy::RoundRobin) {
        topicEntry._roundRobinStrategy.reset(new cppkafka::RoundRobinPollStrategy(*topicEntry._consumer));
    }
    
    //dynamically subscribe or statically assign partitions to this consumer
    subscribe(topic, topicEntry, topicEntry._configuration.getInitialPartitionAssignment());
    
    //Set the consumer timeout.
    //NOTE: This setting must be done after assigning partitions and pausing the consumer (see above)
    //since these operations require a longer than usual timeout. If the user sets a small timeout
    //period, partition assignment might fail.
    std::chrono::milliseconds timeout;
    if (extract(ConsumerConfiguration::Options::timeoutMs, timeout)) {
        topicEntry._consumer->set_timeout(timeout);
    }
    
    extract(ConsumerConfiguration::Options::pollTimeoutMs, topicEntry._pollTimeout);
    if (topicEntry._pollTimeout.count() == (int)TimerValues::Disabled) {
        topicEntry._pollTimeout = topicEntry._consumer->get_timeout();
    }
    
    extract(ConsumerConfiguration::Options::minRoundRobinPollTimeoutMs, topicEntry._minRoundRobinPollTimeout);
}

ConsumerMetadata ConsumerManagerImpl::getMetadata(const std::string& topic)
{
    auto it = findConsumer(topic);
    return makeMetadata(it->second);
}

void ConsumerManagerImpl::preprocess(bool enable, const std::string& topic)
{
    if (topic.empty()) {
        for (auto&& consumer : _consumers) {
            consumer.second._preprocess = enable;
        }
    }
    else {
        findConsumer(topic)->second._preprocess = enable;
    }
}

void ConsumerManagerImpl::pause(const std::string& topic)
{
    if (topic.empty()) {
        for (auto&& consumer : _consumers) {
            ConsumerTopicEntry& topicEntry = consumer.second;
            bool paused = false;
            if (topicEntry._isPaused.compare_exchange_strong(paused, !paused)) {
                topicEntry._consumer->pause();
            }
        }
    }
    else {
        bool paused = false;
        ConsumerTopicEntry& topicEntry = findConsumer(topic)->second;
        if (topicEntry._isPaused.compare_exchange_strong(paused, !paused)) {
            topicEntry._consumer->pause();
        }
    }
}

void ConsumerManagerImpl::resume(const std::string& topic)
{
    if (topic.empty()) {
        for (auto&& consumer : _consumers) {
            ConsumerTopicEntry& topicEntry = consumer.second;
            bool paused = true;
            if (topicEntry._isPaused.compare_exchange_strong(paused, !paused)) {
                topicEntry._consumer->resume();
            }
        }
    }
    else {
        bool paused = true;
        ConsumerTopicEntry& topicEntry = findConsumer(topic)->second;
        if (topicEntry._isPaused.compare_exchange_strong(paused, !paused)) {
            topicEntry._consumer->resume();
        }
    }
}

void ConsumerManagerImpl::subscribe(const std::string& topic,
                                    ConsumerTopicEntry& topicEntry,
                                    const cppkafka::TopicPartitionList& partitionList)
{
    if (topicEntry._isSubscribed) {
        throw ConsumerException(topic, "Already subscribed");
    }
    //process partitions and make sure offsets are valid
    cppkafka::TopicPartitionList assignment = partitionList;
    if ((assignment.size() == 1) &&
        (assignment.front().get_partition() == RD_KAFKA_PARTITION_UA)) {
        //Overwrite the initial assignment if the user provided no partitions
        cppkafka::TopicMetadata metadata = topicEntry._consumer->get_metadata(topicEntry._consumer->get_topic(topic));
        int offset = assignment.front().get_offset();
        assignment = cppkafka::convert(topic, metadata.get_partitions());
        //set the specified offset on all existing partitions
        for (auto& partition : assignment) {
            partition.set_offset(offset);
        }
    }
    if (assignment.empty()) {
        //use updated current assignment
        assignment = topicEntry._partitionAssignment;
    }
    //assign or subscribe
    if (topicEntry._configuration.getPartitionStrategy() == PartitionStrategy::Static) {
        if (assignment.empty()) {
            //we must have valid partitions
            throw ConsumerException(topic, "Empty partition assignment");
        }
        if (topicEntry._pollStrategy == PollStrategy::RoundRobin) {
            topicEntry._roundRobinStrategy->assign(assignment);
        }
        //invoke the assignment callback manually
        ConsumerManagerImpl::assignmentCallback(topicEntry, assignment);
        topicEntry._consumer->assign(assignment);
    }
    else { //Dynamic strategy
        if (!assignment.empty()) {
            topicEntry._setOffsetsOnStart = true;
            //overwrite original if any so that offsets may be used when the assignment callback is invoked
            topicEntry._partitionAssignment = assignment;
        }
        topicEntry._consumer->subscribe({topic});
    }
}

void ConsumerManagerImpl::subscribe(const std::string& topic,
                                    const cppkafka::TopicPartitionList& partitionList)
{
    if (topic.empty()) {
        for (auto&& consumer : _consumers) {
            if (!consumer.second._isSubscribed) {
                subscribe(consumer.first, consumer.second, partitionList);
            }
        }
    }
    else {
        ConsumerTopicEntry& topicEntry = findConsumer(topic)->second;
        if (!topicEntry._isSubscribed) {
            subscribe(topic, topicEntry, partitionList);
        }
        else {
            throw ConsumerException(topic, "Consumer already subscribed");
        }
    }
}

void ConsumerManagerImpl::unsubscribe(const std::string& topic)
{
    if (topic.empty()) {
        for (auto&& consumer : _consumers) {
            ConsumerTopicEntry& topicEntry = consumer.second;
            if (topicEntry._isSubscribed) {
                if (topicEntry._configuration.getPartitionStrategy() == PartitionStrategy::Static) {
                    if (topicEntry._pollStrategy == PollStrategy::RoundRobin) {
                        topicEntry._roundRobinStrategy->revoke();
                    }
                    //invoke the revocation callback manually
                    ConsumerManagerImpl::revocationCallback(topicEntry, consumer.second._consumer->get_assignment());
                    topicEntry._consumer->unassign();
                }
                else {
                    topicEntry._consumer->unsubscribe();
                }
            }
        }
    }
    else {
        ConsumerTopicEntry& topicEntry = findConsumer(topic)->second;
        if (topicEntry._isSubscribed) {
            if (topicEntry._configuration.getPartitionStrategy() == PartitionStrategy::Static) {
                if (topicEntry._pollStrategy == PollStrategy::RoundRobin) {
                    topicEntry._roundRobinStrategy->revoke();
                }
                //invoke the revocation callback manually
                ConsumerManagerImpl::revocationCallback(topicEntry, topicEntry._consumer->get_assignment());
                topicEntry._consumer->unassign();
            }
            else {
                topicEntry._consumer->unsubscribe();
            }
        }
    }
}

cppkafka::Error ConsumerManagerImpl::commit(const cppkafka::TopicPartition& topicPartition,
                                            const void* opaque,
                                            bool forceSync)
{
    return commit(cppkafka::TopicPartitionList{topicPartition}, opaque, forceSync);
}

cppkafka::Error ConsumerManagerImpl::commit(const cppkafka::TopicPartitionList& topicPartitions,
                                            const void* opaque,
                                            bool forceSync)
{
    if (topicPartitions.empty()) {
        return RD_KAFKA_RESP_ERR_INVALID_PARTITIONS;
    }
    auto it = _consumers.find(topicPartitions.at(0).get_topic());
    if (it == _consumers.end()) {
        return RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART;
    }
    return commitImpl(it->second, topicPartitions, opaque, forceSync);
}

cppkafka::Error ConsumerManagerImpl::commitImpl(ConsumerTopicEntry& entry,
                                                const cppkafka::TopicPartitionList& topicPartitions,
                                                const void* opaque,
                                                bool forceSync)
{
    try {
        const cppkafka::TopicPartition& headPartition = topicPartitions.at(0);
        if (entry._committer->get_consumer().get_configuration().get_offset_commit_callback() && (opaque != nullptr)) {
            entry._offsets.insert(headPartition, opaque);
        }
        if (entry._autoOffsetPersistStrategy == OffsetPersistStrategy::Commit || forceSync) {
            if ((entry._autoCommitExec == ExecMode::Sync) || forceSync) {
                auto ctx = quantum::local::context();
                if (headPartition.get_partition() == RD_KAFKA_PARTITION_UA) {
                    //commit the current assignment
                    if (ctx) {
                        ctx->postAsyncIo([&entry](IoTracker)->int {
                            entry._committer->commit();
                            return 0;
                        }, IoTracker(entry._ioTracker))->get(ctx);
                    }
                    else {
                        entry._committer->commit();
                    }
                }
                else {
                    if (ctx) {
                        ctx->postAsyncIo([&entry, &topicPartitions](IoTracker)->int {
                            entry._committer->commit(topicPartitions);
                            return 0;
                        }, IoTracker(entry._ioTracker))->get(ctx);
                    }
                    else {
                        entry._committer->commit(topicPartitions);
                    }
                }
            }
            else { // async
                if (headPartition.get_partition() == RD_KAFKA_PARTITION_UA) {
                    //commit the current assignment
                    entry._committer->get_consumer().async_commit();
                }
                else {
                    entry._committer->get_consumer().async_commit(topicPartitions);
                }
            }
        }
        else { //OffsetPersistStrategy::Store
            entry._committer->get_consumer().store_offsets(topicPartitions);
        }
    }
    catch (const cppkafka::HandleException& ex) {
        return ex.get_error();
    }
    catch (const cppkafka::ActionTerminatedException& ex) {
        return RD_KAFKA_RESP_ERR__FAIL; //no more retries left
    }
    catch (...) {
        return RD_KAFKA_RESP_ERR_UNKNOWN;
    }
    return {};
}

const ConsumerConfiguration& ConsumerManagerImpl::getConfiguration(const std::string& topic) const
{
    auto it = findConsumer(topic);
    return it->second._configuration;
}

std::vector<std::string> ConsumerManagerImpl::getTopics() const
{
    std::vector<std::string> topics;
    topics.reserve(_consumers.size());
    for (const auto& entry : _consumers) {
        topics.emplace_back(entry.first);
    }
    return topics;
}

void ConsumerManagerImpl::shutdown()
{
    if (!_shutdownInitiated.test_and_set()) {
        _shuttingDown = true;
        //unsubscribe all the consumers
        unsubscribe({});
        //see if we have any IO left
        auto sleepInterval = _shutdownIoWaitTimeoutMs/10;
        while (_shutdownIoWaitTimeoutMs.count() > 0) {
            bool hasIo = false;
            for (auto &&entry : _consumers) {
                if (entry.second._ioTracker.use_count() > 1) {
                    hasIo = true;
                    break;
                }
            }
            if (!hasIo) break;
            //wait and decrement
            std::this_thread::sleep_for(sleepInterval);
            _shutdownIoWaitTimeoutMs -= sleepInterval;
        }
    }
}

void ConsumerManagerImpl::poll()
{
    auto now = std::chrono::steady_clock::now();
    for (auto&& entry : _consumers) {
        if (_shuttingDown) {
            entry.second._shuttingDown = true;
            continue;
        }
        // Adjust throttling if necessary
        adjustThrottling(entry.second, now);
        
        // Poll for events
        entry.second._eventQueue.consume(std::chrono::milliseconds::zero());
        
        // Poll for messages
        bool pollMessages = !entry.second._pollFuture ||
                            (entry.second._pollFuture->waitFor(std::chrono::milliseconds::zero()) == std::future_status::ready);
        if (pollMessages) {
            // Check if we have any new messages
            if (hasNewMessages(entry.second)) {
                if (entry.second._pollStrategy == PollStrategy::Batch) {
                    entry.second._pollFuture =
                      _dispatcher.post((int)entry.second._processCoroThreadId,
                                       true,
                                       pollBatchCoro,
                                       entry.second,
                                       IoTracker(entry.second._ioTracker));
                }
                else {
                    //Round-robin or serial
                    entry.second._pollFuture =
                        _dispatcher.post((int)entry.second._processCoroThreadId,
                                         true,
                                         pollCoro,
                                         entry.second,
                                         IoTracker(entry.second._ioTracker));
                }
            }
        }
    }
}

void ConsumerManagerImpl::pollEnd()
{
    for (auto&& entry : _consumers) {
        if (entry.second._pollFuture) {
            //wait until poll task has completed
            entry.second._pollFuture->get();
        }
    }
}

void ConsumerManagerImpl::errorCallback2(
                        ConsumerTopicEntry& topicEntry,
                        cppkafka::KafkaHandleBase& handle,
                        int error,
                        const std::string& reason)
{
    errorCallback(topicEntry, handle, (rd_kafka_resp_err_t)error, reason, nullptr);
}

void ConsumerManagerImpl::errorCallback(
                        ConsumerTopicEntry& topicEntry,
                        cppkafka::KafkaHandleBase& handle,
                        cppkafka::Error error,
                        const std::string& reason,
                        const cppkafka::Message* message)
{
    const void* errorOpaque = topicEntry._configuration.getErrorCallbackOpaque() ?
                              topicEntry._configuration.getErrorCallbackOpaque() : message;
    cppkafka::CallbackInvoker<Callbacks::ErrorCallback>
        ("error", topicEntry._configuration.getErrorCallback(), &handle)
            (makeMetadata(topicEntry), error, reason, const_cast<void*>(errorOpaque));
}

void ConsumerManagerImpl::throttleCallback(
                        ConsumerTopicEntry& topicEntry,
                        cppkafka::KafkaHandleBase& handle,
                        const std::string& brokerName,
                        int32_t brokerId,
                        std::chrono::milliseconds throttleDuration)
{
    if (!topicEntry._isPaused) {
        //consumer is not explicitly paused by the application.
        cppkafka::Consumer& consumer = static_cast<cppkafka::Consumer&>(handle);
        //calculate throttling status
        ThrottleControl::Status status = topicEntry._throttleControl.handleThrottleCallback(throttleDuration);
        if (status == ThrottleControl::Status::On) {
            consumer.pause();
        }
        else if (status == ThrottleControl::Status::Off) {
            consumer.resume();
        }
    }
    cppkafka::CallbackInvoker<Callbacks::ThrottleCallback>
        ("throttle", topicEntry._configuration.getThrottleCallback(), &handle)
            (makeMetadata(topicEntry), brokerName, brokerId, throttleDuration);
}

void ConsumerManagerImpl::logCallback(
                        ConsumerTopicEntry& topicEntry,
                        cppkafka::KafkaHandleBase& handle,
                        int level,
                        const std::string& facility,
                        const std::string& message)
{
    cppkafka::CallbackInvoker<Callbacks::LogCallback>
        ("log", topicEntry._configuration.getLogCallback(), &handle)
            (makeMetadata(topicEntry), static_cast<cppkafka::LogLevel>(level), facility, message);
}

void ConsumerManagerImpl::statsCallback(
                        ConsumerTopicEntry& topicEntry,
                        cppkafka::KafkaHandleBase& handle,
                        const std::string& json)
{
    cppkafka::CallbackInvoker<Callbacks::StatsCallback>
        ("stats", topicEntry._configuration.getStatsCallback(), &handle)
            (makeMetadata(topicEntry), json);
}

void ConsumerManagerImpl::offsetCommitCallback(
                        ConsumerTopicEntry& topicEntry,
                        cppkafka::Consumer& consumer,
                        cppkafka::Error error,
                        const cppkafka::TopicPartitionList& topicPartitions)
{
    // Check if we have opaque data
    std::vector<void*> opaques;
    if (!topicEntry._offsets.empty()) {
        opaques.reserve(topicPartitions.size());
    }
    for (auto& partition : topicPartitions) {
        //remove the opaque values and pass them back to the application
        if (!topicEntry._offsets.empty()) {
            opaques.push_back(const_cast<void*>(topicEntry._offsets.remove(partition)));
        }
    }
    cppkafka::CallbackInvoker<Callbacks::OffsetCommitCallback>
        ("offset commit", topicEntry._configuration.getOffsetCommitCallback(), &consumer)
            (makeMetadata(topicEntry), error, topicPartitions, opaques);
}

bool ConsumerManagerImpl::offsetCommitErrorCallback(
                        ConsumerTopicEntry& topicEntry,
                        cppkafka::Error error)
{
    report(topicEntry, cppkafka::LogLevel::LogErr, error, "Failed to commit offset.", nullptr);
    return ((error.get_error() != RD_KAFKA_RESP_ERR_OFFSET_OUT_OF_RANGE) &&
            (error.get_error() != RD_KAFKA_RESP_ERR_OFFSET_METADATA_TOO_LARGE) &&
            (error.get_error() != RD_KAFKA_RESP_ERR_INVALID_COMMIT_OFFSET_SIZE));
}

bool ConsumerManagerImpl::preprocessorCallback(
                        ConsumerTopicEntry& topicEntry,
                        const cppkafka::Message& rawMessage)
{
    // Check if we have opaque data
    return cppkafka::CallbackInvoker<Callbacks::PreprocessorCallback>
        ("preprocessor", topicEntry._configuration.getPreprocessorCallback(), topicEntry._consumer.get())
            (rawMessage);
}

void ConsumerManagerImpl::assignmentCallback(
                        ConsumerTopicEntry& topicEntry,
                        cppkafka::TopicPartitionList& topicPartitions)
{
    // Clear any throttling we may have
    topicEntry._isSubscribed = true;
    topicEntry._throttleControl.reset();
    PartitionStrategy strategy = topicEntry._configuration.getPartitionStrategy();
    if ((strategy == PartitionStrategy::Dynamic) &&
        !topicEntry._partitionAssignment.empty() &&
        topicEntry._setOffsetsOnStart) {
        topicEntry._setOffsetsOnStart = false;
        //perform first offset assignment based on user config
        for (auto&& partition : topicPartitions) {
            for (const auto& assigned : topicEntry._configuration.getInitialPartitionAssignment()) {
                if (partition.get_partition() == assigned.get_partition()) {
                    //we have a match
                    partition.set_offset(assigned.get_offset());
                    break;
                }
            }
        }
    }
    topicEntry._partitionAssignment = topicPartitions; //overwrite original if any
    cppkafka::CallbackInvoker<Callbacks::RebalanceCallback>
        ("assignment", topicEntry._configuration.getRebalanceCallback(), topicEntry._consumer.get())
            (makeMetadata(topicEntry), cppkafka::Error(RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS), topicPartitions);
}

void ConsumerManagerImpl::revocationCallback(
                        ConsumerTopicEntry& topicEntry,
                        const cppkafka::TopicPartitionList& topicPartitions)
{
    topicEntry._isSubscribed = false;
    PartitionStrategy strategy = topicEntry._configuration.getPartitionStrategy();
    topicEntry._partitionAssignment.clear(); //clear assignment
    topicEntry._watermarks.clear(); //clear offset watermarks
    cppkafka::CallbackInvoker<Callbacks::RebalanceCallback>
        ("revocation", topicEntry._configuration.getRebalanceCallback(), topicEntry._consumer.get())
            (makeMetadata(topicEntry), cppkafka::Error(RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS), const_cast<cppkafka::TopicPartitionList&>(topicPartitions));
}

void ConsumerManagerImpl::rebalanceErrorCallback(
                        ConsumerTopicEntry& topicEntry,
                        cppkafka::Error error)
{
    topicEntry._isSubscribed = false;
    PartitionStrategy strategy = topicEntry._configuration.getPartitionStrategy();
    topicEntry._partitionAssignment.clear(); //clear assignment
    topicEntry._watermarks.clear(); //clear offset watermarks
    cppkafka::TopicPartitionList topicPartitions;
    cppkafka::CallbackInvoker<Callbacks::RebalanceCallback>
        ("rebalance", topicEntry._configuration.getRebalanceCallback(), topicEntry._consumer.get())
            (makeMetadata(topicEntry), error, topicPartitions);
}

void ConsumerManagerImpl::adjustThrottling(ConsumerTopicEntry& topicEntry,
                                           const std::chrono::steady_clock::time_point& now)
{
    if (!topicEntry._isPaused && topicEntry._throttleControl.reduceThrottling(now)) {
        // Resume only if this consumer is not paused explicitly by the user
        topicEntry._consumer->resume();
    }
}

void ConsumerManagerImpl::report(
                    ConsumerTopicEntry& topicEntry,
                    cppkafka::LogLevel level,
                    cppkafka::Error error,
                    const std::string& reason,
                    const cppkafka::Message* message)
{
    if (error) {
        errorCallback(topicEntry, *topicEntry._consumer, error, reason, message);
    }
    if (topicEntry._logLevel >= level) {
        logCallback(topicEntry, *topicEntry._consumer, (int)level, "corokafka", reason);
    }
}

MessageBatch ConsumerManagerImpl::messageBatchReceiveTask(ConsumerTopicEntry& entry, IoTracker)
{
    if (entry._shuttingDown) {
        return {};
    }
    try {
        return entry._consumer->poll_batch(entry._readSize, entry._pollTimeout);
    }
    catch (const std::exception& ex) {
        exceptionHandler(ex, entry);
        return {};
    }
}

int ConsumerManagerImpl::messageRoundRobinReceiveTask(quantum::ThreadPromise<MessageContainer>::Ptr promise,
                                                      ConsumerTopicEntry& entry,
                                                      IoTracker)
{
    try {
        int readSize = entry._readSize;
        std::chrono::milliseconds timeout = entry._pollTimeout;
        std::chrono::milliseconds timeoutPerRound = (timeout.count() == (int)TimerValues::Unlimited) ?
                                                    entry._minRoundRobinPollTimeout :
                                                    std::max(timeout/(int64_t)entry._readSize,
                                                             entry._minRoundRobinPollTimeout);
        auto endTime = std::chrono::steady_clock::now() + timeout;
        
        //Get messages until the batch is filled or until the timeout expires
        while (((entry._readSize == -1) || (readSize > 0)) &&
               ((timeout.count() == (int)TimerValues::Unlimited) || (std::chrono::steady_clock::now() < endTime))) {
            if (entry._shuttingDown) {
                //Exit early
                break;
            }
            cppkafka::Message message = entry._roundRobinStrategy->poll(std::chrono::milliseconds::zero());
            if (message) {
                --readSize;
                //We have a valid message
                promise->push(std::move(message));
            }
            else {
                std::this_thread::sleep_for(timeoutPerRound);
            }
        }
    }
    catch (const std::exception& ex) {
        exceptionHandler(ex, entry);
    }
    return promise->closeBuffer();
}

int ConsumerManagerImpl::messageSerialReceiveTask(quantum::ThreadPromise<MessageContainer>::Ptr promise,
                                                  ConsumerTopicEntry& entry,
                                                  IoTracker)
{
    try {
        int readSize = entry._readSize;
        auto endTime = std::chrono::steady_clock::now() + entry._pollTimeout;
        
        //Get messages until the batch is filled or until the timeout expires
        cppkafka::Message message;
        while ((entry._readSize == -1) || (readSize > 0)) {
            if (entry._shuttingDown) {
                //Exit early
                break;
            }
            //Do a non-blocking poll to see if there are any messages in the queue
            message = entry._consumer->poll(std::chrono::milliseconds::zero());
            if (!message) {
                //Do a blocking poll
                std::chrono::milliseconds remaining((int)TimerValues::Unlimited);
                if (entry._pollTimeout.count() != (int)TimerValues::Unlimited) {
                    //calculate the next timeout value
                    remaining = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - std::chrono::steady_clock::now());
                    if (remaining < std::chrono::milliseconds::zero()) {
                        //we have exceeded the poll time
                        break;
                    }
                }
                message = entry._consumer->poll(remaining);
            }
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

DeserializedMessage
ConsumerManagerImpl::deserializeMessage(ConsumerTopicEntry& entry,
                                        const cppkafka::Message& kafkaMessage)
{
    DeserializerError de;
    const TypeErasedDeserializer& deserializer = entry._configuration.getTypeErasedDeserializer();
    
    //Get the topic partition
    cppkafka::TopicPartition toppar(kafkaMessage.get_topic(), kafkaMessage.get_partition(), kafkaMessage.get_offset());
    
    //Deserialize the key
    boost::any key = cppkafka::CallbackInvoker<Deserializer>("key_deserializer",
                                                   *deserializer._keyDeserializer,
                                                   entry._consumer.get())
                     (toppar, kafkaMessage.get_key());
    if (key.empty()) {
        // Decoding failed
        de._error = RD_KAFKA_RESP_ERR__KEY_DESERIALIZATION;
        de._source |= (uint8_t)DeserializerError::Source::Key;
        report(entry, cppkafka::LogLevel::LogErr, RD_KAFKA_RESP_ERR__KEY_DESERIALIZATION, "Failed to deserialize key", &kafkaMessage);
    }
    
    //Deserialize the headers if any
    HeaderPack headers(deserializer._headerEntries.size());
    int num = 0;
    const cppkafka::HeaderList<cppkafka::Header<cppkafka::Buffer>>& kafkaHeaders = kafkaMessage.get_header_list();
    for (auto it = kafkaHeaders.begin(); it != kafkaHeaders.end(); ++it) {
        try {
            const TypeErasedDeserializer::HeaderEntry& headerEntry = deserializer._headerDeserializers.at(it->get_name());
            headers[headerEntry._pos].first = it->get_name();
            headers[headerEntry._pos].second = cppkafka::CallbackInvoker<Deserializer>("header_deserializer",
                                                                                       *headerEntry._deserializer,
                                                                                       entry._consumer.get())
                                                                         (toppar, it->get_value());
            if (headers[headerEntry._pos].second.empty()) {
                // Decoding failed
                de._error = RD_KAFKA_RESP_ERR__VALUE_DESERIALIZATION;
                de._source |= (uint8_t)DeserializerError::Source::Header;
                de._headerNum = num;
                std::ostringstream oss;
                oss << "Failed to deserialize header: " << it->get_name();
                report(entry, cppkafka::LogLevel::LogErr, RD_KAFKA_RESP_ERR__VALUE_DESERIALIZATION, oss.str(), &kafkaMessage);
                break;
            }
        }
        catch (const std::exception& ex) {
            if (entry._skipUnknownHeaders) {
                report(entry, cppkafka::LogLevel::LogWarning, {}, ex.what(), &kafkaMessage);
                continue;
            }
            de._error = RD_KAFKA_RESP_ERR__NOT_IMPLEMENTED;
            de._source |= (uint8_t)DeserializerError::Source::Header;
            de._headerNum = num;
            report(entry, cppkafka::LogLevel::LogErr, RD_KAFKA_RESP_ERR__NOT_IMPLEMENTED, ex.what(), &kafkaMessage);
            break;
        }
        ++num;
    }
    
    //Deserialize the payload
    boost::any payload = cppkafka::CallbackInvoker<Deserializer>("payload_deserializer",
                                                                 *deserializer._payloadDeserializer,
                                                                 entry._consumer.get())
                     (toppar, kafkaMessage.get_payload());
    if (payload.empty()) {
        // Decoding failed
        de._error = RD_KAFKA_RESP_ERR__VALUE_DESERIALIZATION;
        de._source |= (uint8_t)DeserializerError::Source::Payload;
        report(entry, cppkafka::LogLevel::LogErr, RD_KAFKA_RESP_ERR__VALUE_DESERIALIZATION, "Failed to deserialize payload", &kafkaMessage);
    }
    
    return DeserializedMessage(std::move(key), std::move(payload), std::move(headers), de);
}

int ConsumerManagerImpl::invokeReceiver(ConsumerTopicEntry& entry,
                                        cppkafka::Message&& kafkaMessage,
                                        IoTracker)
{
    DeserializedMessage deserializedMessage;
    //pre-process message
    if (entry._preprocess &&
        entry._preprocessorCallback &&
        !kafkaMessage.is_eof() &&
        !entry._preprocessorCallback(kafkaMessage)) {
        std::get<(int)Field::Error>(deserializedMessage)._source |= (uint8_t)DeserializerError::Source::Preprocessor;
    }
    //check errors (if any)
    if (kafkaMessage.get_error()) {
        std::get<(int)Field::Error>(deserializedMessage)._error = kafkaMessage.get_error();
        std::get<(int)Field::Error>(deserializedMessage)._source |= (uint8_t)DeserializerError::Source::Kafka;
    }
    //deserialize
    if (std::get<(int)Field::Error>(deserializedMessage)._source == 0) {
        //no errors were detected
        deserializedMessage = deserializeMessage(entry, kafkaMessage);
    }
    //call receiver callback
    entry._enableWatermarkCheck = true;
    cppkafka::CallbackInvoker<Receiver>("receiver", entry._configuration.getTypeErasedReceiver(), entry._consumer.get())
          (*entry._committer,
           entry._offsets,
           std::move(kafkaMessage), //kafka raw message
           std::get<(int)Field::Key>(std::move(deserializedMessage)), //key
           std::get<(int)Field::Payload>(std::move(deserializedMessage)), //payload
           std::get<(int)Field::Headers>(std::move(deserializedMessage)), //headers
           std::get<(int)Field::Error>(std::move(deserializedMessage)), //error
           makeOffsetPersistSettings(entry));
    return (std::get<(int)Field::Error>(deserializedMessage)._source == 0) ? 0 : -1;
}

int
ConsumerManagerImpl::pollCoro(quantum::VoidContextPtr ctx,
                              ConsumerTopicEntry& entry,
                              IoTracker tracker)
{
    try {
        // Start the IO task to get messages
        quantum::CoroFuture<MessageContainer>::Ptr future;
        if (entry._pollStrategy == PollStrategy::Serial) {
            future = ctx->postAsyncIo((int) entry._pollIoThreadId,
                                      false,
                                      messageSerialReceiveTask,
                                      entry,
                                      IoTracker(tracker));
        }
        else {
            //Round robin
            future = ctx->postAsyncIo((int)entry._pollIoThreadId,
                                      false,
                                      messageRoundRobinReceiveTask,
                                      entry,
                                      IoTracker(tracker));
        }
        // Receive all messages from kafka and deserialize in parallel
        bool isBufferClosed = false;
        while (!isBufferClosed) {
            cppkafka::Message message = future->pull(ctx, isBufferClosed);
            if (!isBufferClosed) {
                //deliver to application
                processMessage(ctx, entry, std::move(message));
            }
        }
    }
    catch (const std::exception& ex) {
        exceptionHandler(ex, entry);
        return -1;
    }
    // Pass the message queue to the processor coroutine
    return 0;
}

void ConsumerManagerImpl::processMessageBatch(quantum::VoidContextPtr ctx,
                                              ConsumerTopicEntry& entry,
                                              MessageBatch&& kafkaMessages)
{
    if (kafkaMessages.empty()) {
        return; //nothing to do
    }
    if (entry._receiverThread == ThreadType::IO) {
        const int numThreads = entry._receiveCallbackThreadRange.second -
                               entry._receiveCallbackThreadRange.first + 1;
        if (numThreads == 1) {
            // optimization: no need to spend time on message distribution for a single io queue
            int queueId = mapPartitionToQueue(kafkaMessages.front().get_partition(), entry);
            quantum::ICoroFuture<int>::Ptr future =
                    ctx->postAsyncIo(queueId,
                                     false,
                                     receiveMessageBatch,
                                     entry,
                                     std::move(kafkaMessages),
                                     IoTracker(entry._ioTracker));
            if (entry._receiveCallbackExec == ExecMode::Sync) {
                future->get(ctx);
            }
        }
        else {
            Batches receiverBatches(numThreads);
            //reserve enough space
            for (auto &batch : receiverBatches) {
                //we reserve conservatively
                batch.reserve(kafkaMessages.size()/2);
            }
            //distribute batches to be invoked on separate IO threads
            for (auto &&message : kafkaMessages) {
                receiverBatches[message.get_partition() % numThreads].emplace_back(std::move(message));
            }
            // invoke batch jobs for the partitioned messages
            std::vector<quantum::ICoroFuture<int>::Ptr> ioFutures;
            if (entry._receiveCallbackExec == ExecMode::Sync) {
                ioFutures.reserve(numThreads);
            }
            for (int i = 0; i < numThreads; ++i) {
                if (!receiverBatches[i].empty()) {
                    int queueId = mapPartitionToQueue(receiverBatches[i].front().get_partition(), entry);
                    quantum::ICoroFuture<int>::Ptr future =
                            ctx->postAsyncIo(queueId,
                                             false,
                                             receiveMessageBatch,
                                             entry,
                                             std::move(receiverBatches[i]),
                                             IoTracker(entry._ioTracker));
                    if (entry._receiveCallbackExec == ExecMode::Sync) {
                        ioFutures.push_back(future);
                    }
                }
            }
            // wait until all the batches are processed
            for (auto &&c: ioFutures) {
                c->get(ctx);
            }
        }
    }
    else {
        //call directly on this coroutine
        receiveMessageBatch(entry, std::move(kafkaMessages), IoTracker{});
    }
}

int ConsumerManagerImpl::pollBatchCoro(quantum::VoidContextPtr ctx,
                                       ConsumerTopicEntry& entry,
                                       IoTracker tracker)
{
    try {
        // get the messages from the pre-fetched future, or
        MessageBatch raw;
        if (entry._batchPrefetch && entry._batchPrefetchFuture)
        {
            raw = entry._batchPrefetchFuture->get(ctx);
        }
        else {
            raw = ctx->postAsyncIo((int)entry._pollIoThreadId,
                                   false,
                                   messageBatchReceiveTask,
                                   entry,
                                   IoTracker(tracker))->get(ctx);
        }
        if (entry._batchPrefetch && !entry._shuttingDown) {
            // start pre-fetching for the next round
            entry._batchPrefetchFuture = ctx->postAsyncIo((int)entry._pollIoThreadId,
                                                          false,
                                                          messageBatchReceiveTask,
                                                          entry,
                                                          IoTracker(tracker));
        }
        // process messages
        processMessageBatch(ctx, entry, std::move(raw));
        return 0;
    }
    catch (const std::exception& ex) {
        exceptionHandler(ex, entry);
        return -1;
    }
}

int ConsumerManagerImpl::receiveMessageBatch(ConsumerTopicEntry& entry,
                                             MessageBatch&& kafkaMessages,
                                             IoTracker ioTracker)
{
    int rc = 0;
    for (auto&& message : kafkaMessages) {
        try {
            int temp = invokeReceiver(entry, std::move(message), ioTracker);
            if (temp != 0) rc = temp;
        }
        catch (const std::exception& ex) {
            exceptionHandler(ex, entry);
            rc = -1;
        }
    }
    return rc;
}

int ConsumerManagerImpl::processMessage(quantum::VoidContextPtr ctx,
                                        ConsumerTopicEntry& entry,
                                        cppkafka::Message&& kafkaMessage)
{
    try {
        if (entry._receiverThread == ThreadType::IO) {
            // Find out on which IO thread we should process this message
            int ioQueue = mapPartitionToQueue(kafkaMessage.get_partition(), entry);
            // Post and wait until delivered
            quantum::ICoroFuture<int>::Ptr future = ctx->postAsyncIo(ioQueue,
                                                                     false,
                                                                     invokeReceiver,
                                                                     entry,
                                                                     std::move(kafkaMessage),
                                                                     IoTracker(entry._ioTracker));
            if (entry._receiveCallbackExec == ExecMode::Sync) {
                future->get(ctx);
            }
        }
        else {
            //call serially on this coroutine
            invokeReceiver(entry, std::move(kafkaMessage), IoTracker{});
        }
    }
    catch (const std::exception& ex) {
        exceptionHandler(ex, entry);
        return -1;
    }
    return 0;
}

void ConsumerManagerImpl::exceptionHandler(const std::exception& ex,
                                           const ConsumerTopicEntry& topicEntry)
{
    handleException(ex, makeMetadata(topicEntry), topicEntry._configuration, topicEntry._logLevel);
}

ConsumerMetadata ConsumerManagerImpl::makeMetadata(const ConsumerTopicEntry& topicEntry)
{
    return ConsumerMetadata(topicEntry._configuration.getTopic(),
                            topicEntry._consumer.get(),
                            topicEntry._configuration.getPartitionStrategy());
}

int ConsumerManagerImpl::mapPartitionToQueue(int partition,
                                             const ConsumerTopicEntry& topicEntry)
{
    int rangeSize = topicEntry._receiveCallbackThreadRange.second - topicEntry._receiveCallbackThreadRange.first + 1;
    if ((topicEntry._numIoThreads == rangeSize) && !topicEntry._preserveMessageOrder) {
        return (int)quantum::IQueue::QueueId::Any; //pick any queue
    }
    //select specific queue
    return (partition % rangeSize) + topicEntry._receiveCallbackThreadRange.first;
}

OffsetPersistSettings ConsumerManagerImpl::makeOffsetPersistSettings(const ConsumerTopicEntry& topicEntry)
{
    return {topicEntry._autoOffsetPersist,
            topicEntry._autoOffsetPersistOnException,
            topicEntry._autoOffsetPersistStrategy,
            topicEntry._autoCommitExec};
}

ConsumerManagerImpl::Consumers::iterator
ConsumerManagerImpl::findConsumer(const std::string& topic)
{
    auto it = _consumers.find(topic);
    if (it == _consumers.end()) {
        throw TopicException(topic, "Not found");
    }
    return it;
}

ConsumerManagerImpl::Consumers::const_iterator
ConsumerManagerImpl::findConsumer(const std::string& topic) const
{
    auto it = _consumers.find(topic);
    if (it == _consumers.end()) {
        throw TopicException(topic, "Not found");
    }
    return it;
}

bool ConsumerManagerImpl::hasNewMessages(ConsumerTopicEntry& entry) const
{
    if (!entry._enableWatermarkCheck) {
        return true;
    }
    Metadata::OffsetWatermarkList watermarks = makeMetadata(entry).getOffsetWatermarks();
    if (entry._watermarks.empty()) {
        //update watermarks
        entry._watermarks = watermarks;
        return true;
    }
    bool hasNew = false;
    for (size_t i = 0; i < watermarks.size(); ++i) {
        if (watermarks[i]._watermark._high < 0) continue;
        if (watermarks[i]._watermark._high > entry._watermarks[i]._watermark._high) {
            hasNew = true;
            //update watermarks
            entry._watermarks = watermarks;
            break;
        }
    }
    return hasNew;
}

}
}
