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
#include <iterator>

using namespace std::placeholders;

namespace Bloomberg {
namespace corokafka {

//===========================================================================================
//                               class ConsumerManagerImpl
//===========================================================================================
ConsumerManagerImpl::ConsumerManagerImpl(quantum::Dispatcher& dispatcher,
                                         const ConnectorConfiguration& connectorConfiguration,
                                         const ConfigMap& configs,
                                         std::atomic_bool& interrupt) :
    _dispatcher(dispatcher),
    _connectorConfiguration(connectorConfiguration),
    _shutdownIoWaitTimeoutMs(connectorConfiguration.getShutdownIoWaitTimeout())
{
    //Create a consumer for each topic and apply the appropriate configuration
    for (const auto& entry : configs) {
        // Process each configuration
        auto it = _consumers.emplace(entry.first, ConsumerTopicEntry(nullptr,
                                                                     _connectorConfiguration,
                                                                     entry.second,
                                                                     interrupt,
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
                                         ConfigMap&& configs,
                                         std::atomic_bool& interrupt) :
    _dispatcher(dispatcher),
    _connectorConfiguration(connectorConfiguration),
    _shutdownIoWaitTimeoutMs(connectorConfiguration.getShutdownIoWaitTimeout())
{
    //Create a consumer for each topic and apply the appropriate configuration
    for (auto&& entry : configs) {
        // Process each configuration
        auto it = _consumers.emplace(entry.first, ConsumerTopicEntry(nullptr,
                                                                     _connectorConfiguration,
                                                                     std::move(entry.second),
                                                                     interrupt,
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
    const Configuration::OptionList &rdKafkaOptions = topicEntry._configuration.
            getOptions(Configuration::OptionType::RdKafka);
    const Configuration::OptionList &rdKafkaTopicOptions = topicEntry._configuration.
            getTopicOptions(Configuration::OptionType::RdKafka);
    
    auto extract = [&](const std::string &name, auto &value) -> bool
    {
        const cppkafka::ConfigurationOption* op = topicEntry._configuration.getOption(name);
        if (!op) {
            op = topicEntry._configuration.getTopicOption(name);
        }
        return ConsumerConfiguration::extract(name)(topic, op, &value);
    };
    
    //Validate config
    if (!topicEntry._configuration.getOption(Configuration::RdKafkaOptions::metadataBrokerList)) {
        throw InvalidOptionException(topic, Configuration::RdKafkaOptions::metadataBrokerList, "Missing");
    }
    
    if (!topicEntry._configuration.getOption(Configuration::RdKafkaOptions::groupId)) {
        throw InvalidOptionException(topic, Configuration::RdKafkaOptions::groupId, "Missing");
    }
    
    //Set the rdkafka configuration options
    cppkafka::Configuration kafkaConfig(rdKafkaOptions);
    kafkaConfig.set_default_topic_configuration(cppkafka::TopicConfiguration(rdKafkaTopicOptions));
    
    extract(ConsumerConfiguration::Options::autoThrottle, topicEntry._throttleControl.autoThrottle());
    extract(ConsumerConfiguration::Options::autoThrottleMultiplier, topicEntry._throttleControl.throttleMultiplier());
    
    //Set the global callbacks
    if (topicEntry._configuration.getErrorCallback()) {
        auto errorFunc = std::bind(errorCallbackInternal, std::ref(topicEntry), _1, _2, _3);
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
    
    extract(ConsumerConfiguration::Options::offsetPersistStrategy, topicEntry._autoOffsetPersistStrategy);
    
    extract(ConsumerConfiguration::Options::autoOffsetPersistOnException, topicEntry._autoOffsetPersistOnException);
    
    extract(ConsumerConfiguration::Options::commitExec, topicEntry._autoCommitExec);
    
    // Set underlying rdkafka options
    kafkaConfig.set(Configuration::RdKafkaOptions::enableAutoOffsetStore, false);
    kafkaConfig.set(Configuration::RdKafkaOptions::enableAutoCommit,
                    topicEntry._autoOffsetPersistStrategy == OffsetPersistStrategy::Store);
    if (topicEntry._autoOffsetPersistStrategy == OffsetPersistStrategy::Store) {
        //Make sure the application has properly set the commit interval. If the option is not set explicitly,
        // it will assume the default RdKafka value.
        const auto *option = topicEntry._configuration.getOption(Configuration::RdKafkaOptions::autoCommitIntervalMs);
        if (option) {
            //make sure it's a positive value (min 1ms, max 1min)
            Configuration::extractCounterValue(topic, Configuration::RdKafkaOptions::autoCommitIntervalMs, *option, 1, 60000);
        }
    }
    
    //=======================================================================================
    //DO NOT UPDATE ANY KAFKA CONFIG OPTIONS BELOW THIS POINT SINCE THE CONSUMER MAKES A COPY
    //=======================================================================================
    
    //Create a consumer
    topicEntry._consumer = std::make_unique<cppkafka::Consumer>(kafkaConfig);
    topicEntry._committer = std::make_unique<cppkafka::BackoffCommitter>(*topicEntry._consumer);
    
    // Set the consumer callbacks
    auto assignmentFunc = std::bind(&ConsumerManagerImpl::assignmentCallback, std::ref(topicEntry), _1);
    topicEntry._consumer->set_assignment_callback(std::move(assignmentFunc));

    auto revocationFunc = std::bind(&ConsumerManagerImpl::revocationCallback, std::ref(topicEntry), _1);
    topicEntry._consumer->set_revocation_callback(std::move(revocationFunc));
    
    auto rebalanceErrorFunc = std::bind(&ConsumerManagerImpl::rebalanceErrorCallback, std::ref(topicEntry), _1);
    topicEntry._consumer->set_rebalance_error_callback(std::move(rebalanceErrorFunc));
    
    auto offsetCommitErrorFunc = std::bind(&offsetCommitErrorCallback, std::ref(topicEntry), _1);
    topicEntry._committer->set_error_callback(offsetCommitErrorFunc);
    
    //Set the startup timeout
    std::chrono::milliseconds defaultTimeout = topicEntry._consumer->get_timeout();
    topicEntry._brokerTimeout = defaultTimeout;
    if (extract(TopicConfiguration::Options::brokerTimeoutMs, topicEntry._brokerTimeout)) {
        topicEntry._consumer->set_timeout(topicEntry._brokerTimeout);
    }
    else if (extract(ConsumerConfiguration::Options::startupTimeoutMs, topicEntry._brokerTimeout)) {
        //Deprecated option
        topicEntry._consumer->set_timeout(topicEntry._brokerTimeout);
    }
    
    //Get events queue for polling
    topicEntry._eventQueue = topicEntry._consumer->get_main_queue();
    
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
    topicEntry._committer->set_backoff_step(commitBackoffStep);
    if (extract(ConsumerConfiguration::Options::commitBackoffIntervalMs, commitBackoffStep)) {
        topicEntry._committer->set_initial_backoff(commitBackoffStep);
        topicEntry._committer->set_backoff_step(commitBackoffStep);
    }
    
    std::chrono::milliseconds commitMaxBackoff;
    if (extract(ConsumerConfiguration::Options::commitMaxBackoffMs, commitMaxBackoff)) {
        if (commitMaxBackoff < commitBackoffStep) {
            throw InvalidOptionException(topic, ConsumerConfiguration::Options::commitMaxBackoffMs, std::to_string(commitMaxBackoff.count()));
        }
        topicEntry._committer->set_maximum_backoff(commitMaxBackoff);
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
    if (((int)topicEntry._pollIoThreadId < 0) &&
        ((int)topicEntry._pollIoThreadId >= _dispatcher.getNumIoThreads())) {
        throw InvalidOptionException(topic, ConsumerConfiguration::Options::pollIoThreadId, "Value is out of bounds");
    }
    
    extract(ConsumerConfiguration::Options::processCoroThreadId, topicEntry._processCoroThreadId);
    if (((int)topicEntry._processCoroThreadId < 0) &&
        ((int)topicEntry._processCoroThreadId >= _dispatcher.getNumCoroutineThreads())) {
        throw InvalidOptionException(topic, ConsumerConfiguration::Options::processCoroThreadId, "Value is out of bounds");
    }
    
    bool pauseOnStart = false;
    if (extract(ConsumerConfiguration::Options::pauseOnStart, pauseOnStart)) {
        if (pauseOnStart) {
            pauseImpl(topicEntry);
        }
    }
    
    extract(ConsumerConfiguration::Options::pollStrategy, topicEntry._pollStrategy);
    if (topicEntry._pollStrategy == PollStrategy::RoundRobin) {
        topicEntry._roundRobinPoller = std::make_unique<cppkafka::RoundRobinPollStrategy>(*topicEntry._consumer);
    }
    
    //dynamically subscribe or statically assign partitions to this consumer
    subscribe(topic, topicEntry._subscription._partitionAssignment);
    
    //Set the consumer timeouts
    std::chrono::milliseconds timeout;
    if (extract(ConsumerConfiguration::Options::timeoutMs, timeout)) {
        topicEntry._consumer->set_timeout(timeout);
    }
    else {
        //reset default timeout
        topicEntry._consumer->set_timeout(defaultTimeout);
    }
    
    extract(ConsumerConfiguration::Options::pollTimeoutMs, topicEntry._pollTimeout);
    if (topicEntry._pollTimeout.count() == EnumValue(TimerValues::Disabled)) {
        topicEntry._pollTimeout = topicEntry._consumer->get_timeout();
    }
    
    extract(ConsumerConfiguration::Options::minRoundRobinPollTimeoutMs, topicEntry._minPollInterval); //deprecated
    if (topicEntry._minPollInterval.count() == EnumValue(TimerValues::Disabled)) {
        extract(ConsumerConfiguration::Options::minPollIntervalMs, topicEntry._minPollInterval);
    }
    if (topicEntry._minPollInterval.count() == EnumValue(TimerValues::Disabled)) {
        //set default value
        topicEntry._minPollInterval = std::chrono::milliseconds(10);
    }
    //The interval cannot be larger than the poll timeout
    if ((topicEntry._pollTimeout >= std::chrono::milliseconds::zero()) &&
        ((topicEntry._minPollInterval > topicEntry._pollTimeout) ||
        (topicEntry._minPollInterval.count() == EnumValue(TimerValues::Unlimited)))) {
        throw InvalidOptionException(topic, ConsumerConfiguration::Options::minPollIntervalMs, "Must be smaller than poll timeout or -2.");
    }
}

ConsumerMetadata ConsumerManagerImpl::getMetadata(const std::string& topic)
{
    auto it = findConsumer(topic);
    return makeMetadata(it->second);
}

void ConsumerManagerImpl::enablePreprocessing()
{
    for (auto&& consumer : _consumers) {
        consumer.second._preprocess = true;
    }
}

void ConsumerManagerImpl::enablePreprocessing(const std::string& topic)
{
    findConsumer(topic)->second._preprocess = true;
}

void ConsumerManagerImpl::disablePreprocessing()
{
    for (auto&& consumer : _consumers) {
        consumer.second._preprocess = false;
    }
}

void ConsumerManagerImpl::disablePreprocessing(const std::string& topic)
{
    findConsumer(topic)->second._preprocess = false;
}

void ConsumerManagerImpl::pause()
{
    for (auto&& consumer : _consumers) {
        pauseImpl(consumer.second);
    }
}

void ConsumerManagerImpl::pause(const std::string& topic)
{
    auto& topicEntry = findConsumer(topic)->second;
    pauseImpl(topicEntry);
}

void ConsumerManagerImpl::pauseImpl(ConsumerTopicEntry& topicEntry)
{
    bool paused = false;
    if (topicEntry._isPaused.compare_exchange_strong(paused, true)) {
        if (topicEntry._subscription._isSubscribed) {
            //Pause assigned partitions only
            topicEntry._consumer->pause_partitions(topicEntry._subscription._partitionAssignment);
        }
        else {
            //Pause all partitions
            topicEntry._consumer->pause_partitions(
                  cppkafka::convert(topicEntry._configuration.getTopic(),
                                    makeMetadata(topicEntry).
                                    getTopicMetadata(topicEntry._brokerTimeout).
                                    get_partitions()));
        }
    }
}

void ConsumerManagerImpl::resume()
{
    for (auto&& consumer : _consumers) {
        resumeImpl(consumer.second);
    }
}

void ConsumerManagerImpl::resume(const std::string& topic)
{
    auto& topicEntry = findConsumer(topic)->second;
    resumeImpl(topicEntry);
}

void ConsumerManagerImpl::resumeImpl(ConsumerTopicEntry& topicEntry)
{
    bool paused = true;
    if (topicEntry._isPaused.compare_exchange_strong(paused, false)) {
        if (topicEntry._subscription._isSubscribed) {
            //Resume assigned partitions only
            topicEntry._consumer->resume_partitions(topicEntry._subscription._partitionAssignment);
        }
        else {
            //Resume all partitions
            topicEntry._consumer->resume_partitions(
                   cppkafka::convert(topicEntry._configuration.getTopic(),
                                     makeMetadata(topicEntry).
                                     getTopicMetadata(topicEntry._brokerTimeout).
                                     get_partitions()));
        }
    }
}

void ConsumerManagerImpl::subscribe(const cppkafka::TopicPartitionList& partitionList)
{
    for (auto&& consumer : _consumers) {
        subscribeImpl(consumer.second, partitionList);
    }
}

void ConsumerManagerImpl::subscribe(const std::string& topic,
                                    const cppkafka::TopicPartitionList& partitionList)
{
    subscribeImpl(findConsumer(topic)->second, partitionList);
}

void ConsumerManagerImpl::subscribeImpl(ConsumerTopicEntry& topicEntry,
                                        const cppkafka::TopicPartitionList& partitionList)
{
    quantum::Mutex::Guard lock(quantum::local::context(),
                               topicEntry._subscription._mutex);
    if (topicEntry._subscription._isSubscribed) {
        return; //nothing to do
    }
    //process partitions and make sure offsets are valid
    const std::string& topic = topicEntry._configuration.getTopic();
    cppkafka::TopicPartitionList assignment = partitionList.empty() ?
            topicEntry._configuration.getInitialPartitionAssignment() : partitionList;
    if ((assignment.size() == 1) &&
        (assignment.front().get_partition() == RD_KAFKA_PARTITION_UA)) {
        //Overwrite the initial assignment if the user provided no partitions
        cppkafka::TopicMetadata metadata =
                topicEntry._consumer->get_metadata(topicEntry._consumer->get_topic(topic),
                                                   topicEntry._brokerTimeout);
        int offset = assignment.front().get_offset();
        assignment = cppkafka::convert(topic, metadata.get_partitions());
        //set the specified offset on all existing partitions
        for (auto& partition : assignment) {
            partition.set_offset(offset);
        }
    }
    if (assignment.empty()) {
        //use updated current assignment. Note that the current _partitionAssignment could be empty.
        assignment = topicEntry._subscription._partitionAssignment;
    }
    else {
        topicEntry._subscription._partitionAssignment = assignment;
    }
    //assign or subscribe
    if (topicEntry._configuration.getPartitionStrategy() == PartitionStrategy::Static) {
        if (assignment.empty()) {
            //we must have valid partitions
            throw ConsumerException(topic, "Empty partition assignment");
        }
        if (topicEntry._pollStrategy == PollStrategy::RoundRobin) {
            topicEntry._roundRobinPoller->assign(assignment);
        }
        //invoke the assignment callback manually
        ConsumerManagerImpl::assignmentCallbackImpl(topicEntry, assignment);
        topicEntry._consumer->assign(assignment);
    }
    else { //Dynamic strategy
        topicEntry._subscription._setOffsetsOnStart = true;
        topicEntry._consumer->subscribe({topic});
    }
}

void ConsumerManagerImpl::unsubscribe()
{
    for (auto&& consumer : _consumers) {
        unsubscribeImpl(consumer.second);
    }
}

void ConsumerManagerImpl::unsubscribe(const std::string& topic)
{
    unsubscribeImpl(findConsumer(topic)->second);
}

void ConsumerManagerImpl::unsubscribeImpl(ConsumerTopicEntry& topicEntry)
{
    quantum::Mutex::Guard lock(quantum::local::context(),
                               topicEntry._subscription._mutex);
    if (!topicEntry._subscription._isSubscribed) {
        return;
    }
    if (topicEntry._configuration.getPartitionStrategy() == PartitionStrategy::Static) {
        if (topicEntry._pollStrategy == PollStrategy::RoundRobin) {
            topicEntry._roundRobinPoller->revoke();
        }
        //invoke the revocation callback manually
        ConsumerManagerImpl::revocationCallbackImpl(topicEntry, topicEntry._consumer->get_assignment());
        topicEntry._consumer->unassign();
    }
    else {
        topicEntry._consumer->unsubscribe();
    }
}

cppkafka::Error ConsumerManagerImpl::commit(const cppkafka::TopicPartition& topicPartition,
                                            ExecMode execMode,
                                            const void* opaque)
{
    return commit(cppkafka::TopicPartitionList{topicPartition}, execMode, opaque);
}

cppkafka::Error ConsumerManagerImpl::commit(const cppkafka::TopicPartition& topicPartition,
                                            const void* opaque)
{
    return commit(cppkafka::TopicPartitionList{topicPartition}, opaque);
}

cppkafka::Error ConsumerManagerImpl::commit(const cppkafka::TopicPartitionList& topicPartitions,
                                            const void* opaque)
{
    return commitImpl(topicPartitions, nullptr, opaque);
}

cppkafka::Error ConsumerManagerImpl::commit(const cppkafka::TopicPartitionList& topicPartitions,
                                            ExecMode execMode,
                                            const void* opaque)
{
    return commitImpl(topicPartitions, &execMode, opaque);
}

cppkafka::Error ConsumerManagerImpl::commitImpl(const cppkafka::TopicPartitionList& topicPartitions,
                                                ExecMode* execMode,
                                                const void* opaque)
{
    if (topicPartitions.empty()) {
        return RD_KAFKA_RESP_ERR_INVALID_PARTITIONS;
    }
    //Group the original partitions by topic
    std::unordered_map<std::string, cppkafka::TopicPartitionList> partitionsByTopic;
    for (const auto& topicPartition : topicPartitions) {
        partitionsByTopic[topicPartition.get_topic()].emplace_back(topicPartition);
    }
    cppkafka::Error error;
    for (const auto& listEntry : partitionsByTopic) {
        const cppkafka::TopicPartitionList& partitionList = listEntry.second;
        auto it = _consumers.find(listEntry.first);
        if (it == _consumers.end()) {
            return RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART;
        }
        error = commitImpl(it->second, partitionList, execMode ? *execMode : it->second._autoCommitExec, opaque);
        if (error) {
            break;
        }
    }
    return error;
}

cppkafka::Error ConsumerManagerImpl::commitImpl(ConsumerTopicEntry& entry,
                                                const cppkafka::TopicPartitionList& topicPartitions,
                                                ExecMode execMode,
                                                const void* opaque)
{
    try {
        if (topicPartitions.empty()) {
            return RD_KAFKA_RESP_ERR_INVALID_PARTITIONS;
        }
        const cppkafka::TopicPartition& headPartition = topicPartitions[0];
        if (entry._committer->get_consumer().get_configuration().get_offset_commit_callback() && (opaque != nullptr)) {
            entry._offsets.insert(headPartition, opaque);
        }
        if (entry._autoOffsetPersistStrategy == OffsetPersistStrategy::Commit) {
            if (execMode == ExecMode::Sync) {
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

void ConsumerManagerImpl::raiseWatermark(ConsumerTopicEntry& entry,
                                         const cppkafka::TopicPartition& toppar)
{
    int partition = toppar.get_partition();
    for (auto& w : entry._watermarks) {
        if ((w._partition == partition) &&
            (w._watermark._high < toppar.get_offset())) {
            //Update the watermark level with the new one.
            w._watermark._high = toppar.get_offset();
            break;
        }
    }
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
        //unsubscribe all the consumers
        unsubscribe();
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
                bool highPriority{true};
                if (entry.second._pollStrategy == PollStrategy::Batch) {
                    entry.second._pollFuture =
                      _dispatcher.post(static_cast<int>(entry.second._processCoroThreadId),
                                       highPriority,
                                       pollBatchCoro,
                                       entry.second,
                                       IoTracker(entry.second._ioTracker));
                }
                else {
                    //Round-robin or serial
                    entry.second._pollFuture =
                        _dispatcher.post(static_cast<int>(entry.second._processCoroThreadId),
                                         highPriority,
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

void ConsumerManagerImpl::errorCallbackInternal(
                        ConsumerTopicEntry& topicEntry,
                        cppkafka::KafkaHandleBase& handle,
                        int error,
                        const std::string& reason)
{
    //call application error callback
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
        //calculate throttling status
        ThrottleControl::Status status = topicEntry._throttleControl.handleThrottleCallback(throttleDuration);
        if (status == ThrottleControl::Status::On) {
            pauseImpl(topicEntry);
        }
        else if (status == ThrottleControl::Status::Off) {
            resumeImpl(topicEntry);
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
        raiseWatermark(topicEntry, partition);
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
    quantum::Mutex::Guard lock(quantum::local::context(),
                               topicEntry._subscription._mutex);
    assignmentCallbackImpl(topicEntry, topicPartitions);
}

void ConsumerManagerImpl::assignmentCallbackImpl(
                        ConsumerTopicEntry& topicEntry,
                        cppkafka::TopicPartitionList& topicPartitions)
{
    topicEntry._subscription._isSubscribed = true;
    // Clear any throttling we may have
    topicEntry._throttleControl.reset();
    PartitionStrategy strategy = topicEntry._configuration.getPartitionStrategy();
    if ((strategy == PartitionStrategy::Dynamic) &&
        !topicEntry._subscription._partitionAssignment.empty() &&
        topicEntry._subscription._setOffsetsOnStart) {
        topicEntry._subscription._setOffsetsOnStart = false;
        //perform first offset assignment based on user config
        for (auto&& partition : topicPartitions) {
            auto it = std::find_if(topicEntry._subscription._partitionAssignment.begin(),
                                   topicEntry._subscription._partitionAssignment.end(),
                                   [&partition](const auto& assigned)->bool{
                return (partition.get_partition() == assigned.get_partition());
            });
            if (it != topicEntry._subscription._partitionAssignment.end()) {
                partition.set_offset(it->get_offset());
            }
        }
    }
    topicEntry._subscription._partitionAssignment = topicPartitions; //overwrite original if any
    cppkafka::CallbackInvoker<Callbacks::RebalanceCallback>
        ("assignment", topicEntry._configuration.getRebalanceCallback(), topicEntry._consumer.get())
            (makeMetadata(topicEntry), cppkafka::Error(RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS), topicPartitions);
}

void ConsumerManagerImpl::revocationCallback(
                        ConsumerTopicEntry& topicEntry,
                        const cppkafka::TopicPartitionList& topicPartitions)
{
    quantum::Mutex::Guard lock(quantum::local::context(),
                               topicEntry._subscription._mutex);
    revocationCallbackImpl(topicEntry, topicPartitions);
}

void ConsumerManagerImpl::revocationCallbackImpl(
                        ConsumerTopicEntry& topicEntry,
                        const cppkafka::TopicPartitionList& topicPartitions)
{
    topicEntry._subscription.reset();
    PartitionStrategy strategy = topicEntry._configuration.getPartitionStrategy();
    topicEntry._watermarks.clear(); //clear offset watermarks
    cppkafka::CallbackInvoker<Callbacks::RebalanceCallback>
        ("revocation", topicEntry._configuration.getRebalanceCallback(), topicEntry._consumer.get())
            (makeMetadata(topicEntry), cppkafka::Error(RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS), const_cast<cppkafka::TopicPartitionList&>(topicPartitions));
}

void ConsumerManagerImpl::rebalanceErrorCallback(
                        ConsumerTopicEntry& topicEntry,
                        cppkafka::Error error)
{
    quantum::Mutex::Guard lock(quantum::local::context(),
                               topicEntry._subscription._mutex);
    rebalanceErrorCallbackImpl(topicEntry, error);
}

void ConsumerManagerImpl::rebalanceErrorCallbackImpl(
                        ConsumerTopicEntry& topicEntry,
                        cppkafka::Error error)
{
    topicEntry._subscription.reset();
    PartitionStrategy strategy = topicEntry._configuration.getPartitionStrategy();
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
        logCallback(topicEntry, *topicEntry._consumer, EnumValue(level), "corokafka", reason);
    }
}

MessageBatch ConsumerManagerImpl::messageBatchReceiveTask(ConsumerTopicEntry& entry, IoTracker)
{
    MessageBatch batch;
    try {
        if (entry._pollTimeout >= std::chrono::milliseconds::zero()) {
            //make a single poll
            batch = entry._consumer->poll_batch(entry._readSize, entry._pollTimeout);
        }
        else {
            //break the call into smaller chunks so we don't block permanently if shutting down
            while (!entry._interrupt && ((ssize_t)batch.size() < entry._readSize)) {
                using IterType = MessageBatch::iterator;
                MessageBatch tempBatch = entry._consumer->poll_batch(entry._readSize, entry._minPollInterval);
                if (batch.empty()) {
                    std::swap(batch, tempBatch);
                }
                else {
                    //merge batches
                    batch.reserve(entry._readSize);
                    batch.insert(batch.end(),
                                 std::move_iterator<IterType>(tempBatch.begin()),
                                 std::move_iterator<IterType>(tempBatch.end()));
                }
            }
        }
    }
    catch (const std::exception& ex) {
        exceptionHandler(ex, entry);
    }
    return batch;
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
        de.setError(DeserializerError::Source::Key);
        report(entry, cppkafka::LogLevel::LogErr, RD_KAFKA_RESP_ERR__KEY_DESERIALIZATION, "Failed to deserialize key", &kafkaMessage);
    }
    
    //Deserialize the headers if any
    HeaderPack headers(deserializer._headerEntries.size());
    using Header = cppkafka::Header<cppkafka::Buffer>;
    using Headers = cppkafka::HeaderList<Header>;
    const Headers& kafkaHeaders = kafkaMessage.get_header_list();
    std::vector<Headers::Iterator> unknownHeaders;
    unknownHeaders.reserve(kafkaHeaders.size());
    int num = 0;
    for (auto headerIt = kafkaHeaders.begin(); headerIt != kafkaHeaders.end(); ++headerIt) {
        //Find appropriate deserializer
        auto deserIter = deserializer._headerDeserializers.find(headerIt->get_name());
        if (deserIter == deserializer._headerDeserializers.end()) {
            if (entry._skipUnknownHeaders) {
                unknownHeaders.push_back(headerIt);
                continue;
            }
            //Always log error
            std::ostringstream oss;
            oss << "Unknown headers found in topic " << toppar.get_topic() << ": " << headerIt->get_name();
            de._error = RD_KAFKA_RESP_ERR__NOT_IMPLEMENTED;
            de.setError(DeserializerError::Source::Header);
            de._headerNum = num;
            report(entry, cppkafka::LogLevel::LogErr, RD_KAFKA_RESP_ERR__NOT_IMPLEMENTED, oss.str(), &kafkaMessage);
            break;
        }
        //Decode header
        const TypeErasedDeserializer::HeaderEntry& headerEntry = deserIter->second;
        headers[headerEntry._pos].first = headerIt->get_name();
        headers[headerEntry._pos].second = cppkafka::CallbackInvoker<Deserializer>("header_deserializer",
                                                                                   *headerEntry._deserializer,
                                                                                   entry._consumer.get())
                                                                     (toppar, headerIt->get_value());
        if (headers[headerEntry._pos].second.empty()) {
            // Decoding failed
            de._error = RD_KAFKA_RESP_ERR__VALUE_DESERIALIZATION;
            de.setError(DeserializerError::Source::Header);
            de._headerNum = num;
            std::ostringstream oss;
            oss << "Failed to deserialize header: " << headerIt->get_name();
            report(entry, cppkafka::LogLevel::LogErr, RD_KAFKA_RESP_ERR__VALUE_DESERIALIZATION, oss.str(), &kafkaMessage);
            break;
        }
        ++num;
    }
    //Output a warning for all unknown headers
    if (!unknownHeaders.empty()) {
        std::ostringstream oss;
        oss << "Unknown headers found in topic " << toppar.get_topic() << ": ";
        for (size_t h = 0; h < unknownHeaders.size(); ++h) {
            oss << unknownHeaders[h]->get_name();
            if (h != unknownHeaders.size()-1) {
                oss << ",";
            }
        }
        report(entry, cppkafka::LogLevel::LogWarning, RD_KAFKA_RESP_ERR__NOT_IMPLEMENTED, oss.str(), &kafkaMessage);
    }
    
    //Deserialize the payload
    boost::any payload = cppkafka::CallbackInvoker<Deserializer>("payload_deserializer",
                                                                 *deserializer._payloadDeserializer,
                                                                 entry._consumer.get())
                     (toppar, kafkaMessage.get_payload());
    if (payload.empty()) {
        // Decoding failed
        de._error = RD_KAFKA_RESP_ERR__VALUE_DESERIALIZATION;
        de.setError(DeserializerError::Source::Payload);
        report(entry, cppkafka::LogLevel::LogErr, RD_KAFKA_RESP_ERR__VALUE_DESERIALIZATION, "Failed to deserialize payload", &kafkaMessage);
    }
    
    return DeserializedMessage(std::move(key), std::move(payload), std::move(headers), de);
}

int ConsumerManagerImpl::invokeReceiver(ConsumerTopicEntry& entry,
                                        cppkafka::Message&& kafkaMessage,
                                        IoTracker)
{
    if (entry._interrupt) {
        return 0; //nothing to do
    }
    DeserializedMessage deserializedMessage;
    //pre-process message
    if (entry._preprocess &&
        entry._preprocessorCallback &&
        !kafkaMessage.is_eof() &&
        !entry._preprocessorCallback(kafkaMessage)) {
        std::get<EnumValue(Field::Error)>(deserializedMessage).setError(DeserializerError::Source::Preprocessor);
    }
    //check errors (if any)
    if (kafkaMessage.get_error()) {
        std::get<EnumValue(Field::Error)>(deserializedMessage)._error = kafkaMessage.get_error();
        std::get<EnumValue(Field::Error)>(deserializedMessage).setError(DeserializerError::Source::Kafka);
    }
    //deserialize
    if (!std::get<EnumValue(Field::Error)>(deserializedMessage).hasError()) {
        //no errors were detected
        deserializedMessage = deserializeMessage(entry, kafkaMessage);
    }
    //call receiver callback
    cppkafka::CallbackInvoker<Receiver>("receiver", entry._configuration.getTypeErasedReceiver(), entry._consumer.get())
          (*entry._committer,
           entry._offsets,
           std::move(kafkaMessage), //kafka raw message
           std::get<EnumValue(Field::Key)>(std::move(deserializedMessage)), //key
           std::get<EnumValue(Field::Payload)>(std::move(deserializedMessage)), //payload
           std::get<EnumValue(Field::Headers)>(std::move(deserializedMessage)), //headers
           std::get<EnumValue(Field::Error)>(std::move(deserializedMessage)), //error
           makeOffsetPersistSettings(entry));
    return (std::get<EnumValue(Field::Error)>(deserializedMessage)._source == 0) ? 0 : -1;
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
            future = ctx->postAsyncIo(static_cast<int>(entry._pollIoThreadId),
                                      false,
                                      messageReceiveTask<cppkafka::Consumer>,
                                      *entry._consumer,
                                      entry,
                                      IoTracker(tracker));
        }
        else {
            //Round robin
            future = ctx->postAsyncIo(static_cast<int>(entry._pollIoThreadId),
                                      false,
                                      messageReceiveTask<cppkafka::PollStrategyBase>,
                                      *entry._roundRobinPoller,
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
    if (kafkaMessages.empty() || entry._interrupt) {
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
            raw = ctx->postAsyncIo(static_cast<int>(entry._pollIoThreadId),
                                   false,
                                   messageBatchReceiveTask,
                                   entry,
                                   IoTracker(tracker))->get(ctx);
        }
        if (entry._batchPrefetch && !entry._interrupt) {
            // start pre-fetching for the next round
            entry._batchPrefetchFuture = ctx->postAsyncIo(static_cast<int>(entry._pollIoThreadId),
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
            if (temp != 0) {
                rc = temp;
            }
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
                            topicEntry._configuration.getPartitionStrategy(),
                            topicEntry._brokerTimeout);
}

int ConsumerManagerImpl::mapPartitionToQueue(int partition,
                                             const ConsumerTopicEntry& topicEntry)
{
    int rangeSize = topicEntry._receiveCallbackThreadRange.second - topicEntry._receiveCallbackThreadRange.first + 1;
    if ((topicEntry._numIoThreads == rangeSize) && !topicEntry._preserveMessageOrder) {
        return EnumValue(quantum::IQueue::QueueId::Any); //pick any queue
    }
    //select specific queue
    return (partition % rangeSize) + topicEntry._receiveCallbackThreadRange.first;
}

OffsetPersistSettings ConsumerManagerImpl::makeOffsetPersistSettings(const ConsumerTopicEntry& topicEntry)
{
    return {topicEntry._autoOffsetPersistStrategy,
            topicEntry._autoCommitExec,
            topicEntry._autoOffsetPersist,
            topicEntry._autoOffsetPersistOnException};
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
    OffsetWatermarkList watermarks = makeMetadata(entry).getOffsetWatermarks();
    if (watermarks.empty()) {
        return true;
    }
    bool hasNew = false;
    if (entry._watermarks.empty()) {
        //update watermarks
        entry._watermarks = watermarks;
        //Set high watermarks to invalid so that they can be set to the last committed value
        //once we start processing messages.
        for (auto& w : entry._watermarks) {
            w._watermark._high = cppkafka::TopicPartition::OFFSET_INVALID;
        }
        hasNew = true;
    }
    else {
        for (size_t i = 0; i < watermarks.size(); ++i) {
            //Note that for dynamic consumers, before the subscription takes effect, the
            //watermarks are always set to invalid. Therefore we must ensure that we poll
            //for messages until we have a valid subscription.
            if ((watermarks[i]._watermark._high > entry._watermarks[i]._watermark._high) ||
                (watermarks[i]._watermark._high == cppkafka::TopicPartition::OFFSET_INVALID)) {
                hasNew = true;
                break;
            }
        }
    }
    return hasNew;
}

}
}
