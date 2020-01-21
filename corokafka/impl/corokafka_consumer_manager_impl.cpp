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
#include <cppkafka/macros.h>
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
    _dispatcher(dispatcher)
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
    _dispatcher(dispatcher)
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
    const Configuration::OptionList& rdKafkaOptions = topicEntry._configuration.getOptions(Configuration::OptionType::RdKafka);
    const Configuration::OptionList& rdKafkaTopicOptions = topicEntry._configuration.getTopicOptions(Configuration::OptionType::RdKafka);
    const Configuration::OptionList& internalOptions = topicEntry._configuration.getOptions(Configuration::OptionType::Internal);
    
    auto extract = [&topic, &internalOptions](const std::string& name, auto& value)->bool {
        return ConsumerConfiguration::extract(name)(topic, Configuration::findOption(name, internalOptions), &value);
    };
    
    //Validate config
    const cppkafka::ConfigurationOption* brokerList =
        Configuration::findOption(Configuration::RdKafkaOptions::metadataBrokerList, rdKafkaOptions);
    if (!brokerList) {
        throw InvalidOptionException(topic, Configuration::RdKafkaOptions::metadataBrokerList, "Missing");
    }
    
    const cppkafka::ConfigurationOption* groupId =
        Configuration::findOption(Configuration::RdKafkaOptions::groupId, rdKafkaOptions);
    if (!groupId) {
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
        auto offsetCommitFunc = std::bind(offsetCommitCallback, std::ref(topicEntry), _1,  _2, _3);
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
    
    auto offsetCommitErrorFunc = std::bind(&offsetCommitErrorCallback, std::ref(topicEntry), _1);
    topicEntry._committer->set_error_callback(offsetCommitErrorFunc);
    
    //Set internal config options
    extract(ConsumerConfiguration::Options::skipUnknownHeaders, topicEntry._skipUnknownHeaders);
    
    std::chrono::milliseconds timeout;
    if (extract(ConsumerConfiguration::Options::timeoutMs, timeout)) {
        topicEntry._consumer->set_timeout(timeout);
    }
    
    extract(ConsumerConfiguration::Options::pollTimeoutMs, topicEntry._pollTimeout);
    
    extract(ConsumerConfiguration::Options::roundRobinMinPollTimeoutMs, topicEntry._roundRobinMinPollTimeout);
    
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
    
    extract(ConsumerConfiguration::Options::readSize, topicEntry._batchSize);
    
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
    if (topicEntry._receiverThread == ThreadType::Coro) {
        topicEntry._receiveCallbackExec = ExecMode::Sync; //override user setting
    }

    extract(ConsumerConfiguration::Options::batchPrefetch, topicEntry._batchPrefetch);
    
    extract(ConsumerConfiguration::Options::preprocessMessages, topicEntry._preprocess);
    
    extract(ConsumerConfiguration::Options::preprocessInvokeThread, topicEntry._preprocessorThread);
    
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
    
    //subscribe or statically assign partitions to this consumer
    if (topicEntry._configuration.getPartitionStrategy() == PartitionStrategy::Static) {
        if ((topicEntry._configuration.getInitialPartitionAssignment().size() == 1) &&
            (topicEntry._configuration.getInitialPartitionAssignment().front().get_partition() == RD_KAFKA_PARTITION_UA)) {
            //assign all partitions belonging to this topic
            cppkafka::TopicMetadata metadata = topicEntry._consumer->get_metadata(topicEntry._consumer->get_topic(topic));
            cppkafka::TopicPartitionList partitions = cppkafka::convert(topic, metadata.get_partitions());
            //set the specified offset on all partitions
            for (auto& p : partitions) {
                p.set_offset(topicEntry._configuration.getInitialPartitionAssignment().front().get_offset());
            }
            topicEntry._consumer->assign(partitions);
        }
        else {
            topicEntry._consumer->assign(topicEntry._configuration.getInitialPartitionAssignment());
        }
    }
    else {
        topicEntry._consumer->subscribe({topic});
    }
    
    PollStrategy pollStrategy = PollStrategy::Batch;
    extract(ConsumerConfiguration::Options::pollStrategy, pollStrategy);
    if (pollStrategy == PollStrategy::RoundRobin) {
        //This needs to be created after the partitions are assigned to the consumer
        topicEntry._roundRobin.reset(new cppkafka::RoundRobinPollStrategy(*topicEntry._consumer));
    }
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
            bool paused = false;
            if (consumer.second._isPaused.compare_exchange_strong(paused, !paused)) {
                consumer.second._consumer->pause();
            }
        }
    }
    else {
        bool paused = false;
        ConsumerTopicEntry& consumerTopicEntry = findConsumer(topic)->second;
        if (consumerTopicEntry._isPaused.compare_exchange_strong(paused, !paused)) {
            consumerTopicEntry._consumer->pause();
        }
    }
}

void ConsumerManagerImpl::resume(const std::string& topic)
{
    if (topic.empty()) {
        for (auto&& consumer : _consumers) {
            bool paused = true;
            if (consumer.second._isPaused.compare_exchange_strong(paused, !paused)) {
                consumer.second._consumer->resume();
            }
        }
    }
    else {
        bool paused = true;
        ConsumerTopicEntry& consumerTopicEntry = findConsumer(topic)->second;
        if (consumerTopicEntry._isPaused.compare_exchange_strong(paused, !paused)) {
            consumerTopicEntry._consumer->resume();
        }
    }
}

void ConsumerManagerImpl::subscribe(const std::string& topic,
                                    cppkafka::TopicPartitionList partitionList)
{
    ConsumerTopicEntry& topicEntry = findConsumer(topic)->second;
    if (topicEntry._isSubscribed) {
        throw ConsumerException(topic, "Already subscribed");
    }
    //subscribe or statically assign partitions to this consumer
    topicEntry._isSubscribed = true;
    topicEntry._setOffsetsOnStart = true;
    if (!partitionList.empty()) {
        //Overwrite the initial assignment
        topicEntry._configuration.assignInitialPartitions(topicEntry._configuration.getPartitionStrategy(),
                                                          std::move(partitionList));
    }
    if (topicEntry._configuration.getPartitionStrategy() == PartitionStrategy::Static) {
        cppkafka::TopicPartitionList partitions = topicEntry._configuration.getInitialPartitionAssignment();
        for (auto& partition : partitions) {
            partition.set_offset(RD_KAFKA_OFFSET_STORED);
        }
        topicEntry._consumer->assign(partitions);
    }
    else {
        topicEntry._consumer->subscribe({topic});
    }
}

void ConsumerManagerImpl::unsubscribe(const std::string& topic)
{
    if (topic.empty()) {
        for (auto&& consumer : _consumers) {
            if (consumer.second._isSubscribed) {
                if (consumer.second._configuration.getPartitionStrategy() == PartitionStrategy::Static) {
                    consumer.second._consumer->unassign();
                }
                else {
                    consumer.second._consumer->unsubscribe();
                }
                consumer.second._isSubscribed = false;
            }
        }
    }
    else {
        ConsumerTopicEntry& consumerTopicEntry = findConsumer(topic)->second;
        if (consumerTopicEntry._isSubscribed) {
            if (consumerTopicEntry._configuration.getPartitionStrategy() == PartitionStrategy::Static) {
                consumerTopicEntry._consumer->unassign();
            }
            else {
                consumerTopicEntry._consumer->unsubscribe();
            }
        }
    }
}

cppkafka::Error ConsumerManagerImpl::commit(const cppkafka::TopicPartition& topicPartition,
                                            const void* opaque,
                                            bool forceSync)
{
    auto it = _consumers.find(topicPartition.get_topic());
    if (it == _consumers.end()) {
        return RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART;
    }
    return commitImpl(it->second, cppkafka::TopicPartitionList{topicPartition}, opaque, forceSync);
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
                if (headPartition.get_partition() == RD_KAFKA_PARTITION_UA) {
                    //commit the current assignment
                    entry._committer->commit();
                }
                else {
                    entry._committer->commit(topicPartitions);
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
        unsubscribe({});
    }
}

void ConsumerManagerImpl::poll()
{
    auto now = std::chrono::steady_clock::now();
    for (auto&& entry : _consumers) {
        if (!entry.second._isSubscribed) {
            continue; //we are no longer subscribed here
        }
        // Adjust throttling if necessary
        adjustThrottling(entry.second, now);
        bool doPoll = !entry.second._pollFuture || (entry.second._pollFuture->waitFor(std::chrono::milliseconds(0)) == std::future_status::ready);
        if (doPoll) {
            // Round-robin
            if (entry.second._roundRobin) {
                entry.second._pollFuture =
                    _dispatcher.postFirst((int)quantum::IQueue::QueueId::Any, true, pollCoro, entry.second)->
                                then(processorCoro, entry.second)->
                                end();
            }
            else {
                // Batch
                entry.second._pollFuture =
                  _dispatcher.post((int)quantum::IQueue::QueueId::Any, true, pollBatchCoro, entry.second);
            }
        }
    }
}

void ConsumerManagerImpl::errorCallback2(
                        ConsumerTopicEntry& topicEntry,
                        cppkafka::KafkaHandleBase& handle,
                        int error,
                        const std::string& reason)
{
    errorCallback(topicEntry, handle, error, reason, nullptr);
}

void ConsumerManagerImpl::errorCallback(
                        ConsumerTopicEntry& topicEntry,
                        cppkafka::KafkaHandleBase& handle,
                        int error,
                        const std::string& reason,
                        const cppkafka::Message* message)
{
    const void* errorOpaque = topicEntry._configuration.getErrorCallbackOpaque() ?
                              topicEntry._configuration.getErrorCallbackOpaque() : message;
    cppkafka::CallbackInvoker<Callbacks::ErrorCallback>
        ("error", topicEntry._configuration.getErrorCallback(), &handle)
            (makeMetadata(topicEntry), cppkafka::Error((rd_kafka_resp_err_t)error), reason, const_cast<void*>(errorOpaque));
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
    report(topicEntry, cppkafka::LogLevel::LogErr, error.get_error(), "Failed to commit offset.", nullptr);
    return ((error.get_error() != RD_KAFKA_RESP_ERR_OFFSET_OUT_OF_RANGE) &&
            (error.get_error() != RD_KAFKA_RESP_ERR_OFFSET_METADATA_TOO_LARGE) &&
            (error.get_error() != RD_KAFKA_RESP_ERR_INVALID_COMMIT_OFFSET_SIZE));
}

bool ConsumerManagerImpl::preprocessorCallback(
                        ConsumerTopicEntry& topicEntry,
                        cppkafka::TopicPartition hint)
{
    // Check if we have opaque data
    return cppkafka::CallbackInvoker<Callbacks::PreprocessorCallback>
        ("preprocessor", topicEntry._configuration.getPreprocessorCallback(), topicEntry._consumer.get())
            (hint);
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
        !topicEntry._configuration.getInitialPartitionAssignment().empty() &&
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
    topicEntry._configuration.assignInitialPartitions(strategy, topicPartitions); //overwrite original if any
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
    topicEntry._configuration.assignInitialPartitions(strategy, {}); //clear assignment
    cppkafka::CallbackInvoker<Callbacks::RebalanceCallback>
        ("revocation", topicEntry._configuration.getRebalanceCallback(), topicEntry._consumer.get())
            (makeMetadata(topicEntry), cppkafka::Error(RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS), const_cast<cppkafka::TopicPartitionList&>(topicPartitions));
}

void ConsumerManagerImpl::rebalanceErrorCallback(
                        ConsumerTopicEntry& topicEntry,
                        cppkafka::Error error)
{
    cppkafka::TopicPartitionList partitions;
    cppkafka::CallbackInvoker<Callbacks::RebalanceCallback>
        ("rebalance", topicEntry._configuration.getRebalanceCallback(), topicEntry._consumer.get())
            (makeMetadata(topicEntry), error, partitions);
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
                    int error,
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

void ConsumerManagerImpl::setConsumerBatchSize(size_t size)
{
    _batchSize = size;
}

size_t ConsumerManagerImpl::getConsumerBatchSize() const
{
    return _batchSize;
}

std::vector<cppkafka::Message> ConsumerManagerImpl::messageBatchReceiveTask(ConsumerTopicEntry& entry)
{
    try {
        if (entry._pollTimeout.count() == (int)TimerValues::Disabled) {
            return entry._consumer->poll_batch(entry._batchSize);
        }
        else {
            return entry._consumer->poll_batch(entry._batchSize, entry._pollTimeout);
        }
    }
    catch (const std::exception& ex) {
        exceptionHandler(ex, entry);
    }
    return {};
}

int ConsumerManagerImpl::messageRoundRobinReceiveTask(quantum::ThreadPromise<MessageContainer>::Ptr promise,
                                                      ConsumerTopicEntry& entry)
{
    try {
        int batchSize = entry._batchSize;
        std::chrono::milliseconds timeout = (entry._pollTimeout.count() == (int)TimerValues::Disabled) ?
                                             entry._consumer->get_timeout() : entry._pollTimeout;
        std::chrono::milliseconds timeoutPerRound = std::max(timeout/(int64_t)entry._batchSize,
                                                             entry._roundRobinMinPollTimeout);
        while (batchSize--) {
            if (!entry._isSubscribed) {
                break;
            }
            cppkafka::Message message = entry._roundRobin->poll(timeoutPerRound);
            if (message) {
                promise->push(std::move(message));
            }
        }
    }
    catch (const std::exception& ex) {
        exceptionHandler(ex, entry);
    }
    return promise->closeBuffer();
}

ConsumerManagerImpl::DeserializedMessage
ConsumerManagerImpl::deserializeMessage(ConsumerTopicEntry& entry,
                                        const cppkafka::Message& kafkaMessage)
{
    DeserializerError de;
    if (kafkaMessage.get_error()) {
        de._error = kafkaMessage.get_error();
        de._source |= (uint8_t)DeserializerError::Source::Kafka;
        return DeserializedMessage(boost::any(), boost::any(), HeaderPack{}, de);
    }
    
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
                report(entry, cppkafka::LogLevel::LogWarning, 0, ex.what(), &kafkaMessage);
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

ConsumerManagerImpl::DeserializedMessage
ConsumerManagerImpl::deserializeCoro(quantum::VoidContextPtr ctx,
                                     ConsumerTopicEntry& entry,
                                     const cppkafka::Message& kafkaMessage)
{
    bool skip = false;
    if (entry._preprocessorCallback && entry._preprocess) {
        if (entry._preprocessorThread == ThreadType::IO) {
            // Call the preprocessor callback
            skip = ctx->template postAsyncIo(preprocessorTask, entry, kafkaMessage)->get(ctx);
        }
        else {
            //run in this coroutine
            skip = entry._preprocessorCallback(cppkafka::TopicPartition(kafkaMessage.get_topic(),
                                               kafkaMessage.get_partition(),
                                               kafkaMessage.get_offset()));
        }
        if (skip) {
            //return immediately and skip de-serializing
            DeserializedMessage dm;
            std::get<3>(dm)._error = RD_KAFKA_RESP_ERR__BAD_MSG;
            std::get<3>(dm)._source |= (uint8_t)DeserializerError::Source::Preprocessor;
            return dm;
        }
    }
    // Deserialize the message
    return deserializeMessage(entry, kafkaMessage);
}

std::vector<bool> ConsumerManagerImpl::executePreprocessorCallbacks(
                                              quantum::VoidContextPtr ctx,
                                              ConsumerTopicEntry& entry,
                                              const std::vector<cppkafka::Message>& messages)
{
    // Preprocessor IO threads
    const size_t callbackThreadRangeSize = entry._receiveCallbackThreadRange.second -
                                           entry._receiveCallbackThreadRange.first + 1;
    int numPerBatch = messages.size()/callbackThreadRangeSize;
    int remainder = messages.size()%callbackThreadRangeSize;
    std::vector<bool> skipMessages(messages.size(), false);
    std::vector<quantum::CoroFuturePtr<int>> futures;
    futures.reserve(callbackThreadRangeSize);
    auto inputIt = messages.cbegin();
    size_t batchIndex = 0;
    
    // Run the preprocessor callbacks in batches
    for (int i = 0; i < (int)callbackThreadRangeSize; ++i) {
        //get the begin and end iterators for each batch
        size_t batchSize = (i < remainder) ? numPerBatch + 1 : numPerBatch;
        if (batchSize == 0) {
            break; //nothing to do
        }
        int ioQueueId = i + entry._receiveCallbackThreadRange.first;
        futures.emplace_back(ctx->postAsyncIo(ioQueueId, false,
            [&entry, &skipMessages, batchIndex, batchSize, inputIt]() mutable ->int
        {
            for (size_t j = batchIndex;
                 j < (batchIndex + batchSize) && entry._preprocess;
                 ++j, ++inputIt) {
                if (!inputIt->get_error()) {
                    skipMessages[j] = entry._preprocessorCallback(cppkafka::TopicPartition(inputIt->get_topic(),
                                                                                           inputIt->get_partition(),
                                                                                           inputIt->get_offset()));
                }
            }
            return 0;
        }));
        // Advance index and iterator
        batchIndex += batchSize;
        std::advance(inputIt, batchSize);
    }
    
    //Wait on preprocessor stage to finish
    for (auto&& f : futures) {
        f->wait(ctx);
    }
    return skipMessages;
}

std::vector<ConsumerManagerImpl::DeserializedMessage>
ConsumerManagerImpl::deserializeBatchCoro(quantum::VoidContextPtr ctx,
                                          ConsumerTopicEntry& entry,
                                          const std::vector<cppkafka::Message>& messages)
{
    std::vector<bool> skipMessages;
    if (entry._configuration.getPreprocessorCallback() && entry._preprocess) {
        if (entry._preprocessorThread == ThreadType::IO) {
            skipMessages = executePreprocessorCallbacks(ctx, entry, messages);
        }
        else {
            skipMessages.resize(messages.size(), false);
        }
    }
    
    // Reset values
    size_t numCoros = entry._coroQueueIdRangeForAny.second - entry._coroQueueIdRangeForAny.first + 1;
    int numPerBatch = messages.size()/numCoros;
    int remainder = messages.size()%numCoros;
    std::vector<DeserializedMessage> deserializedMessages(messages.size()); //pre-allocate default constructed messages
    std::vector<quantum::CoroContextPtr<int>> futures;
    futures.reserve(numCoros);
    auto inputIt = messages.cbegin();
    size_t batchIndex = 0;
    
    // Post unto all the coroutine threads.
    for (int h = 0, i = entry._coroQueueIdRangeForAny.first; i <= entry._coroQueueIdRangeForAny.second; ++i, ++h) {
        //get the begin and end iterators for each batch
        size_t batchSize = (h < remainder) ? numPerBatch + 1 : numPerBatch;
        if (batchSize == 0) {
            break; //nothing to do
        }
        futures.emplace_back(ctx->post(i, false,
            [&entry, &deserializedMessages, &skipMessages, inputIt, batchIndex, batchSize]
            (quantum::CoroContextPtr<int>) mutable ->int
        {
            for (size_t j = batchIndex; j < (batchIndex + batchSize); ++j, ++inputIt) {
                if (entry._configuration.getPreprocessorCallback() &&
                    entry._preprocess &&
                    (entry._preprocessorThread == ThreadType::Coro) &&
                    !inputIt->get_error()) {
                    // Run the preprocessor on the coroutine thread
                    skipMessages[j] = entry._preprocessorCallback(cppkafka::TopicPartition(inputIt->get_topic(),
                                                                  inputIt->get_partition(),
                                                                  inputIt->get_offset()));
                }
                if (!skipMessages.empty() && skipMessages[j]) {
                    // Set error and mark source as preprocessor
                    std::get<3>(deserializedMessages[j])._error = RD_KAFKA_RESP_ERR__BAD_MSG;
                    std::get<3>(deserializedMessages[j])._source |= (uint8_t)DeserializerError::Source::Preprocessor;
                }
                else {
                    // Deserialize message
                    deserializedMessages[j] = deserializeMessage(entry, *inputIt);
                }
            }
            return 0;
        }));
        // Advance index and iterator
        batchIndex += batchSize;
        std::advance(inputIt, batchSize);
    }
    
    //Wait on deserialize stage to finish
    for (auto&& f : futures) {
        f->wait(ctx);
    }
    return deserializedMessages;
}

int ConsumerManagerImpl::invokeReceiver(ConsumerTopicEntry& entry,
                                        cppkafka::Message&& kafkaMessage,
                                        DeserializedMessage&& deserializedMessage)
{
    cppkafka::CallbackInvoker<Receiver>("receiver", entry._configuration.getTypeErasedReceiver(), entry._consumer.get())
          (*entry._committer,
           entry._offsets,
           std::move(kafkaMessage), //kafka raw message
           std::get<0>(std::move(deserializedMessage)), //key
           std::get<1>(std::move(deserializedMessage)), //payload
           std::get<2>(std::move(deserializedMessage)), //headers
           std::get<3>(std::move(deserializedMessage)), //error
           makeOffsetPersistSettings(entry));
    return 0;
}

int ConsumerManagerImpl::receiverTask(ConsumerTopicEntry& entry,
                                      cppkafka::Message&& kafkaMessage,
                                      DeserializedMessage&& deserializedMessage)
{
    return invokeReceiver(entry, std::move(kafkaMessage), std::move(deserializedMessage));
}

std::deque<ConsumerManagerImpl::MessageTuple>
ConsumerManagerImpl::pollCoro(quantum::VoidContextPtr ctx,
                              ConsumerTopicEntry& entry)
{
    std::deque<MessageTuple> messageQueue;
    try {
        // Start the IO task to get messages in a round-robin way
        quantum::CoroFuture<MessageContainer>::Ptr future = ctx->postAsyncIo(
            (int)quantum::IQueue::QueueId::Any, true, messageRoundRobinReceiveTask, entry);
            
        // Receive all messages from kafka and deserialize in parallel
        bool isBufferClosed = false;
        while (!isBufferClosed) {
            cppkafka::Message message = future->pull(ctx, isBufferClosed);
            if (!isBufferClosed) {
                messageQueue.emplace_back(MessageTuple(std::move(message), nullptr));
                MessageTuple& tuple = messageQueue.back();
                if (!std::get<0>(tuple).get_error()) { // check if message has any errors
                    std::get<1>(tuple) = ctx->post(deserializeCoro, entry, std::get<0>(tuple));
                }
            }
        }
    }
    catch (const std::exception& ex) {
        exceptionHandler(ex, entry);
    }
    // Pass the message queue to the processor coroutine
    return messageQueue;
}

void ConsumerManagerImpl::processMessageBatchOnIoThreads(quantum::VoidContextPtr ctx,
                                                         ConsumerTopicEntry& entry,
                                                         std::vector<cppkafka::Message>&& raw,
                                                         std::vector<DeserializedMessage>&& deserializedMessages)
{
    const std::pair<int,int>& threadRange = entry._receiveCallbackThreadRange;
    const int callbackThreadRangeSize = threadRange.second - threadRange.first + 1;
    if (callbackThreadRangeSize > 1) {
        // split the messages into io queues
        std::vector<ReceivedBatch> partitions(callbackThreadRangeSize);
        size_t rawIx = 0;
        for (auto&& deserializedMessage : deserializedMessages) {
            cppkafka::Message& rawMessage = raw[rawIx++];
            if (rawIx > raw.size()) {
                throw ConsumerException(entry._configuration._topic, "Invalid message index");
            }
            // Find out on which IO thread we should process this message
            const int ioQueue = mapPartitionToQueue(rawMessage.get_partition(), threadRange);
            partitions[ioQueue - threadRange.first]
                .emplace_back(std::make_tuple(std::move(rawMessage), std::move(deserializedMessage)));
        }
        if (rawIx != raw.size()) {
            throw ConsumerException(entry._configuration._topic, "Not all messages were processed");
        }
        // invoke batch jobs for the partitioned messages
        std::vector<quantum::ICoroFuture<int>::Ptr> ioFutures;
        ioFutures.reserve(partitions.size());
        for (size_t queueIx = 0; queueIx < partitions.size(); ++queueIx) {
            const int ioQueue = queueIx + threadRange.first;
            quantum::ICoroFuture<int>::Ptr future =
                ctx->postAsyncIo(ioQueue,
                                  false,
                                  receiverMultipleBatchesTask,
                                  entry,
                                  std::move(partitions[queueIx]));
            if (entry._receiveCallbackExec == ExecMode::Sync) {
                ioFutures.push_back(future);
            }
        }
        // wait until all the batches are processed
        for (auto&& c: ioFutures) {
            c->get(ctx);
        }
    }
    else {
        // optimization: no need to spend time on message distribution for a single io queue
        quantum::ICoroFuture<int>::Ptr future =
            ctx->postAsyncIo(threadRange.first,
                              false,
                              receiverSingleBatchTask,
                              entry,
                              std::move(raw),
                              std::move(deserializedMessages));
        if (entry._receiveCallbackExec == ExecMode::Sync) {
            future->get(ctx);
        }
    }
}

int ConsumerManagerImpl::pollBatchCoro(quantum::VoidContextPtr ctx,
                                       ConsumerTopicEntry& entry)
{
    try{
        // get the messages from the prefetched future, or
        std::vector<cppkafka::Message> raw;
        if (entry._batchPrefetch)
        {
            if (entry._messagePrefetchFuture) {
                //get the pre-fetched batch
                raw = entry._messagePrefetchFuture->get(ctx);
            }
            // start pre-fetching for the next batch
            entry._messagePrefetchFuture = ctx->postAsyncIo
                ((int)quantum::IQueue::QueueId::Any, true, messageBatchReceiveTask, entry);
        }
        else {
            raw = ctx->postAsyncIo((int)quantum::IQueue::QueueId::Any,
                                   true,
                                   messageBatchReceiveTask,
                                   entry)->get(ctx);
        }
        
        if (!raw.empty()) {
            // process messages
            std::vector<DeserializedMessage> deserializedMessages = ctx->post(deserializeBatchCoro, entry, raw)
                                                                       ->get(ctx);
    
            if (entry._receiverThread == ThreadType::IO) {
                processMessageBatchOnIoThreads(ctx, entry, std::move(raw), std::move(deserializedMessages));
            }
            else {
                invokeSingleBatchReceiver(entry, std::move(raw), std::move(deserializedMessages));
            }
        }
        return 0;
    }
    catch (const std::exception& ex) {
        exceptionHandler(ex, entry);
        return -1;
    }
}
                                  
int ConsumerManagerImpl::receiverMultipleBatchesTask(ConsumerTopicEntry& entry,
                                                     ReceivedBatch&& messageBatch)
{
    for (auto&& messageTuple : messageBatch) {
        cppkafka::CallbackInvoker<Receiver>("receiver", entry._configuration.getTypeErasedReceiver(), entry._consumer.get())
            (*entry._committer,
             entry._offsets,
             std::get<0>(std::move(messageTuple)), //kafka raw message
             std::get<0>(std::get<1>(std::move(messageTuple))), //key
             std::get<1>(std::get<1>(std::move(messageTuple))), //payload
             std::get<2>(std::get<1>(std::move(messageTuple))), //headers
             std::get<3>(std::get<1>(std::move(messageTuple))), //error
             makeOffsetPersistSettings(entry));
    }
    return 0;
}

int ConsumerManagerImpl::invokeSingleBatchReceiver(ConsumerTopicEntry& entry,
                                                   std::vector<cppkafka::Message>&& rawMessages,
                                                   std::vector<DeserializedMessage>&& deserializedMessages)
{
    size_t rawIx = 0;
    for (auto&& deserializedMessage : deserializedMessages) {
        cppkafka::Message& rawMessage = rawMessages[rawIx++];
        if (rawIx > rawMessages.size()) {
            throw ConsumerException(entry._configuration._topic, "Invalid message index");
        }
        cppkafka::CallbackInvoker<Receiver>("receiver", entry._configuration.getTypeErasedReceiver(), entry._consumer.get())
            (*entry._committer,
             entry._offsets,
             std::move(rawMessage), //kafka raw message
             std::get<0>(std::move(deserializedMessage)), //key
             std::get<1>(std::move(deserializedMessage)), //payload
             std::get<2>(std::move(deserializedMessage)), //headers
             std::get<3>(std::move(deserializedMessage)), //error
             makeOffsetPersistSettings(entry));
    }
    if (rawIx != rawMessages.size()) {
        throw ConsumerException(entry._configuration._topic, "Not all messages were processed");
    }
    return 0;
}

int ConsumerManagerImpl::receiverSingleBatchTask(ConsumerTopicEntry& entry,
                                                 std::vector<cppkafka::Message>&& rawMessages,
                                                 std::vector<DeserializedMessage>&& deserializedMessages)
{
    return invokeSingleBatchReceiver(entry, std::move(rawMessages), std::move(deserializedMessages));
}

bool ConsumerManagerImpl::preprocessorTask(ConsumerTopicEntry& entry,
                                           const cppkafka::Message& kafkaMessage)
{
    return entry._preprocessorCallback(cppkafka::TopicPartition(kafkaMessage.get_topic(),
                                       kafkaMessage.get_partition(),
                                       kafkaMessage.get_offset()));
}

int ConsumerManagerImpl::processorCoro(quantum::VoidContextPtr ctx,
                                       ConsumerTopicEntry& entry)
{
    //Get the polled messages from the previous stage (non-blocking)
    std::deque<MessageTuple> messageQueue = ctx->getPrev<std::deque<MessageTuple>>();
    
    // Enqueue all messages and wait for completion
    for (auto& messageTuple : messageQueue) {
        try {
            auto &message = std::get<0>(messageTuple);
            auto deserializedFuture = std::get<1>(messageTuple);
            if (entry._receiverThread == ThreadType::IO) {
                // Find out on which IO thread we should process this message
                int ioQueue = mapPartitionToQueue(message.get_partition(), entry._receiveCallbackThreadRange);
                // Post and wait until delivered
                quantum::ICoroFuture<int>::Ptr future =
                    ctx->postAsyncIo(ioQueue,
                                     false,
                                     receiverTask,
                                     entry,
                                     std::move(message),
                                     deserializedFuture ? deserializedFuture->get(ctx) : DeserializedMessage());
                if (entry._receiveCallbackExec == ExecMode::Sync) {
                    future->get(ctx);
                }
            }
            else {
                //call serially on this coroutine
                invokeReceiver(entry,
                               std::move(message),
                               deserializedFuture ? deserializedFuture->get(ctx) : DeserializedMessage());
            }
        }
        catch (const std::exception& ex) {
            exceptionHandler(ex, entry);
        }
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
                                             const std::pair<int,int>& range)
{
    return (partition % (range.second - range.first + 1)) + range.first;
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

}
}
