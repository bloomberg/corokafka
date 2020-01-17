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
#include <corokafka/impl/corokafka_producer_manager_impl.h>
#include <corokafka/corokafka_utils.h>
#include <corokafka/corokafka_exception.h>
#include <cstdlib>
#include <functional>
#include <cmath>
#include <librdkafka/rdkafka.h>
#include <functional>

namespace Bloomberg {
namespace corokafka {

//===========================================================================================
//                               class ProducerManagerImpl
//===========================================================================================

using namespace std::placeholders;

ProducerManagerImpl::ProducerManagerImpl(quantum::Dispatcher& dispatcher,
                                         const ConnectorConfiguration& connectorConfiguration,
                                         const ConfigMap& configs) :
    _dispatcher(dispatcher)
{
    //Create a producer for each topic and apply the appropriate configuration
    for (const auto& entry : configs) {
        // Process each configuration
        auto it = _producers.emplace(entry.first, ProducerTopicEntry(nullptr, connectorConfiguration, entry.second));
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

ProducerManagerImpl::ProducerManagerImpl(quantum::Dispatcher& dispatcher,
                                         const ConnectorConfiguration& connectorConfiguration,
                                         ConfigMap&& configs) :
    _dispatcher(dispatcher),
    _shutdownInitiated{0}
{
    // Create a producer for each topic and apply the appropriate configuration
    for (auto&& entry : configs) {
        // Process each configuration
        auto it = _producers.emplace(entry.first, ProducerTopicEntry(nullptr, connectorConfiguration, std::move(entry.second)));
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

ProducerManagerImpl::~ProducerManagerImpl()
{
    shutdown();
}

void ProducerManagerImpl::produceMessage(const ProducerTopicEntry& entry,
                                         const ProducerMessageBuilder<ByteArray>& builder)
{
    if (entry._waitForAcksTimeout.count() == (int)TimerValues::Disabled) {
        //async send
        entry._producer->produce(builder);
    }
    else if (entry._waitForAcksTimeout.count() == (int)TimerValues::Unlimited) {
        //full blocking sync send
        entry._producer->sync_produce(builder);
    }
    else {
        //block until ack arrives or timeout
        entry._producer->produce(builder);
        entry._producer->wait_for_acks(entry._waitForAcksTimeout);
    }
}

void ProducerManagerImpl::flush(const ProducerTopicEntry& entry)
{
    if ((entry._flushWaitForAcksTimeout.count() == (int)TimerValues::Disabled) &&
        !entry._forceSyncFlush) {
        //async flush
        entry._producer->async_flush();
    }
    else if (entry._flushWaitForAcksTimeout.count() == (int)TimerValues::Unlimited) {
        //full blocking flush
        entry._producer->flush(entry._preserveMessageOrder);
    }
    else {
        //block until ack arrives or timeout
        entry._producer->flush(entry._flushWaitForAcksTimeout, entry._preserveMessageOrder);
    }
}

void ProducerManagerImpl::setup(const std::string& topic, ProducerTopicEntry& topicEntry)
{
    const Configuration::OptionList& rdKafkaOptions = topicEntry._configuration.getOptions(Configuration::OptionType::RdKafka);
    const Configuration::OptionList& rdKafkaTopicOptions = topicEntry._configuration.getTopicOptions(Configuration::OptionType::RdKafka);
    const Configuration::OptionList& internalOptions = topicEntry._configuration.getOptions(Configuration::OptionType::Internal);
    
    auto extract = [&topic, &internalOptions](const std::string& name, auto& value)->bool {
        return ProducerConfiguration::extract(name)(topic, Configuration::findOption(name, internalOptions), &value);
    };
    
    //Validate config
    const cppkafka::ConfigurationOption* brokerList =
        Configuration::findOption(Configuration::RdKafkaOptions::metadataBrokerList, rdKafkaOptions);
    if (!brokerList) {
        throw InvalidOptionException(topic, Configuration::RdKafkaOptions::metadataBrokerList, "Missing");
    }
    
    //Set the rdkafka configuration options
    cppkafka::Configuration kafkaConfig(rdKafkaOptions);
    cppkafka::TopicConfiguration topicConfig(rdKafkaTopicOptions);
    
    extract(ProducerConfiguration::Options::autoThrottle, topicEntry._throttleControl.autoThrottle());
    extract(ProducerConfiguration::Options::autoThrottleMultiplier, topicEntry._throttleControl.throttleMultiplier());
    
    //Set the global callbacks
    if (topicEntry._configuration.getPartitionerCallback()) {
        auto partitionerFunc = std::bind(&partitionerCallback, std::ref(topicEntry), _1, _2, _3);
        topicConfig.set_partitioner_callback(partitionerFunc);
    }
    
    if (topicEntry._configuration.getErrorCallback()) {
        auto errorFunc = std::bind(&errorCallback2, std::ref(topicEntry), _1, _2, _3);
        kafkaConfig.set_error_callback(errorFunc);
    }
    
    if (topicEntry._configuration.getThrottleCallback() || topicEntry._throttleControl.autoThrottle()) {
        auto throttleFunc = std::bind(&throttleCallback, std::ref(topicEntry), _1, _2, _3, _4);
        kafkaConfig.set_throttle_callback(throttleFunc);
    }
    
    if (topicEntry._configuration.getLogCallback()) {
        auto logFunc = std::bind(&logCallback, std::ref(topicEntry), _1, _2, _3, _4);
        kafkaConfig.set_log_callback(logFunc);
    }
    
    if (topicEntry._configuration.getStatsCallback()) {
        auto statsFunc = std::bind(&statsCallback, std::ref(topicEntry), _1, _2);
        kafkaConfig.set_stats_callback(statsFunc);
    }
    
    extract(ProducerConfiguration::Options::maxQueueLength, topicEntry._maxQueueLength);
    extract(ProducerConfiguration::Options::preserveMessageOrder, topicEntry._preserveMessageOrder);
    if (topicEntry._preserveMessageOrder) {
        // change rdkafka settings
        kafkaConfig.set(Configuration::RdKafkaOptions::maxInFlight, 1);  //limit one request at a time
    }
    
    kafkaConfig.set_default_topic_configuration(topicConfig);
    
    //=======================================================================================
    //DO NOT UPDATE ANY KAFKA CONFIG OPTIONS BELOW THIS POINT SINCE THE PRODUCER MAKES A COPY
    //=======================================================================================
    
    //Create a buffered producer
    topicEntry._producer.reset(new cppkafka::BufferedProducer<ByteArray>(kafkaConfig));
    
    //Set internal config options
    size_t internalProducerRetries = 0;
    if (extract(ProducerConfiguration::Options::retries, internalProducerRetries)) {
        topicEntry._producer->set_max_number_retries(internalProducerRetries);
    }
    
    if (extract(ProducerConfiguration::Options::payloadPolicy, topicEntry._payloadPolicy)) {
        topicEntry._producer->get_producer().set_payload_policy(topicEntry._payloadPolicy);
    }
    
    if (extract(ProducerConfiguration::Options::logLevel, topicEntry._logLevel)) {
        topicEntry._producer->get_producer().set_log_level(topicEntry._logLevel);
    }
    
    std::chrono::milliseconds timeoutMs;
    if (extract(ProducerConfiguration::Options::timeoutMs, timeoutMs)) {
        topicEntry._producer->get_producer().set_timeout(timeoutMs);
    }
    
    extract(ProducerConfiguration::Options::waitForAcksTimeoutMs, topicEntry._waitForAcksTimeout);
    extract(ProducerConfiguration::Options::flushWaitForAcksTimeoutMs, topicEntry._flushWaitForAcksTimeout);
    
    // Set the buffered producer callbacks
    auto produceSuccessFunc = std::bind(
        &produceSuccessCallback, std::ref(topicEntry), _1);
    topicEntry._producer->set_produce_success_callback(produceSuccessFunc);
        
    auto produceTerminationFunc = std::bind(
        &produceTerminationCallback, std::ref(topicEntry), _1);
    topicEntry._producer->set_produce_termination_callback(produceTerminationFunc);
    
    auto flushFailureFunc = std::bind(
        &flushFailureCallback, std::ref(topicEntry), _1, _2);
    topicEntry._producer->set_flush_failure_callback(flushFailureFunc);

    auto flushTerminationFunc = std::bind(
        &flushTerminationCallback, std::ref(topicEntry), _1, _2);
    topicEntry._producer->set_flush_termination_callback(flushTerminationFunc);
    
    extract(ProducerConfiguration::Options::queueFullNotification, topicEntry._queueFullNotification);
    if (topicEntry._configuration.getQueueFullCallback()) {
        if ((topicEntry._queueFullNotification == QueueFullNotification::EdgeTriggered) ||
            (topicEntry._queueFullNotification == QueueFullNotification::EachOccurence)) {
            topicEntry._producer->set_queue_full_notification(ProducerType::QueueFullNotification::EachOccurence);
        }
        else {
            topicEntry._producer->set_queue_full_notification(ProducerType::QueueFullNotification::OncePerMessage);
        }
        auto queueFullFunc = std::bind(&queueFullCallback, std::ref(topicEntry), _1);
        topicEntry._producer->set_queue_full_callback(queueFullFunc);
    }
}

ProducerMetadata ProducerManagerImpl::getMetadata(const std::string& topic)
{
    auto it = findProducer(topic);
    return makeMetadata(it->second);
}

const ProducerConfiguration& ProducerManagerImpl::getConfiguration(const std::string& topic) const
{
    auto it = findProducer(topic);
    return it->second._configuration;
}

std::vector<std::string> ProducerManagerImpl::getTopics() const
{
    std::vector<std::string> topics;
    topics.reserve(_producers.size());
    for (const auto& entry : _producers) {
        topics.emplace_back(entry.first);
    }
    return topics;
}

void ProducerManagerImpl::waitForAcks(const std::string& topic,
                                      std::chrono::milliseconds timeout)
{
    auto it = findProducer(topic);
    if (timeout.count() <= 0) {
        it->second._producer->wait_for_acks();
    }
    else {
        it->second._producer->wait_for_acks(timeout);
    }
}

void ProducerManagerImpl::shutdown()
{
    if (!_shutdownInitiated.test_and_set())
    {
        {
            std::unique_lock<std::mutex> lock(_messageQueueMutex);
            _shuttingDown = true;
        }
        _emptyCondition.notify_one(); //awake the posting thread
        // wait for all pending flushes to complete
        for (auto&& entry : _producers) {
            flush(entry.second);
        }
    }
}

void ProducerManagerImpl::poll()
{
    auto now = std::chrono::steady_clock::now();
    for (auto&& entry : _producers) {
        // Adjust throttling if necessary
        adjustThrottling(entry.second, now);
        bool doPoll = !entry.second._pollFuture ||
                      (entry.second._pollFuture->waitFor(std::chrono::milliseconds(0)) == std::future_status::ready);
        if (doPoll) {
            // Start a poll task
            entry.second._pollFuture = _dispatcher.postAsyncIo((int)quantum::IQueue::QueueId::Any,
                                                               true,
                                                               pollTask,
                                                               entry.second);
        }
    }
}

void ProducerManagerImpl::post()
{
    std::deque<MessageFuture> tempQueue;
    {
        std::unique_lock<std::mutex> lock(_messageQueueMutex);
        _emptyCondition.wait(lock, [this]()->bool { return !_messageQueue.empty() || _shuttingDown; });
        if (_shuttingDown) {
            return;
        }
        std::swap(tempQueue, _messageQueue);
    }
    // Send serialized messages
    int numIoThreads = _dispatcher.getNumIoThreads();
    for (auto&& future : tempQueue) {
        try {
            BuilderTuple builderTuple(future->get()); //may throw FutureException if serialization failed
            ProducerTopicEntry& topicEntry = *std::get<0>(builderTuple);
            ProducerMessageBuilder<ByteArray>& builder = std::get<1>(builderTuple);
            if (builder.topic().empty()) {
                //Failed to serialize
                continue; // Skip this message
            }
            if (_messageFanout) {
                if (topicEntry._topicHash == 0) {
                    topicEntry._topicHash = std::hash<std::string>()(topicEntry._configuration.getTopic());
                }
                _dispatcher.postAsyncIo((int)topicEntry._topicHash % numIoThreads,
                                        false,
                                        produceTask,
                                        topicEntry,
                                        std::move(builder));
            }
            else {
                produceTaskSync(topicEntry, std::get<1>(builderTuple));
            }
        }
        catch (...) {
            // Skip this message
        }
    }
}

void ProducerManagerImpl::resetQueueFullTrigger(const std::string& topic)
{
    auto it = findProducer(topic);
    it->second._queueFullTrigger = true;
}

void ProducerManagerImpl::enableMessageFanout(bool value)
{
    _messageFanout = value;
}

// Callbacks
int32_t ProducerManagerImpl::partitionerCallback(
                        ProducerTopicEntry& topicEntry,
                        const cppkafka::Topic&,
                        const cppkafka::Buffer& key,
                        int32_t partitionCount)
{
    return cppkafka::CallbackInvoker<Callbacks::PartitionerCallback>
        ("partition", topicEntry._configuration.getPartitionerCallback(), &topicEntry._producer->get_producer())
            (makeMetadata(topicEntry), key, partitionCount);
}

void ProducerManagerImpl::errorCallback2(
                        ProducerTopicEntry& topicEntry,
                        cppkafka::KafkaHandleBase& handle,
                        int error,
                        const std::string& reason)
{
    errorCallback(topicEntry, handle, error, reason, nullptr);
}

void ProducerManagerImpl::errorCallback(
                        ProducerTopicEntry& topicEntry,
                        cppkafka::KafkaHandleBase& handle,
                        int error,
                        const std::string& reason,
                        const void* sendOpaque)
{
    const void* errorOpaque = topicEntry._configuration.getErrorCallbackOpaque() ?
                              topicEntry._configuration.getErrorCallbackOpaque() : sendOpaque;
    cppkafka::CallbackInvoker<Callbacks::ErrorCallback>
        ("error", topicEntry._configuration.getErrorCallback(), &handle)
            (makeMetadata(topicEntry), cppkafka::Error((rd_kafka_resp_err_t)error), reason, const_cast<void*>(errorOpaque));
}

void ProducerManagerImpl::throttleCallback(
                        ProducerTopicEntry& topicEntry,
                        cppkafka::KafkaHandleBase& handle,
                        const std::string& brokerName,
                        int32_t brokerId,
                        std::chrono::milliseconds throttleDuration)
{
    if (topicEntry._throttleControl.autoThrottle()) {
        //calculate throttle status
        ThrottleControl::Status status = topicEntry._throttleControl.handleThrottleCallback(throttleDuration);
        if (status == ThrottleControl::Status::On) {
            //Get partition metadata
            handle.pause(topicEntry._configuration.getTopic());
        }
        else if (status == ThrottleControl::Status::Off) {
            handle.resume(topicEntry._configuration.getTopic());
            topicEntry._forceSyncFlush = true;
        }
    }
    cppkafka::CallbackInvoker<Callbacks::ThrottleCallback>
        ("throttle", topicEntry._configuration.getThrottleCallback(), &handle)
            (makeMetadata(topicEntry), brokerName, brokerId, throttleDuration);
}

void ProducerManagerImpl::logCallback(
                        ProducerTopicEntry& topicEntry,
                        cppkafka::KafkaHandleBase& handle,
                        int level,
                        const std::string& facility,
                        const std::string& message)
{
    cppkafka::CallbackInvoker<Callbacks::LogCallback>
        ("log", topicEntry._configuration.getLogCallback(), &handle)
            (makeMetadata(topicEntry), static_cast<cppkafka::LogLevel>(level), facility, message);
}

void ProducerManagerImpl::statsCallback(
                        ProducerTopicEntry& topicEntry,
                        cppkafka::KafkaHandleBase& handle,
                        const std::string& json)
{
    cppkafka::CallbackInvoker<Callbacks::StatsCallback>
        ("stats", topicEntry._configuration.getStatsCallback(), &handle)
            (makeMetadata(topicEntry), json);
}

void ProducerManagerImpl::produceSuccessCallback(
                        ProducerTopicEntry& topicEntry,
                        const cppkafka::Message& kafkaMessage)
{
    const void* opaque = setPackedOpaqueFuture(kafkaMessage); //set future
    cppkafka::CallbackInvoker<Callbacks::DeliveryReportCallback>
        ("produce success", topicEntry._configuration.getDeliveryReportCallback(), &topicEntry._producer->get_producer())
            (makeMetadata(topicEntry), SentMessage(kafkaMessage, opaque));
}

void ProducerManagerImpl::produceTerminationCallback(
                        ProducerTopicEntry& topicEntry,
                        const cppkafka::Message& kafkaMessage)
{
    const void* opaque = setPackedOpaqueFuture(kafkaMessage); //set future
    cppkafka::CallbackInvoker<Callbacks::DeliveryReportCallback>
        ("produce failure", topicEntry._configuration.getDeliveryReportCallback(), &topicEntry._producer->get_producer())
            (makeMetadata(topicEntry), SentMessage(kafkaMessage, opaque));
}

bool ProducerManagerImpl::flushFailureCallback(
                        ProducerTopicEntry& topicEntry,
                        const cppkafka::MessageBuilder& builder,
                        cppkafka::Error error)
{
    std::ostringstream oss;
    oss << "Failed to enqueue message " << builder;
    report(topicEntry, cppkafka::LogLevel::LogErr, error.get_error(), oss.str(), nullptr);
    return ((error.get_error() != RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE) &&
            (error.get_error() != RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION) &&
            (error.get_error() != RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC));
}

void ProducerManagerImpl::flushTerminationCallback(
                        ProducerTopicEntry& topicEntry,
                        const cppkafka::MessageBuilder& builder,
                        cppkafka::Error error)
{
    const void* opaque = setPackedOpaqueFuture(builder, error); //set future
    cppkafka::CallbackInvoker<Callbacks::DeliveryReportCallback>
        ("produce failure", topicEntry._configuration.getDeliveryReportCallback(), &topicEntry._producer->get_producer())
            (makeMetadata(topicEntry), SentMessage(builder, error, opaque));
}

void ProducerManagerImpl::queueFullCallback(ProducerTopicEntry& topicEntry,
                                            const cppkafka::MessageBuilder& builder)
{
    bool notify = (topicEntry._queueFullNotification != QueueFullNotification::EdgeTriggered) || topicEntry._queueFullTrigger;
    if (notify) {
        topicEntry._queueFullTrigger = false; //clear trigger
        cppkafka::CallbackInvoker<Callbacks::QueueFullCallback>
        ("queue full", topicEntry._configuration.getQueueFullCallback(), &topicEntry._producer->get_producer())
            (makeMetadata(topicEntry), SentMessage(builder,
                                                   RD_KAFKA_RESP_ERR__QUEUE_FULL,
                                                   static_cast<PackedOpaque*>(builder.user_data())->first));
    }
}

void ProducerManagerImpl::report(ProducerTopicEntry& topicEntry,
                                 cppkafka::LogLevel level,
                                 int error,
                                 const std::string& reason,
                                 const void* sendOpaque)
{
    if (error) {
        errorCallback(topicEntry, topicEntry._producer->get_producer(), error, reason, sendOpaque);
    }
    if (topicEntry._logLevel >= level) {
        logCallback(topicEntry, topicEntry._producer->get_producer(), (int)level, "corokafka", reason);
    }
}

void ProducerManagerImpl::adjustThrottling(ProducerTopicEntry& topicEntry,
                                           const std::chrono::steady_clock::time_point& now)
{
    if (topicEntry._throttleControl.reduceThrottling(now)) {
        topicEntry._producer->get_producer().resume(topicEntry._configuration.getTopic());
        topicEntry._forceSyncFlush = true;
    }
}

int ProducerManagerImpl::pollTask(ProducerTopicEntry& entry)
{
    try {
        flush(entry);
        return 0;
    }
    catch (const std::exception& ex) {
        exceptionHandler(ex, entry);
        return -1;
    }
}

int ProducerManagerImpl::produceTask(ProducerTopicEntry& entry,
                                     ProducerMessageBuilder<ByteArray>&& builder)
{
    try {
        if (entry._preserveMessageOrder) {
            // Save to local buffer so we can flush synchronously later
            entry._producer->add_message(builder);
        }
        else {
            // Post directly to internal RdKafka outbound queue
            produceMessage(entry, builder);
        }
        return 0;
    }
    catch (const std::exception& ex) {
        exceptionHandler(ex, entry);
        return -1;
    }
}

int ProducerManagerImpl::produceTaskSync(ProducerTopicEntry& entry,
                                         const ProducerMessageBuilder<ByteArray>& builder)
{
    try {
        if (entry._preserveMessageOrder) {
            // Save to local buffer so we can flush synchronously later
            entry._producer->add_message(builder);
        }
        else {
            // Post directly to internal RdKafka outbound queue
            produceMessage(entry, builder);
        }
        return 0;
    }
    catch(const std::exception& ex) {
        exceptionHandler(ex, entry);
        return -1;
    }
}

void ProducerManagerImpl::exceptionHandler(const std::exception& ex,
                                           const ProducerTopicEntry& topicEntry)
{
    handleException(ex, makeMetadata(topicEntry), topicEntry._configuration, topicEntry._logLevel);
}

ProducerMetadata ProducerManagerImpl::makeMetadata(const ProducerTopicEntry& topicEntry)
{
    return ProducerMetadata(topicEntry._configuration.getTopic(), topicEntry._producer.get());
}

const void* ProducerManagerImpl::setPackedOpaqueFuture(const cppkafka::Message& kafkaMessage)
{
    std::unique_ptr<PackedOpaque> packedOpaque(static_cast<PackedOpaque*>(kafkaMessage.get_user_data()));
    packedOpaque->second.set(DeliveryReport(cppkafka::TopicPartition(kafkaMessage.get_topic(),
                                                                     kafkaMessage.get_partition(),
                                                                     kafkaMessage.get_offset()),
                                            kafkaMessage.get_payload().get_size(),
                                            kafkaMessage.get_error(),
                                            packedOpaque->first));
    return packedOpaque->first;
}

const void* ProducerManagerImpl::setPackedOpaqueFuture(const cppkafka::MessageBuilder& builder, cppkafka::Error error)
{
    std::unique_ptr<PackedOpaque> packedOpaque(static_cast<PackedOpaque*>(builder.user_data()));
    packedOpaque->second.set(DeliveryReport(cppkafka::TopicPartition(builder.topic(),
                                                                     builder.partition(),
                                                                     cppkafka::TopicPartition::OFFSET_INVALID),
                                            0,
                                            error,
                                            packedOpaque->first));
    return packedOpaque->first;
}

ProducerManagerImpl::Producers::iterator
ProducerManagerImpl::findProducer(const std::string& topic)
{
    auto it = _producers.find(topic);
    if (it == _producers.end()) {
        throw TopicException(topic, "Not found");
    }
    return it;
}

ProducerManagerImpl::Producers::const_iterator
ProducerManagerImpl::findProducer(const std::string& topic) const
{
    auto it = _producers.find(topic);
    if (it == _producers.end()) {
        throw TopicException(topic, "Not found");
    }
    return it;
}

}
}
