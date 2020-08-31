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
                                         const ConfigMap& configs,
                                         std::atomic_bool& interrupt) :
    _dispatcher(dispatcher),
    _connectorConfiguration(connectorConfiguration),
    _shutdownInitiated{false},
    _shutdownIoWaitTimeoutMs(connectorConfiguration.getShutdownIoWaitTimeout())
{
    //Create a producer for each topic and apply the appropriate configuration
    for (const auto& entry : configs) {
        // Process each configuration
        auto it = _producers.emplace(entry.first, ProducerTopicEntry(nullptr,
                                                                     _connectorConfiguration,
                                                                     entry.second,
                                                                     interrupt,
                                                                     _dispatcher.getNumIoThreads()));
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
                                         ConfigMap&& configs,
                                         std::atomic_bool& interrupt) :
    _dispatcher(dispatcher),
    _connectorConfiguration(connectorConfiguration),
    _shutdownInitiated{false},
    _shutdownIoWaitTimeoutMs(connectorConfiguration.getShutdownIoWaitTimeout())
{
    // Create a producer for each topic and apply the appropriate configuration
    for (auto&& entry : configs) {
        // Process each configuration
        auto it = _producers.emplace(entry.first, ProducerTopicEntry(nullptr,
                                                                     _connectorConfiguration,
                                                                     std::move(entry.second),
                                                                     interrupt,
                                                                     _dispatcher.getNumIoThreads()));
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

bool ProducerManagerImpl::isFlushNonBlocking(const ProducerTopicEntry& topicEntry)
{
    return ((topicEntry._flushWaitForAcksTimeout == std::chrono::milliseconds::zero()) &&
             !topicEntry._forceSyncFlush &&
             !topicEntry._preserveMessageOrder);
}

void ProducerManagerImpl::flush(const ProducerTopicEntry& entry)
{
    if (entry._forceSyncFlush) {
        //block indefinitely
        entry._producer->flush(entry._preserveMessageOrder);
    }
    else {
        //block until ack arrives or timeout
        entry._producer->flush(entry._flushWaitForAcksTimeout, entry._preserveMessageOrder);
    }
}

void ProducerManagerImpl::setup(const std::string& topic, ProducerTopicEntry& topicEntry)
{
    const Configuration::OptionList& rdKafkaOptions = topicEntry._configuration.
            getOptions(Configuration::OptionType::RdKafka);
    const Configuration::OptionList& rdKafkaTopicOptions = topicEntry._configuration.
            getTopicOptions(Configuration::OptionType::RdKafka);
    
    auto extract = [&](const std::string &name, auto &value) -> bool
    {
        return ProducerConfiguration::extract(name)(topic, topicEntry._configuration.getOption(name), &value);
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
    
    std::pair<int, int> prev = topicEntry._syncProducerThreadRange;
    extract(ProducerConfiguration::Options::syncProducerThreadRangeLow, topicEntry._syncProducerThreadRange.first);
    if ((topicEntry._syncProducerThreadRange.first < prev.first) ||
        (topicEntry._syncProducerThreadRange.first > prev.second)) {
        throw InvalidOptionException(topic, ProducerConfiguration::Options::syncProducerThreadRangeLow, std::to_string(topicEntry._syncProducerThreadRange.first));
    }
    extract(ProducerConfiguration::Options::syncProducerThreadRangeHigh, topicEntry._syncProducerThreadRange.second);
    if ((topicEntry._syncProducerThreadRange.second < topicEntry._syncProducerThreadRange.first) ||
        (topicEntry._syncProducerThreadRange.first > prev.second)) {
        throw InvalidOptionException(topic, ProducerConfiguration::Options::syncProducerThreadRangeHigh, std::to_string(topicEntry._syncProducerThreadRange.second));
    }
    
    extract(ProducerConfiguration::Options::autoThrottle, topicEntry._throttleControl.autoThrottle());
    extract(ProducerConfiguration::Options::autoThrottleMultiplier, topicEntry._throttleControl.throttleMultiplier());
    
    //Set the global callbacks
    if (topicEntry._configuration.getPartitionerCallback()) {
        auto partitionerFunc = std::bind(&partitionerCallback, std::ref(topicEntry), _1, _2, _3);
        topicConfig.set_partitioner_callback(partitionerFunc);
    }
    
    if (topicEntry._configuration.getErrorCallback()) {
        auto errorFunc = std::bind(&errorCallbackInternal, std::ref(topicEntry), _1, _2, _3);
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
    bool isIdempotent = false;
    auto option = topicEntry._configuration.getOption(Configuration::RdKafkaOptions::enableIdempotence);
    if (option) {
        isIdempotent = Configuration::extractBooleanValue(topic,
                                                          Configuration::RdKafkaOptions::enableIdempotence,
                                                          *option);
    }
    if (topicEntry._preserveMessageOrder && !isIdempotent) {
        // change rdkafka settings (if not specified by application)
        int maxInFlight;
        if (!topicEntry._configuration.getOption(Configuration::RdKafkaOptions::maxInFlight)) {
            kafkaConfig.set(Configuration::RdKafkaOptions::maxInFlight, 1);  //limit one request at a time
        }
        int messageRetries;
        if (!topicEntry._configuration.getOption(Configuration::RdKafkaOptions::messageSendMaxRetries)) {
            kafkaConfig.set(Configuration::RdKafkaOptions::messageSendMaxRetries, 0);  //don't retry since corokafka will
        }
    }
    
    kafkaConfig.set_default_topic_configuration(topicConfig);
    
    //=======================================================================================
    //DO NOT UPDATE ANY KAFKA CONFIG OPTIONS BELOW THIS POINT SINCE THE PRODUCER MAKES A COPY
    //=======================================================================================
    
    //Create a buffered producer
    topicEntry._producer = std::make_unique<cppkafka::BufferedProducer<ByteArray>>(kafkaConfig);
    
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
    
    //Set the broker timeout
    topicEntry._brokerTimeout = timeoutMs;
    extract(TopicConfiguration::Options::brokerTimeoutMs, topicEntry._brokerTimeout);
    
    extract(ProducerConfiguration::Options::waitForAcksTimeoutMs, topicEntry._waitForAcksTimeout);
    if (topicEntry._waitForAcksTimeout.count() == EnumValue(TimerValues::Disabled)) {
        topicEntry._waitForAcksTimeout = topicEntry._producer->get_producer().get_timeout();
    }
    
    extract(ProducerConfiguration::Options::flushWaitForAcksTimeoutMs, topicEntry._flushWaitForAcksTimeout);
    if (topicEntry._flushWaitForAcksTimeout.count() == EnumValue(TimerValues::Disabled)) {
        topicEntry._flushWaitForAcksTimeout = topicEntry._producer->get_producer().get_timeout();
    }
    
    extract(ProducerConfiguration::Options::pollIoThreadId, topicEntry._pollIoThreadId);
    if (static_cast<int>(topicEntry._pollIoThreadId) >= _dispatcher.getNumIoThreads()) {
        throw InvalidOptionException(topic, ProducerConfiguration::Options::pollIoThreadId, "Value is out of bounds");
    }
    
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

DeliveryReport ProducerManagerImpl::send()
{
    return {};
}

quantum::GenericFuture<DeliveryReport> ProducerManagerImpl::post()
{
    return {};
}

bool ProducerManagerImpl::waitForAcks(const std::string& topic)
{
    auto it = findProducer(topic);
    return it->second._producer->wait_for_acks(it->second._waitForAcksTimeout);
}

bool ProducerManagerImpl::waitForAcks(const std::string& topic,
                                      std::chrono::milliseconds timeout)
{
    return findProducer(topic)->second._producer->wait_for_acks(timeout);
}

void ProducerManagerImpl::shutdown()
{
    if (!_shutdownInitiated.test_and_set())
    {
        //see if we have any IO left
        auto sleepInterval = _shutdownIoWaitTimeoutMs/10;
        while (_shutdownIoWaitTimeoutMs.count() > 0) {
            bool hasIo = false;
            for (auto &&entry : _producers) {
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

void ProducerManagerImpl::poll()
{
    auto now = std::chrono::steady_clock::now();
    for (auto&& producer : _producers) {
        ProducerTopicEntry& entry = producer.second;
        // Adjust throttling if necessary
        adjustThrottling(entry, now);
        bool pollMessages = !entry._pollFuture ||
                            (entry._pollFuture->waitFor(std::chrono::milliseconds::zero()) == std::future_status::ready);
        if (pollMessages) {
            if (entry._producer->get_buffer_size() == 0) {
                // No messages left so just poll for events (this won't congest the IO threads)
                try {
                    entry._producer->wait_for_acks(std::chrono::milliseconds::zero());
                }
                catch (const std::exception& ex) {
                    exceptionHandler(ex, entry);
                }
            }
            else if (isFlushNonBlocking(entry)) {
                //poll directly on this thread
                flushTask(entry, IoTracker{});
            }
            else {
                // Start a poll task and flush messages out synchronously
                entry._pollFuture = _dispatcher.postAsyncIo(static_cast<int>(entry._pollIoThreadId),
                                                            false,
                                                            flushTask,
                                                            entry,
                                                            IoTracker(entry._ioTracker));
            }
        }
    }
}

void ProducerManagerImpl::pollEnd()
{
    for (auto&& entry : _producers) {
        if (entry.second._pollFuture) {
            //wait until poll task has completed
            entry.second._pollFuture->get();
        }
    }
}

void ProducerManagerImpl::resetQueueFullTrigger(const std::string& topic)
{
    auto it = findProducer(topic);
    it->second._queueFullTrigger = true;
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

void ProducerManagerImpl::errorCallbackInternal(
                        ProducerTopicEntry& topicEntry,
                        cppkafka::KafkaHandleBase& handle,
                        int error,
                        const std::string& reason)
{
    //call application error callback
    errorCallback(topicEntry, handle, (rd_kafka_resp_err_t)error, reason, nullptr);
}

void ProducerManagerImpl::errorCallback(
                        ProducerTopicEntry& topicEntry,
                        cppkafka::KafkaHandleBase& handle,
                        cppkafka::Error error,
                        const std::string& reason,
                        const void* sendOpaque)
{
    const void* errorOpaque = topicEntry._configuration.getErrorCallbackOpaque() ?
                              topicEntry._configuration.getErrorCallbackOpaque() : sendOpaque;
    cppkafka::CallbackInvoker<Callbacks::ErrorCallback>
        ("error", topicEntry._configuration.getErrorCallback(), &handle)
            (makeMetadata(topicEntry), error, reason, const_cast<void*>(errorOpaque));
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
    report(topicEntry, cppkafka::LogLevel::LogErr, error, oss.str(), nullptr);
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
                                                   static_cast<PackedOpaque*>(builder.user_data())->_opaque));
    }
}

void ProducerManagerImpl::report(ProducerTopicEntry& topicEntry,
                                 cppkafka::LogLevel level,
                                 cppkafka::Error error,
                                 const std::string& reason,
                                 const void* sendOpaque)
{
    if (error) {
        errorCallback(topicEntry, topicEntry._producer->get_producer(), error, reason, sendOpaque);
    }
    if (topicEntry._logLevel >= level) {
        logCallback(topicEntry, topicEntry._producer->get_producer(), EnumValue(level), "corokafka", reason);
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

int ProducerManagerImpl::flushTask(ProducerTopicEntry& entry, IoTracker)
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

void ProducerManagerImpl::exceptionHandler(const std::exception& ex,
                                           const ProducerTopicEntry& topicEntry)
{
    handleException(ex, makeMetadata(topicEntry), topicEntry._configuration, topicEntry._logLevel);
}

ProducerMetadata ProducerManagerImpl::makeMetadata(const ProducerTopicEntry& topicEntry)
{
    return ProducerMetadata(topicEntry._configuration.getTopic(),
                            topicEntry._producer.get(),
                            topicEntry._brokerTimeout);
}

const void* ProducerManagerImpl::setPackedOpaqueFuture(const cppkafka::Message& kafkaMessage)
{
    std::unique_ptr<PackedOpaque> packedOpaque(static_cast<PackedOpaque*>(kafkaMessage.get_user_data()));
    try {
        DeliveryReport dr;
        dr.topicPartition({kafkaMessage.get_topic(), kafkaMessage.get_partition(), kafkaMessage.get_offset()}).
            error(kafkaMessage.get_error()).
            numBytesWritten(kafkaMessage.get_payload().get_size()).
            opaque(packedOpaque->_opaque);
#if (RD_KAFKA_VERSION >= RD_KAFKA_MESSAGE_STATUS_SUPPORT_VERSION)
        dr.status(kafkaMessage.get_status());
#endif
#if RD_KAFKA_VERSION >= RD_KAFKA_MESSAGE_LATENCY_SUPPORT_VERSION
        dr.latency(kafkaMessage.get_latency());
#endif
        packedOpaque->_deliveryReportPromise.set(std::move(dr));
    }
    catch (...) {}
    return packedOpaque->_opaque;
}

const void* ProducerManagerImpl::setPackedOpaqueFuture(const cppkafka::MessageBuilder& builder,
                                                       cppkafka::Error error)
{
    std::unique_ptr<PackedOpaque> packedOpaque(static_cast<PackedOpaque*>(builder.user_data()));
    try {
        DeliveryReport dr;
        dr.topicPartition({builder.topic(), builder.partition()}).
            error(error).
            opaque(packedOpaque->_opaque);
        packedOpaque->_deliveryReportPromise.set(std::move(dr));
    }
    catch (...) {}
    return packedOpaque->_opaque;
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

int32_t ProducerManagerImpl::mapKeyToQueue(const uint8_t* obj,
                                           size_t len,
                                           const ProducerTopicEntry& topicEntry)
{
    //FNV-1a hash
    static const long long prime = 1099511628211;
    static const long long offset = 0xcbf29ce484222325;
    unsigned long long result = offset;
    for (auto it = obj; it != obj+len; ++it) {
        result = result ^ *it;
        result = result * prime;
    }
    int rangeSize = topicEntry._syncProducerThreadRange.second - topicEntry._syncProducerThreadRange.first + 1;
    if ((topicEntry._numIoThreads == rangeSize) && !topicEntry._preserveMessageOrder) {
        return EnumValue(quantum::IQueue::QueueId::Any); //pick any queue
    }
    //select specific queue
    return (result % rangeSize) + topicEntry._syncProducerThreadRange.first;
}

}
}
