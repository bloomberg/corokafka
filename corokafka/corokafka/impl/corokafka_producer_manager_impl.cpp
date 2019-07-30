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
        setup(entry.first, it.first->second);
    }
}

ProducerManagerImpl::ProducerManagerImpl(quantum::Dispatcher& dispatcher,
                                         const ConnectorConfiguration& connectorConfiguration,
                                         ConfigMap&& configs) :
    _dispatcher(dispatcher),
    _shutdownInitiated(ATOMIC_FLAG_INIT)
{
    // Create a producer for each topic and apply the appropriate configuration
    for (auto&& entry : configs) {
        // Process each configuration
        auto it = _producers.emplace(entry.first, ProducerTopicEntry(nullptr, connectorConfiguration, std::move(entry.second)));
        setup(entry.first, it.first->second);
    }
}

ProducerManagerImpl::~ProducerManagerImpl()
{
    shutdown();
}

void ProducerManagerImpl::produceMessage(const ProducerTopicEntry& entry,
                                         const ConcreteMessageBuilder<ByteArray>& builder)
{
    if (entry._waitForAcks) {
        if (entry._waitForAcksTimeout.count() == -1) {
            //full blocking sync send
            entry._producer->sync_produce(builder);
        }
        else {
            //block until ack arrives or timeout
            entry._producer->produce(builder);
            entry._producer->wait_for_acks(entry._waitForAcksTimeout);
        }
    }
    else {
        //async send
        entry._producer->produce(builder);
    }
}

void ProducerManagerImpl::flush(const ProducerTopicEntry& entry)
{
    if (entry._flushWaitForAcks) {
        if (entry._flushWaitForAcksTimeout.count() == -1) {
            //full blocking flush
            entry._producer->flush();
        }
        else {
            //block until ack arrives or timeout
            entry._producer->flush(entry._flushWaitForAcksTimeout);
        }
    }
    else {
        //async flush
        entry._producer->async_flush();
    }
}

void ProducerManagerImpl::setup(const std::string& topic, ProducerTopicEntry& topicEntry)
{
    //Validate config
    const ConfigurationOption* brokerList =
        Configuration::findConfigOption("metadata.broker.list", topicEntry._configuration.getConfiguration());
    if (!brokerList) {
        throw std::runtime_error("Broker not specified");
    }
    
    if (!topicEntry._configuration.getKeySerializer() || !topicEntry._configuration.getPayloadSerializer()) {
        throw std::runtime_error(std::string("Serializer callback not specified for topic producer: ") + topic);
    }
    
    //Set the rdkafka configuration options
    KafkaConfiguration kafkaConfig(topicEntry._configuration.getConfiguration());
    TopicConfiguration topicConfig(topicEntry._configuration.getTopicConfiguration());
    
    const ConfigurationOption* autoThrottle =
        Configuration::findConfigOption("internal.producer.auto.throttle", topicEntry._configuration.getInternalConfiguration());
    if (autoThrottle) {
        topicEntry._throttleControl.autoThrottle() = StringEqualCompare()(autoThrottle->get_value(), "true");
    }
    
    const ConfigurationOption* throttleMultiplier =
        Configuration::findConfigOption("internal.producer.auto.throttle.multiplier", topicEntry._configuration.getInternalConfiguration());
    if (throttleMultiplier) {
        topicEntry._throttleControl.throttleMultiplier() = std::stol(throttleMultiplier->get_value());
    }
    
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
    
    const ConfigurationOption* maxQueueLength =
        Configuration::findConfigOption("internal.producer.max.queue.length", topicEntry._configuration.getInternalConfiguration());
    if (maxQueueLength) {
        topicEntry._maxQueueLength = std::stoll(maxQueueLength->get_value());
    }
    
    const ConfigurationOption* preserveMessageOrder =
        Configuration::findConfigOption("internal.producer.preserve.message.order", topicEntry._configuration.getInternalConfiguration());
    if (preserveMessageOrder) {
        topicEntry._preserveMessageOrder = StringEqualCompare()(preserveMessageOrder->get_value(), "true");
        if (topicEntry._preserveMessageOrder) {
            // change rdkafka settings
            kafkaConfig.set("max.in.flight", 1);  //limit one request at a time
        }
    }
    
    size_t internalProducerRetries = 0;
    const ConfigurationOption* numRetriesOption =
        Configuration::findConfigOption("internal.producer.retries", topicEntry._configuration.getInternalConfiguration());
    if (numRetriesOption) {
        internalProducerRetries = std::stoll(numRetriesOption->get_value());
    }
    
    const ConfigurationOption* skipUnknownHeaders =
        Configuration::findConfigOption("internal.producer.skip.unknown.headers", topicEntry._configuration.getInternalConfiguration());
    if (skipUnknownHeaders) {
        topicEntry._skipUnknownHeaders = StringEqualCompare()(skipUnknownHeaders->get_value(), "true");
    }
    
    kafkaConfig.set_default_topic_configuration(topicConfig);
    
    //Create a buffered producer
    topicEntry._producer.reset(new BufferedProducer<ByteArray>(kafkaConfig));
    
    //Set internal config options
    if (numRetriesOption) {
        topicEntry._producer->set_max_number_retries(internalProducerRetries);
    }
    
    const ConfigurationOption* payloadPolicy =
        Configuration::findConfigOption("internal.producer.payload.policy", topicEntry._configuration.getInternalConfiguration());
    if (payloadPolicy) {
        if (StringEqualCompare()(payloadPolicy->get_value(), "passthrough")) {
            topicEntry._payloadPolicy = Producer::PayloadPolicy::PASSTHROUGH_PAYLOAD;
        }
        else if (StringEqualCompare()(payloadPolicy->get_value(), "copy")) {
            topicEntry._payloadPolicy = Producer::PayloadPolicy::COPY_PAYLOAD;
        }
        else {
            throw std::runtime_error("Unknown internal.producer.payload.policy");
        }
        topicEntry._producer->get_producer().set_payload_policy(topicEntry._payloadPolicy);
    }
    
    const ConfigurationOption* logLevel =
        Configuration::findConfigOption("internal.producer.log.level", topicEntry._configuration.getInternalConfiguration());
    if (logLevel) {
        LogLevel level = logLevelFromString(logLevel->get_value());
        topicEntry._producer->get_producer().set_log_level(level);
        topicEntry._logLevel = level;
    }
    
    const ConfigurationOption* pollTimeout =
        Configuration::findConfigOption("internal.producer.timeout.ms", topicEntry._configuration.getInternalConfiguration());
    if (pollTimeout) {
        topicEntry._producer->get_producer().set_timeout(std::chrono::milliseconds(std::stoll(pollTimeout->get_value())));
    }
    
    const ConfigurationOption* waitForAcks =
        Configuration::findConfigOption("internal.producer.wait.for.acks", topicEntry._configuration.getInternalConfiguration());
    if (waitForAcks) {
        topicEntry._waitForAcks = StringEqualCompare()(waitForAcks->get_value(), "true");
    }
    
    const ConfigurationOption* flushWaitForAcks =
        Configuration::findConfigOption("internal.producer.flush.wait.for.acks", topicEntry._configuration.getInternalConfiguration());
    if (flushWaitForAcks) {
        topicEntry._flushWaitForAcks = StringEqualCompare()(flushWaitForAcks->get_value(), "true");
    }
    
    const ConfigurationOption* waitForAcksTimeout =
        Configuration::findConfigOption("internal.producer.wait.for.acks.timeout.ms", topicEntry._configuration.getInternalConfiguration());
    if (waitForAcksTimeout) {
        topicEntry._waitForAcksTimeout = std::chrono::milliseconds(std::stoll(waitForAcksTimeout->get_value()));
        if (topicEntry._waitForAcksTimeout.count() < -1) {
            throw std::runtime_error("Invalid setting for internal.producer.wait.for.acks.timeout.ms. Must be >= -1");
        }
    }
    
    const ConfigurationOption* flushWaitForAcksTimeout =
        Configuration::findConfigOption("internal.producer.flush.wait.for.acks.timeout.ms", topicEntry._configuration.getInternalConfiguration());
    if (flushWaitForAcksTimeout) {
        topicEntry._flushWaitForAcksTimeout = std::chrono::milliseconds(std::stoll(flushWaitForAcksTimeout->get_value()));
        if (topicEntry._flushWaitForAcksTimeout.count() < -1) {
            throw std::runtime_error("Invalid setting for internal.producer.flush.wait.for.acks.timeout.ms. Must be >= -1");
        }
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
    
    if (topicEntry._configuration.getQueueFullCallback()) {
        const ConfigurationOption* queueFullNotification =
        Configuration::findConfigOption("internal.producer.queue.full.notification", topicEntry._configuration.getInternalConfiguration());
        if (queueFullNotification) {
            if (StringEqualCompare()(queueFullNotification->get_value(), "edgeTriggered")) {
                topicEntry._queueFullNotification = QueueFullNotification::EdgeTriggered;
                topicEntry._producer->set_queue_full_notification(ProducerType::QueueFullNotification::EachOccurence);
            }
            else if (StringEqualCompare()(queueFullNotification->get_value(), "oncePerMessage")) {
                topicEntry._queueFullNotification = QueueFullNotification::OncePerMessage;
                topicEntry._producer->set_queue_full_notification(ProducerType::QueueFullNotification::OncePerMessage);
            }
            else if (StringEqualCompare()(queueFullNotification->get_value(), "eachOccurence")) {
                topicEntry._queueFullNotification = QueueFullNotification::EachOccurence;
                topicEntry._producer->set_queue_full_notification(ProducerType::QueueFullNotification::EachOccurence);
            }
            else {
                throw std::runtime_error("Unknown internal.producer.queue.full.notification");
            }
        }
        else { //default
            topicEntry._producer->set_queue_full_notification(ProducerType::QueueFullNotification::OncePerMessage);
        }
        auto queueFullFunc = std::bind(&queueFullCallback, std::ref(topicEntry), _1);
        topicEntry._producer->set_queue_full_callback(queueFullFunc);
    }
}

ProducerMetadata ProducerManagerImpl::getMetadata(const std::string& topic)
{
    return makeMetadata(_producers.at(topic));
}

const ProducerConfiguration& ProducerManagerImpl::getConfiguration(const std::string& topic) const
{
    return _producers.at(topic)._configuration;
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

void ProducerManagerImpl::waitForAcks(const std::string& topic)
{
    _producers.at(topic)._producer->wait_for_acks();
}

void ProducerManagerImpl::waitForAcks(const std::string& topic,
                                      std::chrono::milliseconds timeout)
{
    _producers.at(topic)._producer->wait_for_acks(timeout);
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
            entry.second._producer->flush();
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
            BuilderTuple builderTuple(future->get()); //may throw FutureException if deserialization failed
            ProducerTopicEntry& topicEntry = *std::get<0>(builderTuple);
            if (_messageFanout) {
                if (topicEntry._topicHash == 0) {
                    topicEntry._topicHash = std::hash<std::string>()(topicEntry._configuration.getTopic());
                }
                _dispatcher.postAsyncIo((int)topicEntry._topicHash % numIoThreads,
                                        false, produceTask, topicEntry, std::move(*std::get<1>(builderTuple)));
            }
            else {
                produceTaskSync(topicEntry, *std::get<1>(builderTuple));
            }
        }
        catch (...) {
            // Skip this message
        }
    }
}

void ProducerManagerImpl::resetQueueFullTrigger(const std::string& topic)
{
    _producers.at(topic)._queueFullTrigger = true;
}

void ProducerManagerImpl::enableMessageFanout(bool value)
{
    _messageFanout = value;
}

// Callbacks
int32_t ProducerManagerImpl::partitionerCallback(
                        ProducerTopicEntry& topicEntry,
                        const Topic&,
                        const Buffer& key,
                        int32_t partitionCount)
{
    return CallbackInvoker<Callbacks::PartitionerCallback>
        ("partition", topicEntry._configuration.getPartitionerCallback(), &topicEntry._producer->get_producer())
            (makeMetadata(topicEntry), key, partitionCount);
}

void ProducerManagerImpl::errorCallback2(
                        ProducerTopicEntry& topicEntry,
                        KafkaHandleBase& handle,
                        int error,
                        const std::string& reason)
{
    errorCallback(topicEntry, handle, error, reason, nullptr);
}

void ProducerManagerImpl::errorCallback(
                        ProducerTopicEntry& topicEntry,
                        KafkaHandleBase& handle,
                        int error,
                        const std::string& reason,
                        void* opaque)
{
    CallbackInvoker<Callbacks::ErrorCallback>
        ("error", topicEntry._configuration.getErrorCallback(), &handle)
            (makeMetadata(topicEntry), Error((rd_kafka_resp_err_t)error), reason, opaque);
}

void ProducerManagerImpl::throttleCallback(
                        ProducerTopicEntry& topicEntry,
                        KafkaHandleBase& handle,
                        const std::string& brokerName,
                        int32_t brokerId,
                        std::chrono::milliseconds throttleDuration)
{
    if (topicEntry._throttleControl.autoThrottle()) {
        //calculate throttle periods
        ThrottleControl::Status status = topicEntry._throttleControl.handleThrottleCallback(throttleDuration);
        if (status._on) {
            //Get partition metadata
            handle.pause(topicEntry._configuration.getTopic());
        }
        else if (status._off) {
            handle.resume(topicEntry._configuration.getTopic());
            topicEntry._forceSyncFlush = true;
        }
    }
    CallbackInvoker<Callbacks::ThrottleCallback>
        ("throttle", topicEntry._configuration.getThrottleCallback(), &handle)
            (makeMetadata(topicEntry), brokerName, brokerId, throttleDuration);
}

void ProducerManagerImpl::logCallback(
                        ProducerTopicEntry& topicEntry,
                        KafkaHandleBase& handle,
                        int level,
                        const std::string& facility,
                        const std::string& message)
{
    CallbackInvoker<Callbacks::LogCallback>
        ("log", topicEntry._configuration.getLogCallback(), &handle)
            (makeMetadata(topicEntry), static_cast<LogLevel>(level), facility, message);
}

void ProducerManagerImpl::statsCallback(
                        ProducerTopicEntry& topicEntry,
                        KafkaHandleBase& handle,
                        const std::string& json)
{
    CallbackInvoker<Callbacks::StatsCallback>
        ("stats", topicEntry._configuration.getStatsCallback(), &handle)
            (makeMetadata(topicEntry), json);
}

void ProducerManagerImpl::produceSuccessCallback(
                        ProducerTopicEntry& topicEntry,
                        const Message& kafkaMessage)
{
    void* opaque = setPackedOpaqueFuture(kafkaMessage); //set future
    CallbackInvoker<Callbacks::DeliveryReportCallback>
        ("produce success", topicEntry._configuration.getDeliveryReportCallback(), &topicEntry._producer->get_producer())
            (makeMetadata(topicEntry), SentMessage(kafkaMessage, opaque));
}

void ProducerManagerImpl::produceTerminationCallback(
                        ProducerTopicEntry& topicEntry,
                        const Message& kafkaMessage)
{
    void* opaque = setPackedOpaqueFuture(kafkaMessage); //set future
    CallbackInvoker<Callbacks::DeliveryReportCallback>
        ("produce failure", topicEntry._configuration.getDeliveryReportCallback(), &topicEntry._producer->get_producer())
            (makeMetadata(topicEntry), SentMessage(kafkaMessage, opaque));
}

bool ProducerManagerImpl::flushFailureCallback(
                        ProducerTopicEntry& topicEntry,
                        const MessageBuilder& builder,
                        Error error)
{
    std::ostringstream oss;
    oss << "Failed to enqueue message " << builder;
    report(topicEntry, LogLevel::LogErr, error.get_error(), oss.str(), nullptr);
    return ((error.get_error() != RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE) &&
            (error.get_error() != RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION) &&
            (error.get_error() != RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC));
}

void ProducerManagerImpl::flushTerminationCallback(
                        ProducerTopicEntry& topicEntry,
                        const MessageBuilder& builder,
                        Error error)
{
    void* opaque = setPackedOpaqueFuture(builder, error); //set future
    CallbackInvoker<Callbacks::DeliveryReportCallback>
        ("produce failure", topicEntry._configuration.getDeliveryReportCallback(), &topicEntry._producer->get_producer())
            (makeMetadata(topicEntry), SentMessage(builder, error, opaque));
}

void ProducerManagerImpl::queueFullCallback(ProducerTopicEntry& topicEntry,
                                            const MessageBuilder& builder)
{
    bool notify = (topicEntry._queueFullNotification != QueueFullNotification::EdgeTriggered) || topicEntry._queueFullTrigger;
    if (notify) {
        topicEntry._queueFullTrigger = false; //clear trigger
        CallbackInvoker<Callbacks::QueueFullCallback>
        ("queue full", topicEntry._configuration.getQueueFullCallback(), &topicEntry._producer->get_producer())
            (makeMetadata(topicEntry), SentMessage(builder,
                                                   RD_KAFKA_RESP_ERR__QUEUE_FULL,
                                                   static_cast<PackedOpaque*>(builder.user_data())->first));
    }
}

void ProducerManagerImpl::report(
                    ProducerTopicEntry& topicEntry,
                    LogLevel level,
                    int error,
                    const std::string& reason,
                    void* opaque)
{
    if (error) {
        errorCallback(topicEntry, topicEntry._producer->get_producer(), error, reason, opaque);
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

int ProducerManagerImpl::pollTask(quantum::ThreadPromise<int>::Ptr promise,
                                  ProducerTopicEntry& entry)
{
    try {
        if (entry._forceSyncFlush || entry._preserveMessageOrder) {
            entry._producer->flush(entry._preserveMessageOrder); //synchronous flush
            entry._forceSyncFlush = false;
        }
        else {
            flush(entry);
        }
        return promise->set(0);
    }
    catch(const std::exception& ex) {
        exceptionHandler(ex, entry);
        return promise->set(-1);
    }
}

int ProducerManagerImpl::produceTask(quantum::ThreadPromise<int>::Ptr promise,
                                     ProducerTopicEntry& entry,
                                     ConcreteMessageBuilder<ByteArray>&& builder)
{
    try {
        if (entry._preserveMessageOrder) {
            // Save to local buffer so we can flush synchronously later
            entry._producer->add_message(builder);
        }
        else {
            // Post directly to internal librdkafka outbound queue
            produceMessage(entry, builder);
        }
        return promise->set(0);
    }
    catch(const std::exception& ex) {
        exceptionHandler(ex, entry);
        return promise->set(-1);
    }
}

int ProducerManagerImpl::produceTaskSync(ProducerTopicEntry& entry,
                                         const ConcreteMessageBuilder<ByteArray>& builder)
{
    try {
        if (entry._preserveMessageOrder) {
            // Save to local buffer so we can flush synchronously later
            entry._producer->add_message(builder);
        }
        else {
            // Post directly to internal librdkafka outbound queue
            produceMessage(entry, builder);
        }
        return 0;
    }
    catch(const std::exception& ex) {
        exceptionHandler(ex, entry);
        return -1;
    }
}

ConcreteMessageBuilder<ByteArray>
ProducerManagerImpl::serializeMessage(ProducerTopicEntry& entry,
                                      const void* key,
                                      const void* payload,
                                      const HeaderPack* headers,
                                      void* opaque)
{
    ConcreteMessageBuilder<ByteArray> builder(entry._configuration.getTopic());
    
    //Serialize the key
    Serializer::ResultType packedKey = CallbackInvoker<Serializer>
        ("key_serializer", entry._configuration.getKeySerializer(), &entry._producer->get_producer())(key);
    if (packedKey.empty()) {
        // Key is empty or encoding failed
        report(entry, LogLevel::LogErr, RD_KAFKA_RESP_ERR__KEY_SERIALIZATION, "Failed to serialize key", opaque);
        return {""};
    }
    builder.key(std::move(packedKey));
    
    //Serialize the headers if any
    for (auto it = headers->cbegin(); it != headers->cend(); ++it) {
        try {
            //Serialize the header
            Serializer::ResultType packedHeader = CallbackInvoker<Serializer>
                ("header_serializer", entry._configuration.getHeaderSerializer(it->first), &entry._producer->get_producer())
                (boost::unsafe_any_cast<void*>(&it->second));
            if (packedHeader.empty()) {
                // Header is empty or encoding failed
                std::ostringstream oss;
                oss << "Failed to serialize header: " << it->first;
                report(entry, LogLevel::LogErr, RD_KAFKA_RESP_ERR__VALUE_SERIALIZATION, oss.str(), opaque);
                return {""};
            }
            builder.header(Header<ByteArray>(it->first, std::move(packedHeader)));
        }
        catch (const std::out_of_range&) {
            std::ostringstream oss;
            oss << "No serializer found for header: " << it->first;
            const std::string what(oss.str());
            if (entry._skipUnknownHeaders) {
                report(entry, LogLevel::LogDebug, 0, what, opaque);
                continue;
            }
            report(entry, LogLevel::LogErr, RD_KAFKA_RESP_ERR__NOENT, what, opaque);
            return {""};
        }
    }
    
    //Serialize the payload
    Serializer::ResultType packedPayload = CallbackInvoker<Serializer>
        ("payload_serializer", entry._configuration.getPayloadSerializer(), &entry._producer->get_producer())(headers, payload);
    if (packedPayload.empty()) {
        // Payload is empty or encoding failed
        report(entry, LogLevel::LogErr, RD_KAFKA_RESP_ERR__VALUE_SERIALIZATION, "Failed to serialize payload", opaque);
        return {""};
    }
    builder.payload(std::move(packedPayload));
    
    // Add timestamp
    builder.timestamp(std::chrono::high_resolution_clock::now());
    return builder;
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

void* ProducerManagerImpl::setPackedOpaqueFuture(const Message& kafkaMessage)
{
    std::unique_ptr<PackedOpaque> packedOpaque(static_cast<PackedOpaque*>(kafkaMessage.get_user_data()));
    packedOpaque->second.set_value(DeliveryReport(TopicPartition(kafkaMessage.get_topic(),
                                                                 kafkaMessage.get_partition(),
                                                                 kafkaMessage.get_offset()),
                                                  kafkaMessage.get_error(), packedOpaque->first));
    return packedOpaque->first;
}

void* ProducerManagerImpl::setPackedOpaqueFuture(const MessageBuilder& builder, Error error)
{
    std::unique_ptr<PackedOpaque> packedOpaque(static_cast<PackedOpaque*>(builder.user_data()));
    packedOpaque->second.set_value(DeliveryReport(TopicPartition(builder.topic(),
                                                                 builder.partition(),
                                                                 TopicPartition::OFFSET_INVALID),
                                                  error, packedOpaque->first));
    return packedOpaque->first;
}

}
}
