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
#include <corokafka/corokafka_consumer_configuration.h>

namespace Bloomberg {
namespace corokafka {

//========================================================================
//                       CONSUMER CONFIGURATION
//========================================================================
const std::string ConsumerConfiguration::s_internalOptionsPrefix = "internal.consumer.";

const Configuration::OptionMap ConsumerConfiguration::s_internalOptions = {
    {Options::autoOffsetPersist,
     [](const std::string& topic, const cppkafka::ConfigurationOption* option, void* value)->bool{
        if (!option) return false;
        bool temp = Configuration::extractBooleanValue(topic, Options::autoOffsetPersist, *option);
        if (value) *reinterpret_cast<bool*>(value) = temp;
        return true;
     }},
     {Options::autoOffsetPersistOnException,
     [](const std::string& topic, const cppkafka::ConfigurationOption* option, void* value)->bool{
        if (!option) return false;
        bool temp = Configuration::extractBooleanValue(topic, Options::autoOffsetPersistOnException, *option);
        if (value) *reinterpret_cast<bool*>(value) = temp;
        return true;
     }},
    {Options::autoThrottle,
     [](const std::string& topic, const cppkafka::ConfigurationOption* option, void* value)->bool{
        if (!option) return false;
        bool temp = Configuration::extractBooleanValue(topic, Options::autoThrottle, *option);
        if (value) *reinterpret_cast<bool*>(value) = temp;
        return true;
     }},
    {Options::autoThrottleMultiplier,
     [](const std::string& topic, const cppkafka::ConfigurationOption* option, void* value)->bool{
        if (!option) return false;
        ssize_t temp = Configuration::extractCounterValue(topic, Options::autoThrottleMultiplier, *option, 1);
        if (value) *reinterpret_cast<uint16_t*>(value) = temp;
        return true;
     }},
    {Options::batchPrefetch,
     [](const std::string& topic, const cppkafka::ConfigurationOption* option, void* value)->bool{
        if (!option) return false;
        bool temp = Configuration::extractBooleanValue(topic, Options::batchPrefetch, *option);
        if (value) *reinterpret_cast<bool*>(value) = temp;
        return true;
     }},
    {Options::commitBackoffStrategy,
     [](const std::string& topic, const cppkafka::ConfigurationOption* option, void* value)->bool{
        if (!option) return false;
        cppkafka::BackoffPerformer::BackoffPolicy temp;
        if (StringEqualCompare()(option->get_value(), "linear")) {
            temp = cppkafka::BackoffPerformer::BackoffPolicy::LINEAR;
        }
        else if (StringEqualCompare()(option->get_value(), "exponential")) {
           temp = cppkafka::BackoffPerformer::BackoffPolicy::EXPONENTIAL;
        }
        else {
            throw InvalidOptionException(topic, Options::commitBackoffStrategy, option->get_value());
        }
        if (value) *reinterpret_cast<cppkafka::BackoffPerformer::BackoffPolicy*>(value) = temp;
        return true;
     }},
    {Options::commitBackoffIntervalMs,
     [](const std::string& topic, const cppkafka::ConfigurationOption* option, void* value)->bool{
        if (!option) return false;
        std::chrono::milliseconds temp{Configuration::extractCounterValue(topic, Options::commitBackoffIntervalMs, *option, 1)};
        if (value) *reinterpret_cast<std::chrono::milliseconds*>(value) = temp;
        return true;
     }},
    {Options::commitExec,
     [](const std::string& topic, const cppkafka::ConfigurationOption* option, void* value)->bool{
        if (!option) return false;
        ExecMode temp;
        if (StringEqualCompare()(option->get_value(), "sync")) {
            temp = ExecMode::Sync;
        }
        else if (StringEqualCompare()(option->get_value(), "async")) {
            temp = ExecMode::Async;
        }
        else {
            throw InvalidOptionException(topic, Options::commitExec, option->get_value());
        }
        if (value) *reinterpret_cast<ExecMode*>(value) = temp;
        return true;
     }},
    {Options::commitMaxBackoffMs,
     [](const std::string& topic, const cppkafka::ConfigurationOption* option, void* value)->bool{
        if (!option) return false;
        std::chrono::milliseconds temp{Configuration::extractCounterValue(topic, Options::commitMaxBackoffMs, *option, 1)};
        if (value) *reinterpret_cast<std::chrono::milliseconds*>(value) = temp;
        return true;
     }},
    {Options::commitNumRetries,
     [](const std::string& topic, const cppkafka::ConfigurationOption* option, void* value)->bool{
        if (!option) return false;
        ssize_t temp = Configuration::extractCounterValue(topic, Options::commitNumRetries, *option, 0);
        if (value) *reinterpret_cast<size_t*>(value) = temp;
        return true;
     }},
    {Options::logLevel,
     [](const std::string& topic, const cppkafka::ConfigurationOption* option, void* value)->bool{
        if (!option) return false;
        cppkafka::LogLevel temp = Configuration::extractLogLevel(topic, Options::logLevel, option->get_value());
        if (value) *reinterpret_cast<cppkafka::LogLevel*>(value) = temp;
        return true;
     }},
    {Options::offsetPersistStrategy,
     [](const std::string& topic, const cppkafka::ConfigurationOption* option, void* value)->bool{
        if (!option) return false;
        OffsetPersistStrategy temp;
        if (StringEqualCompare()(option->get_value(), "commit")) {
            temp = OffsetPersistStrategy::Commit;
        }
        else if (StringEqualCompare()(option->get_value(), "store")) {
#if (RD_KAFKA_VERSION < RD_KAFKA_STORE_OFFSETS_SUPPORT_VERSION)
            std::ostringstream oss;
            oss << std::hex << "Current RdKafka version " << RD_KAFKA_VERSION
                << " does not support this functionality. Must be greater than "
                << RD_KAFKA_STORE_OFFSETS_SUPPORT_VERSION;
            throw FeatureNotSupportedException(topic, Options::offsetPersistStrategy, oss.str());
#else
            temp = OffsetPersistStrategy::Store;
#endif
        }
        else {
            throw InvalidOptionException(topic, Options::offsetPersistStrategy, option->get_value());
        }
        if (value) *reinterpret_cast<OffsetPersistStrategy*>(value) = temp;
        return true;
     }},
    {Options::pauseOnStart,
     [](const std::string& topic, const cppkafka::ConfigurationOption* option, void* value)->bool{
        if (!option) return false;
        bool temp = Configuration::extractBooleanValue(topic, Options::pauseOnStart, *option);
        if (value) *reinterpret_cast<bool*>(value) = temp;
        return true;
     }},
     {Options::preserveMessageOrder, [](const std::string& topic, const cppkafka::ConfigurationOption* option, void* value)->bool {
        if (!option) return false;
        bool temp = Configuration::extractBooleanValue(topic, Options::preserveMessageOrder, *option);
        if (value) *reinterpret_cast<bool*>(value) = temp;
        return true;
    }},
     {Options::pollIoThreadId, [](const std::string& topic, const cppkafka::ConfigurationOption* option, void* value)->bool {
        if (!option) return false;
        ssize_t temp = Configuration::extractCounterValue(topic, Options::pollIoThreadId, *option, EnumValue(quantum::IQueue::QueueId::Any));
        if (value) *reinterpret_cast<int*>(value) = temp;
        return true;
    }},
    {Options::processCoroThreadId, [](const std::string& topic, const cppkafka::ConfigurationOption* option, void* value)->bool {
        if (!option) return false;
        ssize_t temp = Configuration::extractCounterValue(topic, Options::processCoroThreadId, *option, EnumValue(quantum::IQueue::QueueId::Any));
        if (value) *reinterpret_cast<int*>(value) = temp;
        return true;
    }},
    {Options::pollStrategy,
     [](const std::string& topic, const cppkafka::ConfigurationOption* option, void* value)->bool{
        if (!option) return false;
        PollStrategy temp;
        if (StringEqualCompare()(option->get_value(), "roundrobin")) {
            temp = PollStrategy::RoundRobin;
        }
        else if (StringEqualCompare()(option->get_value(), "batch")) {
            temp = PollStrategy::Batch;
        }
        else if (StringEqualCompare()(option->get_value(), "serial")) {
            temp = PollStrategy::Serial;
        }
        else {
            throw InvalidOptionException(topic, Options::pollStrategy, option->get_value());
        }
        if (value) *reinterpret_cast<PollStrategy*>(value) = temp;
        return true;
     }},
    {Options::pollTimeoutMs,
     [](const std::string& topic, const cppkafka::ConfigurationOption* option, void* value)->bool{
        if (!option) return false;
        std::chrono::milliseconds temp{Configuration::extractCounterValue
            (topic, Options::pollTimeoutMs, *option, EnumValue(TimerValues::Unlimited))};
        if (value) *reinterpret_cast<std::chrono::milliseconds*>(value) = temp;
        return true;
     }},
    {Options::preprocessMessages,
     [](const std::string& topic, const cppkafka::ConfigurationOption* option, void* value)->bool{
        if (!option) return false;
        bool temp = Configuration::extractBooleanValue(topic, Options::preprocessMessages, *option);
        if (value) *reinterpret_cast<bool*>(value) = temp;
        return true;
     }},
    {Options::readSize,
     [](const std::string& topic, const cppkafka::ConfigurationOption* option, void* value)->bool{
        if (!option) return false;
        ssize_t temp = Configuration::extractCounterValue(topic, Options::readSize, *option, -1);
        if (value) *reinterpret_cast<size_t*>(value) = temp;
        return true;
     }},
    {Options::receiveCallbackExec,
     [](const std::string& topic, const cppkafka::ConfigurationOption* option, void* value)->bool{
        if (!option) return false;
        ExecMode temp;
        if (StringEqualCompare()(option->get_value(), "sync")) {
            temp = ExecMode::Sync;
        }
        else if (StringEqualCompare()(option->get_value(), "async")) {
            temp = ExecMode::Async;
        }
        else {
            throw InvalidOptionException(topic, Options::receiveCallbackExec, option->get_value());
        }
        if (value) *reinterpret_cast<ExecMode*>(value) = temp;
        return true;
     }},
    {Options::receiveCallbackThreadRangeLow,
     [](const std::string& topic, const cppkafka::ConfigurationOption* option, void* value)->bool{
        if (!option) return false;
        int temp = Configuration::extractCounterValue(topic, Options::receiveCallbackThreadRangeLow, *option, 0);
        if (value) *reinterpret_cast<int*>(value) = temp;
        return true;
     }},
    {Options::receiveCallbackThreadRangeHigh,
     [](const std::string& topic, const cppkafka::ConfigurationOption* option, void* value)->bool{
        if (!option) return false;
        int temp = Configuration::extractCounterValue(topic, Options::receiveCallbackThreadRangeHigh, *option, 0);
        if (value) *reinterpret_cast<int*>(value) = temp;
        return true;
     }},
    {Options::receiveInvokeThread,
     [](const std::string& topic, const cppkafka::ConfigurationOption* option, void* value)->bool{
        if (!option) return false;
        ThreadType temp;
        if (StringEqualCompare()(option->get_value(), "io")) {
            temp = ThreadType::IO;
        }
        else if (StringEqualCompare()(option->get_value(), "coro")) {
            temp = ThreadType::Coro;
        }
        else {
            throw InvalidOptionException(topic, Options::receiveInvokeThread, option->get_value());
        }
        if (value) *reinterpret_cast<ThreadType*>(value) = temp;
        return true;
     }},
    {Options::minRoundRobinPollTimeoutMs,
     [](const std::string& topic, const cppkafka::ConfigurationOption* option, void* value)->bool{
        if (!option) return false;
        std::chrono::milliseconds temp{Configuration::extractCounterValue(topic, Options::minRoundRobinPollTimeoutMs, *option, 1)};
        if (value) *reinterpret_cast<std::chrono::milliseconds*>(value) = temp;
        return true;
     }},
     {Options::minPollIntervalMs,
     [](const std::string& topic, const cppkafka::ConfigurationOption* option, void* value)->bool{
        if (!option) return false;
        std::chrono::milliseconds temp{Configuration::extractCounterValue(topic, Options::minPollIntervalMs, *option, 1)};
        if (value) *reinterpret_cast<std::chrono::milliseconds*>(value) = temp;
        return true;
     }},
    {Options::skipUnknownHeaders,
     [](const std::string& topic, const cppkafka::ConfigurationOption* option, void* value)->bool{
        if (!option) return false;
        bool temp = Configuration::extractBooleanValue(topic, Options::skipUnknownHeaders, *option);
        if (value) *reinterpret_cast<bool*>(value) = temp;
        return true;
     }},
    {Options::timeoutMs,
     [](const std::string& topic, const cppkafka::ConfigurationOption* option, void* value)->bool{
        if (!option) return false;
        std::chrono::milliseconds temp{Configuration::extractCounterValue(topic, Options::timeoutMs, *option, EnumValue(TimerValues::Unlimited))};
        if (value) *reinterpret_cast<std::chrono::milliseconds*>(value) = temp;
        return true;
     }},
     {Options::startupTimeoutMs,
     [](const std::string& topic, const cppkafka::ConfigurationOption* option, void* value)->bool{
        if (!option) return false;
        std::chrono::milliseconds temp{Configuration::extractCounterValue(topic, Options::startupTimeoutMs, *option, (int)TimerValues::Unlimited)};
        if (value) *reinterpret_cast<std::chrono::milliseconds*>(value) = temp;
        return true;
     }}
};

const Configuration::OptionMap ConsumerConfiguration::s_internalTopicOptions;

PartitionStrategy ConsumerConfiguration::getPartitionStrategy() const
{
    return _strategy;
}

const cppkafka::TopicPartitionList& ConsumerConfiguration::getInitialPartitionAssignment() const
{
    return _initialPartitionList;
}

void ConsumerConfiguration::assignInitialPartitions(PartitionStrategy strategy,
                                                    cppkafka::TopicPartitionList partitions)
{
    if ((strategy == PartitionStrategy::Static) && partitions.empty()) {
        throw ConfigurationException(getTopic(), "Initial partition assignment is empty");
    }
    _strategy = strategy;
    _initialPartitionList = std::move(partitions);
}

void ConsumerConfiguration::setOffsetCommitCallback(Callbacks::OffsetCommitCallback callback)
{
    _offsetCommitCallback = std::move(callback);
}

const Callbacks::OffsetCommitCallback& ConsumerConfiguration::getOffsetCommitCallback() const
{
    return _offsetCommitCallback;
}

void ConsumerConfiguration::setRebalanceCallback(Callbacks::RebalanceCallback callback)
{
    _rebalanceCallback = std::move(callback);
}

const Callbacks::RebalanceCallback& ConsumerConfiguration::getRebalanceCallback() const
{
    return _rebalanceCallback;
}

void ConsumerConfiguration::setPreprocessorCallback(Callbacks::PreprocessorCallback callback)
{
    _preprocessorCallback = std::move(callback);
}

const Callbacks::PreprocessorCallback& ConsumerConfiguration::getPreprocessorCallback() const
{
    return _preprocessorCallback;
}

const TypeErasedDeserializer& ConsumerConfiguration::getTypeErasedDeserializer() const
{
    return _typeErasedDeserializer;
}

const Receiver& ConsumerConfiguration::getTypeErasedReceiver() const
{
    if (!_receiver) {
        throw ConfigurationException(getTopic(), "Receiver callback not set");
    }
    return *_receiver;
}

const Configuration::OptionExtractorFunc&
ConsumerConfiguration::extract(const std::string& option)
{
    return Configuration::extractOption(s_internalOptions, option);
}

}
}
