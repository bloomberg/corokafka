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

const Configuration::OptionSet ConsumerConfiguration::s_internalOptions = {
    Options::autoOffsetPersist,
    Options::autoOffsetPersistOnException,
    Options::autoThrottle,
    Options::autoThrottleMultiplier,
    Options::batchPrefetch,
    Options::commitBackoffStrategy,
    Options::commitBackoffIntervalMs,
    Options::commitExec,
    Options::commitMaxBackoffMs,
    Options::commitNumRetries,
    Options::logLevel,
    Options::offsetPersistStrategy,
    Options::pauseOnStart,
    Options::pollStrategy,
    Options::pollTimeoutMs,
    Options::preprocessMessages,
    Options::preprocessInvokeThread,
    Options::readSize,
    Options::receiveCallbackExec,
    Options::receiveCallbackThreadRangeLow,
    Options::receiveCallbackThreadRangeHigh,
    Options::receiveInvokeThread,
    Options::skipUnknownHeaders,
    Options::timeoutMs
};

const Configuration::OptionSet ConsumerConfiguration::s_internalTopicOptions;

ConsumerConfiguration::ConsumerConfiguration(const std::string& topic,
                                             OptionList options,
                                             OptionList topicOptions) :
    TopicConfiguration(KafkaType::Consumer, topic, std::move(options), std::move(topicOptions))
{
}

ConsumerConfiguration::ConsumerConfiguration(const std::string& topic,
                                             std::initializer_list<cppkafka::ConfigurationOption> options,
                                             std::initializer_list<cppkafka::ConfigurationOption> topicOptions) :
    TopicConfiguration(KafkaType::Consumer, topic, std::move(options), std::move(topicOptions))
{
}

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

}
}
