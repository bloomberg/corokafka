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
    "internal.consumer.read.size",
    "internal.consumer.auto.offset.persist",
    "internal.consumer.auto.offset.persist.on.exception",
    "internal.consumer.offset.persist.strategy",
    "internal.consumer.commit.exec",
    "internal.consumer.commit.backoff.strategy",
    "internal.consumer.commit.backoff.interval.ms",
    "internal.consumer.commit.max.backoff.ms",
    "internal.consumer.commit.num.retries",
    "internal.consumer.pause.on.start",
    "internal.consumer.poll.strategy",
    "internal.consumer.receive.callback.thread.range.low",
    "internal.consumer.receive.callback.thread.range.high",
    "internal.consumer.receive.callback.exec",
    "internal.consumer.timeout.ms",
    "internal.consumer.poll.timeout.ms",
    "internal.consumer.batch.prefetch",
    "internal.consumer.log.level",
    "internal.consumer.skip.unknown.headers",
    "internal.consumer.auto.throttle",
    "internal.consumer.auto.throttle.multiplier",
    "internal.consumer.preprocess.messages",
    "internal.consumer.preprocess.invoke.thread",
    "internal.consumer.receive.invoke.thread"
};

const Configuration::OptionSet ConsumerConfiguration::s_internalTopicOptions;

ConsumerConfiguration::ConsumerConfiguration(const std::string& topic,
                                             Options options,
                                             Options topicOptions) :
    Configuration(KafkaType::Consumer, topic, std::move(options), std::move(topicOptions))
{

}

ConsumerConfiguration::ConsumerConfiguration(const std::string& topic,
                                             std::initializer_list<ConfigurationOption> options,
                                             std::initializer_list<ConfigurationOption> topicOptions) :
    Configuration(KafkaType::Consumer, topic, std::move(options), std::move(topicOptions))
{

}

PartitionStrategy ConsumerConfiguration::getPartitionStrategy() const
{
    return _strategy;
}

const TopicPartitionList& ConsumerConfiguration::getInitialPartitionAssignment() const
{
    return _initialPartitionList;
}

void ConsumerConfiguration::assignInitialPartitions(PartitionStrategy strategy,
                                                    TopicPartitionList partitions)
{
    if ((strategy == PartitionStrategy::Static) && partitions.empty()) {
        throw std::invalid_argument("Initial partition assignment is empty");
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

const Receiver& ConsumerConfiguration::getReceiver() const
{
    if (!_receiver) {
        throw std::runtime_error("Receiver not set");
    }
    return *_receiver;
}

const Deserializer& ConsumerConfiguration::getKeyDeserializer() const
{
    if (!_keyDeserializer) {
        throw std::runtime_error("Key deserializer not set");
    }
    return *_keyDeserializer;
}

const Deserializer& ConsumerConfiguration::getPayloadDeserializer() const
{
    if (!_payloadDeserializer) {
        throw std::runtime_error("Payload deserializer not set");
    }
    return *_payloadDeserializer;
}

const Deserializer& ConsumerConfiguration::getHeaderDeserializer(const std::string& name) const
{
    auto it = _headerDeserializers.find(name);
    if (it == _headerDeserializers.end()) {
        throw std::runtime_error("Header deserializer not set for " + name);
    }
    return *it->second;
}

}
}
