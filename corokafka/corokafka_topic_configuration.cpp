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
#include <corokafka/corokafka_topic_configuration.h>
#include <corokafka/corokafka_producer_configuration.h>
#include <corokafka/corokafka_consumer_configuration.h>

namespace Bloomberg {
namespace corokafka {

TopicConfiguration::TopicConfiguration(KafkaType type,
                                       const std::string& topic,
                                       OptionList options,
                                       OptionList topicOptions) :
    Configuration(std::move(options)),
    _type(type),
    _topic(topic)
{
    _topicOptions[(int)OptionType::All] = std::move(topicOptions);
    filterOptions();
}

TopicConfiguration::TopicConfiguration(KafkaType type,
                                       const std::string& topic,
                                       std::initializer_list<cppkafka::ConfigurationOption> options,
                                       std::initializer_list<cppkafka::ConfigurationOption> topicOptions) :
    Configuration(std::move(options)),
    _type(type),
    _topic(topic)
{
    _topicOptions[(int)OptionType::All] = std::move(topicOptions);
    filterOptions();
}

KafkaType TopicConfiguration::configType() const
{
    return _type;
}

const std::string& TopicConfiguration::getTopic() const
{
    return _topic;
}

const Configuration::OptionList& TopicConfiguration::getTopicOptions(OptionType type) const
{
    return _topicOptions[(int)type];
}

void TopicConfiguration::setErrorCallback(Callbacks::ErrorCallback callback)
{
    _errorCallback = std::move(callback);
}

void TopicConfiguration::setThrottleCallback(Callbacks::ThrottleCallback callback)
{
    _throttleCallback = std::move(callback);
}

void TopicConfiguration::setLogCallback(Callbacks::LogCallback callback)
{
    _logCallback = std::move(callback);
}

void TopicConfiguration::setStatsCallback(Callbacks::StatsCallback callback)
{
    _statsCallback = std::move(callback);
}

bool TopicConfiguration::operator<(const TopicConfiguration& other) const
{
    return _topic < other._topic;
}

const Callbacks::ErrorCallback& TopicConfiguration::getErrorCallback() const
{
    return _errorCallback;
}

const Callbacks::ThrottleCallback& TopicConfiguration::getThrottleCallback() const
{
    return _throttleCallback;
}

const Callbacks::LogCallback& TopicConfiguration::getLogCallback() const
{
    return _logCallback;
}

const Callbacks::StatsCallback& TopicConfiguration::getStatsCallback() const
{
    return _statsCallback;
}

const cppkafka::ConfigurationOption* TopicConfiguration::getTopicOption(const std::string& name) const
{
    return findOption(name, _topicOptions[(int)OptionType::All]);
}

void TopicConfiguration::filterOptions()
{
    const std::string& internalOptionsPrefix = (_type == KafkaType::Producer) ?
        ProducerConfiguration::s_internalOptionsPrefix : ConsumerConfiguration::s_internalOptionsPrefix;
    
    // Consumer/Producer options parsing
    const OptionSet& internalOptions = (_type == KafkaType::Producer) ?
        ProducerConfiguration::s_internalOptions : ConsumerConfiguration::s_internalOptions;
    parseOptions(internalOptionsPrefix, internalOptions, _options);
    
    // Topic options parsing
    const OptionSet& internalTopicOptions = (_type == KafkaType::Producer) ?
        ProducerConfiguration::s_internalTopicOptions : ConsumerConfiguration::s_internalTopicOptions;
    parseOptions(internalOptionsPrefix, internalTopicOptions, _topicOptions);
}

}}

