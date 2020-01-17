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
#include <corokafka/corokafka_producer_configuration.h>
#include <corokafka/corokafka_producer_topic_entry.h>

namespace Bloomberg {
namespace corokafka {

//========================================================================
//                       PRODUCER CONFIGURATION
//========================================================================
const std::string ProducerConfiguration::s_internalOptionsPrefix = "internal.producer.";

const Configuration::OptionMap ProducerConfiguration::s_internalOptions = {
    {Options::autoThrottle,
     [](const std::string& topic, const cppkafka::ConfigurationOption* option, void* value)->bool {
        if (!option) return false;
        bool temp = Configuration::extractBooleanValue(topic, ProducerConfiguration::Options::autoThrottle, *option);
        if (value) *reinterpret_cast<bool*>(value) = temp;
        return true;
    }},
    {Options::autoThrottleMultiplier,
     [](const std::string& topic, const cppkafka::ConfigurationOption* option, void* value)->bool {
        if (!option) return false;
        ssize_t temp = Configuration::extractCounterValue(topic, ProducerConfiguration::Options::autoThrottleMultiplier, *option, 1);
        if (value) *reinterpret_cast<ssize_t*>(value) = temp;
        return true;
    }},
    {Options::flushWaitForAcksTimeoutMs, [](const std::string& topic, const cppkafka::ConfigurationOption* option, void* value)->bool {
        if (!option) return false;
        std::chrono::milliseconds temp(Configuration::extractCounterValue(
            topic, ProducerConfiguration::Options::flushWaitForAcksTimeoutMs, *option, (int)TimerValues::Unlimited));
        if (value) *reinterpret_cast<std::chrono::milliseconds*>(value) = temp;
        return true;
    }},
    {Options::logLevel, [](const std::string& topic, const cppkafka::ConfigurationOption* option, void* value)->bool{
        if (!option) return false;
        cppkafka::LogLevel temp = Configuration::extractLogLevel(topic, ProducerConfiguration::Options::logLevel, option->get_value());
        if (value) *reinterpret_cast<cppkafka::LogLevel*>(value) = temp;
        return true;
    }},
    {Options::maxQueueLength, [](const std::string& topic, const cppkafka::ConfigurationOption* option, void* value)->bool{
        if (!option) return false;
        size_t temp = Configuration::extractCounterValue(topic, ProducerConfiguration::Options::maxQueueLength, *option, 1);
        if (value) *reinterpret_cast<size_t*>(value) = temp;
        return true;
    }},
    {Options::payloadPolicy, [](const std::string& topic, const cppkafka::ConfigurationOption* option, void* value)->bool{
        if (!option) return false;
        cppkafka::Producer::PayloadPolicy temp;
        if (StringEqualCompare()(option->get_value(), "passthrough")) {
            temp = cppkafka::Producer::PayloadPolicy::PASSTHROUGH_PAYLOAD;
        }
        else if (StringEqualCompare()(option->get_value(), "copy")) {
            temp = cppkafka::Producer::PayloadPolicy::COPY_PAYLOAD;
        }
        else {
            throw InvalidOptionException(topic, ProducerConfiguration::Options::payloadPolicy, option->get_value());
        }
        if (value) *reinterpret_cast<cppkafka::Producer::PayloadPolicy*>(value) = temp;
        return true;
    }},
    {Options::preserveMessageOrder, [](const std::string& topic, const cppkafka::ConfigurationOption* option, void* value)->bool {
        if (!option) return false;
        bool temp = Configuration::extractBooleanValue(
            topic, ProducerConfiguration::Options::preserveMessageOrder, *option);
        if (value) *reinterpret_cast<bool*>(value) = temp;
        return true;
    }},
    {Options::queueFullNotification, [](const std::string& topic, const cppkafka::ConfigurationOption* option, void* value){
        if (!option) return false;
        QueueFullNotification temp;
        if (StringEqualCompare()(option->get_value(), "edgeTriggered")) {
            temp = QueueFullNotification::EdgeTriggered;
        }
        else if (StringEqualCompare()(option->get_value(), "oncePerMessage")) {
            temp = QueueFullNotification::OncePerMessage;
        }
        else if (StringEqualCompare()(option->get_value(), "eachOccurence")) {
            temp = QueueFullNotification::EachOccurence;
        }
        else {
            throw InvalidOptionException(topic, ProducerConfiguration::Options::queueFullNotification, option->get_value());
        }
        if (value) *reinterpret_cast<QueueFullNotification*>(value) = temp;
        return true;
    }},
    {Options::retries, [](const std::string& topic, const cppkafka::ConfigurationOption* option, void* value)->bool{
        if (!option) return false;
        size_t temp = Configuration::extractCounterValue(topic, ProducerConfiguration::Options::retries, *option, 0);
        if (value) *reinterpret_cast<size_t*>(value) = temp;
        return true;
    }},
    {Options::timeoutMs, [](const std::string& topic, const cppkafka::ConfigurationOption* option, void* value)->bool{
        if (!option) return false;
        std::chrono::milliseconds temp{Configuration::extractCounterValue(
            topic, ProducerConfiguration::Options::timeoutMs, *option, (int)TimerValues::Unlimited)};
        if (value) *reinterpret_cast<std::chrono::milliseconds*>(value) = temp;
        return true;
    }},
    {Options::waitForAcksTimeoutMs, [](const std::string& topic, const cppkafka::ConfigurationOption* option, void* value)->bool{
        if (!option) return false;
        std::chrono::milliseconds temp{Configuration::extractCounterValue(
            topic, ProducerConfiguration::Options::waitForAcksTimeoutMs, *option, (int)TimerValues::Unlimited)};
        if (value) *reinterpret_cast<std::chrono::milliseconds*>(value) = temp;
        return true;
    }}
};

const Configuration::OptionMap ProducerConfiguration::s_internalTopicOptions;

ProducerConfiguration::ProducerConfiguration(const std::string& topicName,
                                             OptionList options,
                                             OptionList topicOptions) :
    TopicConfiguration(KafkaType::Producer, topicName, std::move(options), std::move(topicOptions))
{
}

ProducerConfiguration::ProducerConfiguration(const std::string& topicName,
                                             OptionInitList options,
                                             OptionInitList topicOptions) :
    TopicConfiguration(KafkaType::Producer, topicName, std::move(options), std::move(topicOptions))
{
}

void ProducerConfiguration::setDeliveryReportCallback(Callbacks::DeliveryReportCallback callback)
{
    _deliveryReportCallback = std::move(callback);
}

const Callbacks::DeliveryReportCallback& ProducerConfiguration::getDeliveryReportCallback() const
{
    return _deliveryReportCallback;
}

void ProducerConfiguration::setPartitionerCallback(Callbacks::PartitionerCallback callback)
{
    _partitionerCallback = callback;
}

const Callbacks::PartitionerCallback& ProducerConfiguration::getPartitionerCallback() const
{
    return _partitionerCallback;
}

void ProducerConfiguration::setQueueFullCallback(Callbacks::QueueFullCallback callback)
{
    _queueFullCallback = callback;
}

const Callbacks::QueueFullCallback& ProducerConfiguration::getQueueFullCallback() const
{
    return _queueFullCallback;
}

const Configuration::OptionExtractorFunc&
ProducerConfiguration::extract(const std::string& option)
{
    return s_internalOptions.at(option);
}

}
}
