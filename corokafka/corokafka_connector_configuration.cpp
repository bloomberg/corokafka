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
#include <corokafka/corokafka_connector_configuration.h>
#include <corokafka/corokafka_exception.h>

namespace Bloomberg {
namespace corokafka {

//========================================================================
//                       CONNECTOR CONFIGURATION
//========================================================================
const std::string ConnectorConfiguration::s_internalOptionsPrefix = "internal.connector.";

const Configuration::OptionMap ConnectorConfiguration::s_internalOptions = {
    {Options::pollIntervalMs,
     [](const std::string& topic, const cppkafka::ConfigurationOption* option, void* value)->bool{
        if (!option) return false;
        std::chrono::milliseconds temp{Configuration::extractCounterValue({}, Options::pollIntervalMs, *option, 1)};
        if (value) *reinterpret_cast<std::chrono::milliseconds*>(value) = temp;
        return true;
    }},
    {Options::maxPayloadOutputLength,
     [](const std::string& topic, const cppkafka::ConfigurationOption* option, void* value)->bool {
        if (!option) return false;
        ssize_t temp = Configuration::extractCounterValue({}, Options::maxPayloadOutputLength, *option, -1);
        if (value) *reinterpret_cast<ssize_t*>(value) = temp;
        return true;
    }},
    {Options::shutdownIoWaitTimeoutMs,
     [](const std::string& topic, const cppkafka::ConfigurationOption* option, void* value)->bool{
        if (!option) return false;
        std::chrono::milliseconds temp{Configuration::extractCounterValue({}, Options::shutdownIoWaitTimeoutMs, *option, 0)};
        if (value) *reinterpret_cast<std::chrono::milliseconds*>(value) = temp;
        return true;
    }},
};

ConnectorConfiguration::ConnectorConfiguration(OptionList options) :
    Configuration(std::move(options))
{
    init();
}

ConnectorConfiguration::ConnectorConfiguration(OptionInitList options) :
    Configuration(std::move(options))
{
    init();
}

void ConnectorConfiguration::init()
{
    //Validate options
    parseOptions({}, s_internalOptionsPrefix, s_internalOptions, _options, OptionsPermission::RdKafkaDisallow);
    extract(Options::pollIntervalMs)
        ({}, Configuration::getOption(Options::pollIntervalMs), &_pollInterval);
    extract(Options::maxPayloadOutputLength)
        ({}, Configuration::getOption(Options::maxPayloadOutputLength), &_maxMessagePayloadLength);
    extract(Options::shutdownIoWaitTimeoutMs)
        ({}, Configuration::getOption(Options::shutdownIoWaitTimeoutMs), &_shutdownIoWaitTimeoutMs);
}

void ConnectorConfiguration::setDispatcherConfiguration(quantum::Configuration config)
{
    _dispatcherConfig = std::move(config);
}

const std::chrono::milliseconds& ConnectorConfiguration::getPollInterval() const
{
    return _pollInterval;
}

ssize_t ConnectorConfiguration::getMaxMessagePayloadOutputLength() const
{
    return _maxMessagePayloadLength;
}

const std::chrono::milliseconds& ConnectorConfiguration::getShutdownIoWaitTimeout() const
{
    return _shutdownIoWaitTimeoutMs;
}

const quantum::Configuration& ConnectorConfiguration::getDispatcherConfiguration() const
{
    return _dispatcherConfig;
}

void ConnectorConfiguration::setLogCallback(Callbacks::ConnectorLogCallback callback)
{
    _logCallback = callback;
}

const Callbacks::ConnectorLogCallback& ConnectorConfiguration::getLogCallback() const
{
    return _logCallback;
}

const Configuration::OptionExtractorFunc&
ConnectorConfiguration::extract(const std::string& option)
{
    return Configuration::extractOption(s_internalOptions, option);
}

}
}
