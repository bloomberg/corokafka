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

const Configuration::OptionSet ConnectorConfiguration::s_internalOptions = {
    Options::pollIntervalMs,
    Options::maxPayloadOutputLength,
};

ConnectorConfiguration::ConnectorConfiguration(Configuration::OptionList options) :
    Configuration(std::move(options))
{
    init();
}

ConnectorConfiguration::ConnectorConfiguration(std::initializer_list<cppkafka::ConfigurationOption> options) :
    Configuration(std::move(options))
{
    init();
}

void ConnectorConfiguration::init()
{
    //Validate options
    parseOptions(s_internalOptionsPrefix, s_internalOptions, _options);
    
    const cppkafka::ConfigurationOption* pollInterval = Configuration::getOption(Options::pollIntervalMs);
    if (pollInterval) {
        int value = std::stoi(pollInterval->get_value());
        if (value <= 0) {
            throw ConfigurationException("Poll interval must be greater than 0");
        }
        _pollInterval = std::chrono::milliseconds(value);
    }
    
    const cppkafka::ConfigurationOption* maxPayloadLength =
        Configuration::getOption(Options::maxPayloadOutputLength);
    if (maxPayloadLength) {
        int value = std::stoi(maxPayloadLength->get_value());
        if (value < -1) {
            throw ConfigurationException("Max payload length value must be >= -1");
        }
        _maxMessagePayloadLength = value;
    }
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

}
}
