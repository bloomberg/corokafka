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

namespace Bloomberg {
namespace corokafka {

//========================================================================
//                       CONNECTOR CONFIGURATION
//========================================================================
void ConnectorConfiguration::setDispatcherConfiguration(quantum::Configuration config)
{
    _dispatcherConfig = std::move(config);
}

void ConnectorConfiguration::setPollInterval(std::chrono::milliseconds interval)
{
    if (interval.count() == 0) {
        throw std::invalid_argument("Poll interval must be greater than 0");
    }
    _pollInterval = interval;
}

const std::chrono::milliseconds& ConnectorConfiguration::getPollInterval() const
{
    return _pollInterval;
}

void ConnectorConfiguration::setMaxMessagePayloadOutputLength(ssize_t length)
{
    _maxMessagePayloadLength = length;
}

ssize_t ConnectorConfiguration::getMaxMessagePayloadOutputLength() const
{
    return _maxMessagePayloadLength;
}

const quantum::Configuration& ConnectorConfiguration::getDispatcherConfiguration() const
{
    return _dispatcherConfig;
}

void ConnectorConfiguration::setCallback(Callbacks::ConnectorLogCallback callback)
{
    _logCallback = callback;
}

const Callbacks::ConnectorLogCallback& ConnectorConfiguration::getLogCallback() const
{
    return _logCallback;
}

}
}
