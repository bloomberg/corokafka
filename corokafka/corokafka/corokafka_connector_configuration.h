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
#ifndef BLOOMBERG_COROKAFKA_CONNECTOR_CONFIGURATION_H
#define BLOOMBERG_COROKAFKA_CONNECTOR_CONFIGURATION_H

#include <corokafka/corokafka_callbacks.h>
#include <corokafka/corokafka_utils.h>
#include <corokafka/corokafka_configuration.h>
#include <quantum/quantum.h>

using namespace BloombergLP;

namespace Bloomberg {
namespace corokafka {

//========================================================================
//                       CONNECTOR CONFIGURATION
//========================================================================
class ConnectorConfiguration
{
public:
    /**
     * @brief Set the dispatcher configuration
     * @param config The configuration
     */
    void setDispatcherConfiguration(quantum::Configuration config);
    
    /**
     * @brief Get the quantum dispatcher configuration
     * @return A reference to the configuration object.
     */
    const quantum::Configuration& getDispatcherConfiguration() const;
    
    /**
     * @brief Set the internal poll interval for both consumers and producers.
     * @param interval The interval in milliseconds. Default is 100.
     * @remark Polling of all registered consumers and producers is done in parallel at each interval.
     */
    void setPollInterval(std::chrono::milliseconds interval);
    
    /**
     * @brief Get the configured poll interval.
     * @return The interval.
     */
    const std::chrono::milliseconds& getPollInterval() const;
    
    /**
     * @brief Sets the length of the serialized message to be printed when an internal error occurs.
     * @param length The length in bytes. Default is 100. -1 prints the entire message.
     * @remark If a log callback is set, the message length will be respected when invoking it.
     */
    void setMaxMessagePayloadOutputLength(ssize_t length);
    
    /**
     * @brief Get the maximum message length to be printed when an internal error occurs.
     * @return The message length.
     */
    ssize_t getMaxMessagePayloadOutputLength() const;
    
    /**
     * @brief Set the log callback used for the connector.
     * @param callback The callback.
     * @remark This callback is different than those used for producers and consumers.
     */
    void setCallback(Callbacks::ConnectorLogCallback callback);
    
    /**
     * @brief Get the log callback.
     * @return The callback.
     */
    const Callbacks::ConnectorLogCallback& getLogCallback() const;
    
private:
    std::chrono::milliseconds       _pollInterval{100};
    ssize_t                         _maxMessagePayloadLength{100};
    quantum::Configuration          _dispatcherConfig;
    Callbacks::ConnectorLogCallback _logCallback;
};

}
}

#endif //BLOOMBERG_COROKAFKA_CONNECTOR_CONFIGURATION_H
