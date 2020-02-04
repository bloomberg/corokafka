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

namespace Bloomberg {
namespace corokafka {

//========================================================================
//                       CONNECTOR CONFIGURATION
//========================================================================
/**
 * @brief The ConnectorConfiguration is a builder class which contains generic
 *        configuration options for the library as a whole.
 */
class ConnectorConfiguration : public Configuration
{
public:
    /**
     * @brief Internal CoroKafka-specific options for the connector.
     *        See CONFIGURATION.md for more details.
     */
    struct Options
    {
        static constexpr const char* pollIntervalMs =           "internal.connector.poll.interval.ms";
        static constexpr const char* maxPayloadOutputLength =   "internal.connector.max.payload.output.length";
        static constexpr const char* shutdownIoWaitTimeoutMs =  "internal.connector.shutdown.io.wait.timeout.ms";
    };
    
    /**
     * @brief Default constructor
     */
    ConnectorConfiguration() = default;
    
    /**
     * @brief Creates a connector object using the supplied options.
     * @param options The connector options (see 'Options' above).
     * @remark No RdKafka option should be passed here.
     */
    explicit ConnectorConfiguration(OptionList options);
    explicit ConnectorConfiguration(OptionInitList options);
    
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
     * @brief Set the log callback used for the connector.
     * @param callback The callback.
     * @remark This callback is different than those used for producers and consumers.
     */
    void setLogCallback(Callbacks::ConnectorLogCallback callback);
    
    /**
     * @brief Get the log callback.
     * @return The callback.
     */
    const Callbacks::ConnectorLogCallback& getLogCallback() const;
    
private:
    friend class Configuration;
    friend class ConnectorImpl;
    friend class ProducerManagerImpl;
    friend class ConsumerManagerImpl;
    
    void init();
    const std::chrono::milliseconds& getPollInterval() const;
    ssize_t getMaxMessagePayloadOutputLength() const;
    const std::chrono::milliseconds& getShutdownIoWaitTimeout() const;
    
    static const OptionExtractorFunc& extract(const std::string& option);
    
    std::chrono::milliseconds               _pollInterval{100};
    ssize_t                                 _maxMessagePayloadLength{100};
    std::chrono::milliseconds               _shutdownIoWaitTimeoutMs{2000};
    quantum::Configuration                  _dispatcherConfig;
    Callbacks::ConnectorLogCallback         _logCallback;
    static const OptionMap                  s_internalOptions;
    static const std::string                s_internalOptionsPrefix;
};

}
}

#endif //BLOOMBERG_COROKAFKA_CONNECTOR_CONFIGURATION_H
