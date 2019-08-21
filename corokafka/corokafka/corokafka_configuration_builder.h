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
#ifndef BLOOMBERG_COROKAFKA_CONFIGURATION_BUILDER_H
#define BLOOMBERG_COROKAFKA_CONFIGURATION_BUILDER_H

#include <unordered_map>
#include <corokafka/corokafka_connector_configuration.h>
#include <corokafka/corokafka_producer_configuration.h>
#include <corokafka/corokafka_consumer_configuration.h>
#include <rapidjson/schema.h>

namespace Bloomberg {
namespace corokafka {

/**
 * @brief The ConfigurationBuilder is a configuration aggregator for all producer and consumer topic
 *        configurations. Various ProducerConfiguration and ConsumerConfiguration objects, as well
 *        as one ConnectorConfiguration, can be added to the builder, which in turn will be used to
 *        initialize the Connector object and create the appropriate setup in RdKafka as well as
 *        registering the supplied callbacks.
 */
class ConfigurationBuilder
{
    friend class ConnectorImpl;
public:
    template <typename Config>
    using ConfigMap = std::unordered_map<std::string, Config>;
    /**
     * @brief Add a producer or consumer configuration.
     * @details Each configuration supplied will be used to instantiate a producer or consumer
     *          object inside the connector having the respective settings.
     * @param config The producer or consumer configuration.
     * @return A reference to self for chaining purposes.
     */
    ConfigurationBuilder& operator()(const Configuration& config);
    ConfigurationBuilder& operator()(Configuration&& config);
    
    /**
     * @brief The connector configuration.
     * @param config The connector configuration.
     * @return A reference to self for chaining purposes.
     * @remark This method should only be called once.
     */
    ConfigurationBuilder& operator()(const ConnectorConfiguration& config);
    ConfigurationBuilder& operator()(ConnectorConfiguration&& config);
    
    /**
     * @brief Producer configuration getters
     */
    const ConfigMap<ProducerConfiguration>& producerConfigurations() const;
    ConfigMap<ProducerConfiguration>& producerConfigurations();
    
    /**
     * @brief Consumer configuration getters
     */
    const ConfigMap<ConsumerConfiguration>& consumerConfigurations() const;
    ConfigMap<ConsumerConfiguration>& consumerConfigurations();
    
    /**
     * @brief Connector configuration getters
     */
    const ConnectorConfiguration& connectorConfiguration() const;
    ConnectorConfiguration& connectorConfiguration();
    
private:
    // Members
    ConnectorConfiguration            _connectorConfiguration;
    ConfigMap<ProducerConfiguration>  _producerConfigurations;
    ConfigMap<ConsumerConfiguration>  _consumerConfigurations;
};

}
}

#endif //BLOOMBERG_COROKAFKA_CONFIGURATION_BUILDER_H
