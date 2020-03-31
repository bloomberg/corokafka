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
#include <stdexcept>
#include <sstream>
#include <cctype>
#include <corokafka/corokafka_configuration_builder.h>
#include <corokafka/corokafka_utils.h>
#include <quantum/quantum.h>

namespace Bloomberg {
namespace corokafka {

ConfigurationBuilder& ConfigurationBuilder::operator()(const ProducerConfiguration& config)
{
    std::string topic(config.getTopic());
    auto result = _producerConfigurations.emplace(topic, config);
    if (!result.second) {
        throw InvalidArgumentException(0, "Duplicate producer configuration found");
    }
    return *this;
}

ConfigurationBuilder& ConfigurationBuilder::operator()(ProducerConfiguration&& config)
{
    std::string topic(config.getTopic());
    auto result = _producerConfigurations.emplace(topic, std::move(config));
    if (!result.second) {
        throw InvalidArgumentException(0, "Duplicate producer configuration found");
    }
    return *this;
}

ConfigurationBuilder& ConfigurationBuilder::operator()(const ConsumerConfiguration& config)
{
    std::string topic(config.getTopic());
    auto result = _consumerConfigurations.emplace(topic, config);
    if (!result.second) {
        throw InvalidArgumentException(0, "Duplicate consumer configuration found");
    }
    return *this;
}

ConfigurationBuilder& ConfigurationBuilder::operator()(ConsumerConfiguration&& config)
{
    std::string topic(config.getTopic());
    auto result = _consumerConfigurations.emplace(topic, std::move(config));
    if (!result.second) {
        throw InvalidArgumentException(0, "Duplicate consumer configuration found");
    }
    return *this;
}

ConfigurationBuilder& ConfigurationBuilder::operator()(const ConnectorConfiguration& config)
{
    _connectorConfiguration = config;
    return *this;
}

ConfigurationBuilder& ConfigurationBuilder::operator()(ConnectorConfiguration&& config)
{
    _connectorConfiguration = std::move(config);
    return *this;
}

const ConfigurationBuilder::ConfigMap<ProducerConfiguration>& ConfigurationBuilder::producerConfigurations() const
{
    return _producerConfigurations;
}

ConfigurationBuilder::ConfigMap<ProducerConfiguration>& ConfigurationBuilder::producerConfigurations()
{
    return _producerConfigurations;
}

const ConfigurationBuilder::ConfigMap<ConsumerConfiguration>& ConfigurationBuilder::consumerConfigurations() const
{
    return _consumerConfigurations;
}

ConfigurationBuilder::ConfigMap<ConsumerConfiguration>& ConfigurationBuilder::consumerConfigurations()
{
    return _consumerConfigurations;
}

const ConnectorConfiguration& ConfigurationBuilder::connectorConfiguration() const
{
    return std::move(_connectorConfiguration);
}

ConnectorConfiguration& ConfigurationBuilder::connectorConfiguration()
{
    return _connectorConfiguration;
}

}
}

