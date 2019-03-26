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
#include <corokafka/corokafka_configuration.h>
#include <corokafka/corokafka_producer_configuration.h>
#include <corokafka/corokafka_consumer_configuration.h>

namespace Bloomberg {
namespace corokafka {

//========================================================================
//                             CONFIGURATION
//========================================================================
const std::string& Configuration::getJsonSchema()
{
    static const std::string jsonSchema = R"JSON(
    {
        "$schema" : "http://json-schema.org/draft-04/schema#",
        "$id" : "bloomberg:corokafka.json",
        "definitions": {
            "connector": {
                "title": "CoroKafka configuration",
                "type": "object",
                "properties": {
                    "pollIntervalMs": {
                        "type":"number",
                        "default":100
                    },
                    "maxMessagePayloadOutputLength": {
                        "type":"number",
                        "default":100
                    },
                    "quantum": {
                        "$ref": "bloomberg:quantum.json"
                    },
                },
                "additionalProperties": false,
                "required": []
            },
            "option": {
                "title": "Internal options for corokafka, cppkafka and rdkafka",
                "type": "object",
                "patternProperties": {
                    "^.*$": {
                        "anyOf": [
                            {"type":"number"},
                            {"type":"boolean"},
                            {"type":"string"}
                        ],
                        "examples": ["metadata.broker.list", "internal.producer.payload.policy"]
                    }
                }
            },
            "partition": {
                "title": "A kafka partition",
                "type": "object",
                "properties": {
                    "id": {
                        "type":"number"
                    },
                    "offset": {
                        "type":"number"
                    }
                },
                "additionalProperties": false,
                "required": [ "id","offset" ]
            },
            "topic": {
                "title": "Consumer or producer topic",
                "type": "object",
                "properties": {
                    "name": {
                        "type":"string"
                    },
                    "type": {
                        "type":"string",
                        "enum": ["producer", "consumer"]
                    },
                    "config": {
                        "$ref" : "#/definitions/option"
                    },
                    "topicConfig": {
                        "$ref" : "#/definitions/option"
                    },
                    "partitionStrategy": {
                        "description":"Only applies to consumer topics",
                        "type":"string",
                        "enum":["static","dynamic"],
                        "default":"dynamic"
                    },
                    "partitionAssignment": {
                        "description":"Only applies to consumer topics",
                        "type":"array",
                        "items": { "$ref" : "#/definitions/partition"}
                    }
                },
                "additionalProperties": false,
                "required": ["name","type"]
            }
        },
        "title": "Kafka connector settings",
        "type": "object",
        "properties": {
            "connector": { "$ref":"#/definitions/connector" },
            "topics": {
                "type":"array",
                "items": { "$ref": "#/definitions/topic" },
                "minItems": 1,
                "uniqueItems": true
            }
        },
        "additionalProperties": false,
        "required": [ "topics" ]
    }
    )JSON";
    return jsonSchema;
}

const std::string& Configuration::getJsonSchemaUri()
{
    static std::string uri = "bloomberg:corokafka.json";
    return uri;
}

Configuration::Configuration(KafkaType type,
                             const std::string& topic,
                             Options config,
                             Options topicConfig) :
    _type(type),
    _topic(topic)
{
    filterOptions(std::move(config), std::move(topicConfig));
}

Configuration::Configuration(KafkaType type,
                             const std::string& topic,
                             std::initializer_list<ConfigurationOption> config,
                             std::initializer_list<ConfigurationOption> topicConfig) :
    _type(type),
    _topic(topic)
{
    filterOptions(std::move(config), std::move(topicConfig));
}

KafkaType Configuration::configType() const
{
    return _type;
}

const std::string& Configuration::getTopic() const
{
    return _topic;
}

const Configuration::Options& Configuration::getConfiguration() const
{
    return _config;
}

const Configuration::Options& Configuration::getTopicConfiguration() const
{
    return _topicConfig;
}

const Configuration::Options& Configuration::getInternalConfiguration() const
{
    return _internalConfig;
}

const Configuration::Options& Configuration::getInternalTopicConfiguration() const
{
    return _internalTopicConfig;
}

void Configuration::setCallback(Callbacks::ErrorCallback callback)
{
    _errorCallback = std::move(callback);
}

void Configuration::setCallback(Callbacks::ThrottleCallback callback)
{
    _throttleCallback = std::move(callback);
}

void Configuration::setCallback(Callbacks::LogCallback callback)
{
    _logCallback = std::move(callback);
}

void Configuration::setCallback(Callbacks::StatsCallback callback)
{
    _statsCallback = std::move(callback);
}

bool Configuration::operator<(const Configuration& other) const
{
    return _topic < other._topic;
}

const Callbacks::ErrorCallback& Configuration::getErrorCallback() const
{
    return _errorCallback;
}

const Callbacks::ThrottleCallback& Configuration::getThrottleCallback() const
{
    return _throttleCallback;
}

const Callbacks::LogCallback& Configuration::getLogCallback() const
{
    return _logCallback;
}

const Callbacks::StatsCallback& Configuration::getStatsCallback() const
{
    return _statsCallback;
}

const ConfigurationOption* Configuration::getConfiguration(const std::string& name) const
{
    const ConfigurationOption* option = findConfig(name, _config);
    if (!option) {
        option = findConfig(name, _internalConfig);
    }
    return option;
}

const ConfigurationOption* Configuration::getTopicConfiguration(const std::string& name) const
{
    const ConfigurationOption* option = findConfig(name, _topicConfig);
    if (!option) {
        option = findConfig(name, _internalTopicConfig);
    }
    return option;
}

const ConfigurationOption* Configuration::findConfig(const std::string& name,
                                                     const Options& config)
{
    const auto it = std::find_if(config.cbegin(), config.cend(),
                                 [&name](const ConfigurationOption& config)->bool {
        return StringEqualCompare()(config.get_key(), name);
    });
    if (it != config.cend()) {
        return &*it;
    }
    return nullptr;
}

void Configuration::filterOptions(Options&& config,
                                  Options&& topicConfig)
{
    const std::string& internalOptionsPrefix = (_type == KafkaType::Producer) ?
        ProducerConfiguration::s_internalOptionsPrefix : ConsumerConfiguration::s_internalOptionsPrefix;
    
    auto parse = [&internalOptionsPrefix](const OptionSet& allowed, Options& internal, Options& external, Options& config)
    {
        if (!allowed.empty()) {
            for (auto&& option : config) {
                if (StringEqualCompare()(option.get_key(), internalOptionsPrefix, internalOptionsPrefix.length())) {
                    auto it = allowed.find(option.get_key());
                    if (it == allowed.end()) {
                        std::ostringstream oss;
                        oss << "Invalid option found: " << option.get_key();
                        throw std::runtime_error(oss.str().c_str());
                    }
                    //this is an internal option
                    internal.emplace_back(std::move(option));
                }
                else {
                    //rdkafka option
                    external.emplace_back(std::move(option));
                }
            }
        }
        else {
            external.assign(std::make_move_iterator(config.begin()),
                            std::make_move_iterator(config.end()));
        }
    };
    
    // Consumer/Producer options parsing
    const OptionSet& internalOptions = (_type == KafkaType::Producer) ?
        ProducerConfiguration::s_internalOptions : ConsumerConfiguration::s_internalOptions;
    parse(internalOptions, _internalConfig, _config, config);
    
    // Topic options parsing
    const OptionSet& internalTopicOptions = (_type == KafkaType::Producer) ?
        ProducerConfiguration::s_internalTopicOptions : ConsumerConfiguration::s_internalTopicOptions;
    parse(internalTopicOptions, _internalTopicConfig, _topicConfig, topicConfig);
}

}
}
