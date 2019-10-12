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
                    "ids": {
                        "description" : "Partition id(s). Empty = all partitions, one value = single partition, two values = range [first, second]",
                        "type":"array",
                        "items": { "type": "number" },
                        "minItems": 0,
                        "maxItems": 2,
                        "uniqueItems": true
                    },
                    "offset": {
                        "description": "A partition offset. Values are: -1000(stored),-1(begin),-2(end),>=0(exact or relative)",
                        "type":"number",
                        "default":-1000
                    },
                    "relative": {
                        "description": "If true, the offset represents the Nth message before the stored offset (i.e. stored-N).",
                        "type":"boolean",
                        "default": false
                    }
                },
                "additionalProperties": false,
                "required": []
            },
            "topicConfig": {
                "title": "Consumer or producer topic configuration",
                "type": "object",
                "properties": {
                    "name": {
                        "description": "The name of this configuration object",
                        "type":"string"
                    },
                    "type": {
                        "type":"string",
                        "enum": ["producer", "consumer"]
                    },
                    "options": {
                        "description": "The rdkafka and corokafka options for this consumer/producer. Must at least contain 'metadata.broker.list'",
                        "$ref" : "#/definitions/option"
                    },
                    "topicOptions": {
                        "description": "The rdkafka and corokafka topic options for this consumer/producer",
                        "$ref" : "#/definitions/option"
                    },
                    "partitionStrategy": {
                        "description":"Only applies to consumer topic configurations",
                        "type":"string",
                        "enum":["static","dynamic"],
                        "default":"dynamic"
                    },
                    "partitionAssignment": {
                        "description":"Only applies to consumer topic configurations",
                        "type":"array",
                        "items": { "$ref" : "#/definitions/partition" }
                    }
                },
                "additionalProperties": false,
                "required": ["name","type"]
            },
            "topic": {
                "title": "Consumer or producer topic",
                "type": "object",
                "properties": {
                    "name": {
                        "description": "The name of this topic",
                        "type":"string"
                    },
                    "config": {
                        "description": "The config for this topic",
                        "type":"string"
                    }
                },
                "additionalProperties": false,
                "required": ["name","config"]
            }
        },

        "title": "Kafka connector settings",
        "type": "object",
        "properties": {
            "connector": { "$ref":"#/definitions/connector" },
            "topicConfigs": {
                "type":"array",
                "items": { "$ref": "#/definitions/topicConfig" },
                "minItems": 1,
                "uniqueItems": true
            },
            "topics": {
                "type":"array",
                "items": { "$ref": "#/definitions/topic" },
                "minItems": 1,
                "uniqueItems": false
            }
        },
        "additionalProperties": false,
        "required": [ "topics","topicConfigs" ]
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
                             Options options,
                             Options topicOptions) :
    _type(type),
    _topic(topic)
{
    _options[(int)OptionType::All] = std::move(options);
    _topicOptions[(int)OptionType::All] = std::move(topicOptions);
    filterOptions();
}

Configuration::Configuration(KafkaType type,
                             const std::string& topic,
                             std::initializer_list<cppkafka::ConfigurationOption> options,
                             std::initializer_list<cppkafka::ConfigurationOption> topicOptions) :
    _type(type),
    _topic(topic)
{
    _options[(int)OptionType::All] = std::move(options);
    _topicOptions[(int)OptionType::All] = std::move(topicOptions);
    filterOptions();
}

KafkaType Configuration::configType() const
{
    return _type;
}

const std::string& Configuration::getTopic() const
{
    return _topic;
}

const Configuration::Options& Configuration::getOptions(OptionType type) const
{
    return _options[(int)type];
}

const Configuration::Options& Configuration::getTopicOptions(OptionType type) const
{
    return _topicOptions[(int)type];
}

void Configuration::setErrorCallback(Callbacks::ErrorCallback callback)
{
    _errorCallback = std::move(callback);
}

void Configuration::setThrottleCallback(Callbacks::ThrottleCallback callback)
{
    _throttleCallback = std::move(callback);
}

void Configuration::setLogCallback(Callbacks::LogCallback callback)
{
    _logCallback = std::move(callback);
}

void Configuration::setStatsCallback(Callbacks::StatsCallback callback)
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

const cppkafka::ConfigurationOption* Configuration::getOption(const std::string& name) const
{
    return findOption(name, _options[(int)OptionType::All]);
}

const cppkafka::ConfigurationOption* Configuration::getTopicOption(const std::string& name) const
{
    return findOption(name, _topicOptions[(int)OptionType::All]);
}

const cppkafka::ConfigurationOption* Configuration::findOption(const std::string& name,
                                                     const Options& config)
{
    const auto it = std::find_if(config.cbegin(), config.cend(),
                                 [&name](const cppkafka::ConfigurationOption& config)->bool {
        return StringEqualCompare()(config.get_key(), name);
    });
    if (it != config.cend()) {
        return &*it;
    }
    return nullptr;
}

void Configuration::filterOptions()
{
    const std::string& internalOptionsPrefix = (_type == KafkaType::Producer) ?
        ProducerConfiguration::s_internalOptionsPrefix : ConsumerConfiguration::s_internalOptionsPrefix;
    
    auto parse = [&internalOptionsPrefix](const OptionSet& allowed, Options& internal, Options& external, const Options& config)
    {
        if (!allowed.empty()) {
            for (const auto& option : config) {
                if (StringEqualCompare()(option.get_key(), internalOptionsPrefix, internalOptionsPrefix.length())) {
                    auto it = allowed.find(option.get_key());
                    if (it == allowed.end()) {
                        std::ostringstream oss;
                        oss << "Invalid option found: " << option.get_key();
                        throw std::runtime_error(oss.str().c_str());
                    }
                    //this is an internal option
                    internal.emplace_back(option);
                }
                else {
                    //rdkafka option
                    external.emplace_back(option);
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
    parse(internalOptions,
          _options[(int)OptionType::Internal],
          _options[(int)OptionType::RdKafka],
          _options[(int)OptionType::All]);
    
    // Topic options parsing
    const OptionSet& internalTopicOptions = (_type == KafkaType::Producer) ?
        ProducerConfiguration::s_internalTopicOptions : ConsumerConfiguration::s_internalTopicOptions;
    parse(internalTopicOptions,
          _topicOptions[(int)OptionType::Internal],
          _topicOptions[(int)OptionType::RdKafka],
          _topicOptions[(int)OptionType::All]);
}

}
}
