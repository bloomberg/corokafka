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
#include <corokafka/corokafka_exception.h>

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
            "connector": {
                "title": "CoroKafka connector configuration",
                "type": "object",
                "properties": {
                    "options": {
                        "description": "The options for this connector",
                        "$ref" : "#/definitions/option"
                    },
                    "quantum": {
                        "$ref": "bloomberg:quantum.json"
                    },
                },
                "additionalProperties": false,
                "required": []
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
                        "description": "A partition offset. Values are: -1000(stored),-1(end),-2(begin),>=0(exact or relative)",
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
            "partitionConfig": {
                "title": "Partition assignment configuration for a topic.",
                "type": "object",
                "properties": {
                    "strategy": {
                        "description":"Only applies to consumer topic configurations",
                        "type":"string",
                        "enum":["static","dynamic"],
                        "default":"dynamic"
                    },
                    "partitions": {
                        "description":"Only applies to consumer topic configurations",
                        "type":"array",
                        "items": { "$ref" : "#/definitions/partition" }
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
                    },
                    "assignment": {
                        "description": "The partition strategy and assignment (consumers only)",
                        "$ref" : "#/definitions/partitionConfig"
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

Configuration::Configuration(OptionList options)
{
    _options[(int)OptionType::All] = std::move(options);
}

Configuration::Configuration(OptionInitList options)
{
    _options[(int)OptionType::All] = std::move(options);
}

const Configuration::OptionList& Configuration::getOptions(OptionType type) const
{
    return _options[(int)type];
}

const cppkafka::ConfigurationOption* Configuration::getOption(const std::string& name) const
{
    return findOption(name, _options[(int)OptionType::All]);
}

const cppkafka::ConfigurationOption* Configuration::findOption(const std::string& name,
                                                               const OptionList& config)
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

void Configuration::parseOptions(const std::string& topic,
                                 const std::string& optionsPrefix,
                                 const OptionMap& allowed,
                                 OptionList(&optionList)[3],
                                 bool allowRdKafkaOptions)
{
    OptionList& config = optionList[(int)OptionType::All];
    OptionList& rdKafka = optionList[(int)OptionType::RdKafka];
    OptionList& internal = optionList[(int)OptionType::Internal];
    
    if (!allowed.empty()) {
        for (auto& option : config) {
            trim(const_cast<std::string&>(option.get_key()));
            if (option.get_key().empty()) {
                throw InvalidOptionException(topic, "unknown", "Name is empty");
            }
            trim(const_cast<std::string&>(option.get_value()));
            if (option.get_value().empty()) {
                throw InvalidOptionException(topic, option.get_value(), "Value is empty");
            }
            auto it = allowed.find(option.get_key());
            if (it == allowed.end()) {
                //Check if it's an rdkafka option or a misspelled internal option
                if (StringEqualCompare()(option.get_key(), optionsPrefix, optionsPrefix.length())) {
                    //Prefix matches therefore it's a misspelled option
                    throw InvalidOptionException(topic, option.get_key(), "Internal option not found");
                }
                else {
                    //RdKafka option
                    if (!allowRdKafkaOptions) {
                        throw InvalidOptionException(topic, option.get_key(), "Unknown RdKafka option");
                    }
                    rdKafka.emplace_back(option);
                }
            }
            else {
                //validate
                it->second(topic, &option, nullptr);
                //this is an internal option
                internal.emplace_back(option);
            }
        }
    }
    else {
        rdKafka.assign(config.begin(), config.end());
    }
}

bool Configuration::extractBooleanValue(const std::string& topic,
                                        const char* optionName,
                                        const cppkafka::ConfigurationOption& option)
{
    if (StringEqualCompare()(option.get_value(), "true")) {
        return true;
    }
    else if (StringEqualCompare()(option.get_value(), "false")) {
        return false;
    }
    else {
        if (topic.empty()) {
            throw InvalidOptionException(optionName, option.get_value());
        }
        throw InvalidOptionException(topic, optionName, option.get_value());
    }
}

ssize_t Configuration::extractCounterValue(const std::string& topic,
                                           const char* optionName,
                                           const cppkafka::ConfigurationOption& option,
                                           ssize_t minAllowed,
                                           ssize_t maxAllowed)
{
    ssize_t value;
    try {
        value = std::stoll(option.get_value());
    }
    catch (const std::invalid_argument& ex) {
        throw InvalidOptionException(topic, optionName, option.get_value());
    }
    if ((value < minAllowed) || (value > maxAllowed)) {
        if (topic.empty()) {
            throw InvalidOptionException(optionName, option.get_value());
        }
        throw InvalidOptionException(topic, optionName, option.get_value());
    }
    return value;
}

cppkafka::LogLevel Configuration::extractLogLevel(const std::string& topic,
                                                  const char* optionName,
                                                  const std::string& level)
{
    StringEqualCompare compare;
    if (compare(level, "emergency")) {
        return cppkafka::LogLevel::LogEmerg;
    }
    if (compare(level, "alert")) {
        return cppkafka::LogLevel::LogAlert;
    }
    if (compare(level, "critical")) {
        return cppkafka::LogLevel::LogCrit;
    }
    if (compare(level, "error")) {
        return cppkafka::LogLevel::LogErr;
    }
    if (compare(level, "warning")) {
        return cppkafka::LogLevel::LogWarning;
    }
    if (compare(level, "notice")) {
        return cppkafka::LogLevel::LogNotice;
    }
    if (compare(level, "info")) {
        return cppkafka::LogLevel::LogInfo;
    }
    if (compare(level, "debug")) {
        return cppkafka::LogLevel::LogDebug;
    }
    throw InvalidOptionException(topic, optionName, level);
}

}
}
