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
#ifndef BLOOMBERG_COROKAFKA_CONFIGURATION_H
#define BLOOMBERG_COROKAFKA_CONFIGURATION_H

#include <corokafka/corokafka_callbacks.h>
#include <corokafka/corokafka_utils.h>
#include <set>
#include <initializer_list>
#include <limits>
#include <functional>
#include <array>

namespace Bloomberg {
namespace corokafka {

//========================================================================
//                           CONFIGURATION
//========================================================================
class Configuration
{
public:
    using OptionList = std::vector<cppkafka::ConfigurationOption>;
    using OptionInitList = std::initializer_list<cppkafka::ConfigurationOption>;
    enum class OptionType : int { All = 0, RdKafka = 1, Internal = 2 };
    
    /**
     * @brief Get the JSON schema corresponding to this configuration object.
     * @return The draft-04 compatible schema.
     */
    static const std::string& getJsonSchema();
    
    /**
     * @brief Get the schema URI used to resolve remote JSON references '$ref'.
     * @return The URI.
     */
    static const std::string& getJsonSchemaUri();
    
    /**
     * @brief Gets the value for a specific configuration.
     * @param name The name of the configuration option.
     * @return A pointer to the configuration object or null if it's not found.
     */
    const cppkafka::ConfigurationOption* getOption(const std::string& name) const;
    
    /**
     * @brief Get the producer/consumer options list.
     * @type The option type.
     * @return The configuration options.
     */
    const OptionList& getOptions(OptionType type = OptionType::All) const;
    
protected:
    friend class ConsumerManagerImpl;
    friend class ProducerManagerImpl;
    friend class ConnectorImpl;
    /**
     * @brief: RdKafka options modified by this library
     */
    struct RdKafkaOptions
    {
        static constexpr const char* metadataBrokerList =       "metadata.broker.list";
        //consumer
        static constexpr const char* groupId =                  "group.id";
        static constexpr const char* enableAutoOffsetStore =    "enable.auto.offset.store";
        static constexpr const char* enableAutoCommit =         "enable.auto.commit";
        static constexpr const char* autoCommitIntervalMs =     "auto.commit.interval.ms";
        //producer
        static constexpr const char* maxInFlight =              "max.in.flight";
        static constexpr const char* messageSendMaxRetries =    "message.send.max.retries";
        static constexpr const char* enableIdempotence =        "enable.idempotence";
        static constexpr const char* enablePartitionEof =       "enable.partition.eof";
    };

    using OptionExtractorFunc = std::function<bool(const std::string& topic,
                                                   const cppkafka::ConfigurationOption*,
                                                   void*)>;
    using OptionMap = std::map<std::string, OptionExtractorFunc, StringLessCompare>;
    
    Configuration() = default;
    explicit Configuration(OptionList options);
    Configuration(OptionInitList options);
    virtual ~Configuration() = default;
    
    static const cppkafka::ConfigurationOption* findOption(const std::string& name,
                                                           const OptionList& config);
    static void parseOptions(const std::string& topic,
                             const std::string& optionsPrefix,
                             const OptionMap& allowed,
                             std::array<OptionList, 3>& optionList,
                             OptionsPermission enablement);
    
    static bool extractBooleanValue(const std::string& topic,
                                    const char* optionName,
                                    const cppkafka::ConfigurationOption& option);
    static ssize_t extractCounterValue(const std::string& topic,
                                       const char* optionName,
                                       const cppkafka::ConfigurationOption& option,
                                       ssize_t minAllowed = 0,
                                       ssize_t maxAllowed = std::numeric_limits<ssize_t>::max());
    static cppkafka::LogLevel extractLogLevel(const std::string& topic,
                                              const char* optionName,
                                              const std::string &level);
    static const OptionExtractorFunc& extractOption(const OptionMap& options,
                                                    const OptionMap& topicOptions,
                                                    const std::string& option);
    
    // Members
    std::array<OptionList, 3>  _options; //indexed by OptionType
};

} // corokafka
} // Bloomberg

#endif //BLOOMBERG_COROKAFKA_CONFIGURATION_H
