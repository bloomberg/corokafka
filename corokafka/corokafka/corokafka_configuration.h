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

namespace Bloomberg {
namespace corokafka {

//========================================================================
//                             CONFIGURATION
//========================================================================
class Configuration {
    friend class ConsumerManagerImpl;
    friend class ProducerManagerImpl;
public:
    using Options = std::vector<ConfigurationOption>;
    using OptionSet = std::set<std::string, StringLessCompare>;
    
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
     * @brief Get the configuration type.
     * @return The type.
     */
    KafkaType configType() const;
    
    /**
     * @brief Get the topic associated with this configuration.
     * @return The topic name.
     */
    const std::string& getTopic() const;
    
    /**
     * @brief Get the librdkafka configuration options list.
     * @return The configuration options.
     */
    const Options& getConfiguration() const;
    
    /**
     * @brief Get the topic configuration options list.
     * @return The topic configuration options.
     */
    const Options& getTopicConfiguration() const;
    
    /**
     * @brief Set the error callback.
     * @param callback The callback.
     */
    void setCallback(Callbacks::ErrorCallback callback);
    
    /**
     * @brief Set the throttle callback.
     * @param callback The callback.
     * @remark All consumers and producers are automatically throttled by this library.
     *         As such using this callback is optional and discretionary.
     */
    void setCallback(Callbacks::ThrottleCallback callback);
    
    /**
     * @brief Set the log callback.
     * @param callback The callback.
     */
    void setCallback(Callbacks::LogCallback callback);
    
    /**
     * @brief Set the statistics callback.
     * @param callback The callback.
     */
    void setCallback(Callbacks::StatsCallback callback);
    
    /**
     * @brief Get the error callback.
     * @return The callback.
     */
    const Callbacks::ErrorCallback& getErrorCallback() const;
    
    /**
     * @brief Get the throttle callback.
     * @return The callback.
     */
    const Callbacks::ThrottleCallback& getThrottleCallback() const;
    
    /**
     * @brief Get the log callback.
     * @return The callback.
     */
    const Callbacks::LogCallback& getLogCallback() const;
    
    /**
     * @brief Get the statistics callback.
     * @return The callback.
     */
    const Callbacks::StatsCallback& getStatsCallback() const;
    
    /**
     * @brief Gets the value for a certain configuration.
     * @param name The name of the configuration option.
     * @return A pointer to the configuration object or null if it's not found.
     */
    const ConfigurationOption* getConfiguration(const std::string& name) const;
    
    /**
     * @brief Gets the value for a certain topic configuration.
     * @param name The name of the topic configuration option.
     * @return A pointer to the configuration object or null if it's not found.
     */
    const ConfigurationOption* getTopicConfiguration(const std::string& name) const;
    
    /**
     * @brief Comparison operator for ordered containers.
     * @param other The other configuration object to compare to.
     * @return True if less, False otherwise.
     */
    bool operator<(const Configuration& other) const;
    
protected:
    Configuration() = default;
    
    Configuration(KafkaType type,
                  const std::string& topic,
                  Options config,
                  Options topicConfig);
    
    Configuration(KafkaType type,
                  const std::string& topic,
                  std::initializer_list<ConfigurationOption> config,
                  std::initializer_list<ConfigurationOption> topicConfig);
    
    Configuration(const Configuration&) = default;
    Configuration(Configuration&&) = default;
    Configuration& operator=(const Configuration&) = default;
    Configuration& operator=(Configuration&&) = default;
    virtual ~Configuration() = default;
    
private:
    static const ConfigurationOption* findConfig(const std::string& name,
                                                 const Options& config);
        
    void filterOptions(Options&& config,
                       Options&& topicConfig);
    
    const Options& getInternalConfiguration() const;
    
    const Options& getInternalTopicConfiguration() const;
    
    KafkaType                           _type;
    std::string                         _topic;
    Options                             _config;
    Options                             _topicConfig;
    Options                             _internalConfig;
    Options                             _internalTopicConfig;
    Callbacks::ErrorCallback            _errorCallback;
    Callbacks::ThrottleCallback         _throttleCallback;
    Callbacks::LogCallback              _logCallback;
    Callbacks::StatsCallback            _statsCallback;
};

} // corokafka
} // Bloomberg

#endif //BLOOMBERG_COROKAFKA_CONFIGURATION_H
