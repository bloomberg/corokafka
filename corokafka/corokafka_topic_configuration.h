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
#ifndef BLOOMBERG_COROKAFKA_TOPIC_CONFIGURATION_H
#define BLOOMBERG_COROKAFKA_TOPIC_CONFIGURATION_H

#include <corokafka/corokafka_configuration.h>

namespace Bloomberg {
namespace corokafka {

//========================================================================
//                           TOPIC CONFIGURATION
//========================================================================
class TopicConfiguration : public Configuration
{
public:
    /**
     * @brief Internal CoroKafka-specific options for topics. They are used to control this
     *        library's behavior for topics and are complementary to the RdKafka topic options.
     *        For more details please read CONFIGURATION.md document.
     */
    struct Options {
        static constexpr const char *brokerTimeoutMs = "internal.topic.broker.timeout.ms";
    };
    
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
     * @brief Gets the value for a specific topic configuration.
     * @param name The name of the topic configuration option.
     * @return A pointer to the configuration object or null if it's not found.
     */
    const cppkafka::ConfigurationOption* getTopicOption(const std::string& name) const;
    
    /**
     * @brief Get the topic options list.
     * @type The option type.
     * @return The topic configuration options.
     */
    const OptionList& getTopicOptions(OptionType type = OptionType::All) const;
    
    /**
     * @brief Set the error callback.
     * @param callback The callback.
     * @param opaque An application-managed pointer which shall be passed when the callback is invoked.
     */
    void setErrorCallback(Callbacks::ErrorCallback callback,
                          const void* opaque = nullptr);
    
    /**
     * @brief Set the throttle callback.
     * @param callback The callback.
     * @remark All consumers and producers are automatically throttled by this library.
     *         As such using this callback is optional and discretionary.
     */
    void setThrottleCallback(Callbacks::ThrottleCallback callback);
    
    /**
     * @brief Set the log callback.
     * @param callback The callback.
     */
    void setLogCallback(Callbacks::LogCallback callback);
    
    /**
     * @brief Set the statistics callback.
     * @param callback The callback.
     */
    void setStatsCallback(Callbacks::StatsCallback callback);
    
    /**
     * @brief Get the error callback.
     * @return The callback.
     */
    const Callbacks::ErrorCallback& getErrorCallback() const;
    
    /**
     * @brief Get the opaque pointer (if any) for the error callback.
     * @return The application-managed data pointer.
     */
    const void* getErrorCallbackOpaque() const;
    
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
     * @brief Comparison operator for ordered containers.
     * @param other The other configuration object to compare to.
     * @return True if less, False otherwise.
     */
    bool operator<(const TopicConfiguration& other) const;
    
protected:
    friend class ConsumerManagerImpl;
    friend class ProducerManagerImpl;
    
    TopicConfiguration() = default;
    TopicConfiguration(KafkaType type,
                       const std::string& topic,
                       OptionList options,
                       OptionList topicOptions);
    TopicConfiguration(KafkaType type,
                       const std::string& topic,
                       std::initializer_list<cppkafka::ConfigurationOption> options,
                       std::initializer_list<cppkafka::ConfigurationOption> topicOptions);
    
    static const OptionMap              s_internalTopicOptions;
    static const std::string            s_internalTopicOptionsPrefix;
    
private:
    void filterOptions();
    
    KafkaType                           _type;
    std::string                         _topic;
    std::array<OptionList, 3>           _topicOptions;
    Callbacks::ErrorCallback            _errorCallback;
    const void*                         _errorOpaque{nullptr};
    Callbacks::ThrottleCallback         _throttleCallback;
    Callbacks::LogCallback              _logCallback;
    Callbacks::StatsCallback            _statsCallback;
};

}
}

#endif //BLOOMBERG_COROKAFKA_TOPIC_CONFIGURATION_H
