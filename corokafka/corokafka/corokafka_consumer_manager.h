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
#ifndef BLOOMBERG_COROKAFKA_CONSUMER_MANAGER_H
#define BLOOMBERG_COROKAFKA_CONSUMER_MANAGER_H

#include <vector>
#include <map>
#include <chrono>
#include <corokafka/corokafka_utils.h>
#include <corokafka/corokafka_metadata.h>
#include <corokafka/corokafka_configuration_builder.h>
#include <quantum/quantum.h>

using namespace BloombergLP;

namespace Bloomberg {
namespace corokafka {

class ConsumerManagerImpl;

class ConsumerManager
{
public:
    /**
     * @brief Pause all consumption from this topic
     * @param topic The topic name
     */
    void pause(const std::string& topic = {});
    
    /**
     * @brief Resume all consumption from this topic
     * @param topic The topic name
     */
    void resume(const std::string& topic = {});
    
    /**
     * @brief Subscribe a previously unsubscribed consumer.
     * @param topic The topic name.
     * @param partitionList The optional partition list assignment.
     * @note This method will only work if the consumer was been previously 'unsubscribed'. All the original
     *       configuration settings will remain the same, including PartitionStrategy (Static or Dynamic).
     *       If a partionList is not provided, the values specified via ConsumerConfiguration::assignInitialPartitions()
     *       shall be used.
     */
    void subscribe(const std::string& topic,
                   TopicPartitionList partitionList = {});
    
    /**
     * @brief Unsubscribe from this topic.
     * @param topic The topic name.
     */
    void unsubscribe(const std::string& topic = {});
    
    /**
     * @brief Commits an offset. The behavior of this function depends on the 'internal.consumer.offset.persist.strategy' value.
     * @param topicPartition The offset to commit.
     * @param opaque Pointer which will be passed as-is via the 'OffsetCommitCallback'.
     * @warning If this method is used, 'internal.consumer.auto.offset.persist' must be set to 'false' and NO commits
     *          should be made via the ReceivedMessage API.
     */
    void commit(const TopicPartition& topicPartition,
                const void* opaque = nullptr);
    
    /**
     * @brief Similar to the above commit() but supporting a list of partitions.
     * @param topicPartitions Partitions on the *same* topic.
     * @param opaque Pointer which will be passed as-is via the 'OffsetCommitCallback'.
     */
    void commit(const TopicPartitionList& topicPartitions,
                const void* opaque = nullptr);
    
    /**
     * @brief Gracefully shut down all consumers and unsubscribe from all topics.
     * @remark Note that this method is automatically called in the destructor.
     */
    void shutdown();
    
    /**
     * @brief Get Kafka metadata associated with this topic
     * @param topic The topic to query
     * @return The metadata object
     */
    ConsumerMetadata getMetadata(const std::string& topic);
    
    /**
     * @brief Enables or disables the preprocessor callback (if registered)
     * @param enable True to enable, false to disable
     * @param topic The topic name
     * @remark Note that the preprocessor is enabled by default
     */
    void preprocess(bool enable, const std::string& topic = {});
    
protected:
    using ConfigMap = ConfigurationBuilder::ConfigMap<ConsumerConfiguration>;
    ConsumerManager(quantum::Dispatcher& dispatcher,
                    const ConnectorConfiguration& connectorConfiguration,
                    const ConfigMap& config);
    
    ConsumerManager(quantum::Dispatcher& dispatcher,
                    const ConnectorConfiguration& connectorConfiguration,
                    ConfigMap&& config);
    
    virtual ~ConsumerManager();
    
    void poll();
    
private:
    std::unique_ptr<ConsumerManagerImpl>  _impl;
};

}}

#endif //BLOOMBERG_COROKAFKA_CONSUMER_MANAGER_H
