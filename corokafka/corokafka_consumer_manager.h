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

namespace Bloomberg {
namespace corokafka {

class ConsumerManagerImpl;

/**
 * @brief The ConsumerManager is the object through which consumers commit messages as well
 *        as control the behavior of the message processing by pausing, resuming, etc.
 *        Unlike the ProducerManager, the consumers receive messages asynchronously via the
 *        receiver callback, which means they don't need to interface with this class.
 * @note Committing messages can also be done via the message object itself.
 */
class ConsumerManager
{
public:
    /**
     * @brief Pause all consumption from this topic.
     * @param topic The topic name. If the topic is not specified, the function will apply to all topics.
     */
    void pause();
    void pause(const std::string& topic);
    
    /**
     * @brief Resume all consumption from this topic.
     * @param topic The topic name. If the topic is not specified, the function will apply to all topics.
     */
    void resume();
    void resume(const std::string& topic);
    
    /**
     * @brief Subscribe a previously unsubscribed consumer.
     * @param topic The topic name. If the topic is not specified, the function will apply to all topics.
     * @param partitionList The optional partition list assignment.
     * @note This method will only work if the consumer was been previously 'unsubscribed'. All the original
     *       configuration settings will remain the same, including PartitionStrategy (Static or Dynamic).
     *       If a partitionList is not provided, the values specified via ConsumerConfiguration::assignInitialPartitions()
     *       shall be used with offsets set to RD_KAFKA_OFFSET_INVALID.
     */
    void subscribe(const std::string& topic,
                   const cppkafka::TopicPartitionList& partitionList);
    void subscribe(const cppkafka::TopicPartitionList& partitionList);
    
    /**
     * @brief Unsubscribe from this topic.
     * @param topic The topic name. If the topic is not specified, the function will apply to all topics.
     */
    void unsubscribe();
    void unsubscribe(const std::string& topic);
    
    /**
     * @brief Commits an offset. The behavior of this function depends on the 'internal.consumer.offset.persist.strategy' value.
     * @param topicPartition(s) The offset(s) to commit. Must have a valid topic, partition and offset or just a topic (see note).
     * @param opaque Pointer which will be passed as-is via the 'OffsetCommitCallback'.
     * @param execMode If specified, overrides the 'internal.consumer.commit.exec' setting.
     * @return Error object. If the number of retries reach 0, error contains RD_KAFKA_RESP_ERR__FAIL.
     * @note If only the topic is supplied, this API will commit all offsets in the current partition assignment.
     *       If a partition list is supplied, all partitions must belong to the same topic.
     * @note If 'internal.consumer.offset.persist.strategy=store' and 'execMode=Sync', this function will perform
     *       a synchronous commit instead of storing the offset. This is equivalent to having
     *       'internal.consumer.offset.persist.strategy=commit'.
     * @warning If this method is used, 'internal.consumer.auto.offset.persist' must be set to 'false' and NO commits
     *          should be made via the ReceivedMessage::commit() API.
     */
    cppkafka::Error commit(const cppkafka::TopicPartition& topicPartition,
                           const void* opaque = nullptr);
    cppkafka::Error commit(const cppkafka::TopicPartition& topicPartition,
                           ExecMode execMode,
                           const void* opaque = nullptr);
    cppkafka::Error commit(const cppkafka::TopicPartitionList& topicPartitions,
                           ExecMode execMode,
                           const void* opaque = nullptr);
    cppkafka::Error commit(const cppkafka::TopicPartitionList& topicPartitions,
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
     * @brief Enable/disable the preprocessor callback (if registered).
     * @param topic The topic name. If not set, this operation will apply to all topics.
     * @remark Note that the preprocessor is enabled by default
     */
    void enablePreprocessing();
    void enablePreprocessing(const std::string& topic);
    void disablePreprocessing();
    void disablePreprocessing(const std::string& topic);
    
    /**
     * @brief Get the configuration associated with this topic.
     * @param topic The topic.
     * @return A reference to the configuration.
     */
    const ConsumerConfiguration& getConfiguration(const std::string& topic) const;
    
    /**
     * @brief Get all the managed topics
     * @return The topic list.
     */
    std::vector<std::string> getTopics() const;
    
protected:
    using ConfigMap = ConfigurationBuilder::ConfigMap<ConsumerConfiguration>;
    ConsumerManager(quantum::Dispatcher& dispatcher,
                    const ConnectorConfiguration& connectorConfiguration,
                    const ConfigMap& config,
                    std::atomic_bool& interrupt);
    
    ConsumerManager(quantum::Dispatcher& dispatcher,
                    const ConnectorConfiguration& connectorConfiguration,
                    ConfigMap&& config,
                    std::atomic_bool& interrupt);
    
    virtual ~ConsumerManager() = default;
    
    void poll();
    
    void pollEnd();
    
private:
    std::unique_ptr<ConsumerManagerImpl>  _impl;
};

}}

#endif //BLOOMBERG_COROKAFKA_CONSUMER_MANAGER_H
