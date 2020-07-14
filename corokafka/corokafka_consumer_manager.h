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

#include <corokafka/interface/corokafka_impl.h>
#include <corokafka/interface/corokafka_iconsumer_manager.h>
#include <corokafka/corokafka_configuration_builder.h>
#include <quantum/quantum.h>
#include <map>

namespace Bloomberg {
namespace corokafka {

namespace mocks {
    struct ConnectorMock;
}

/**
 * @brief The ConsumerManager is the object through which consumers commit messages as well
 *        as control the behavior of the message processing by pausing, resuming, etc.
 *        Unlike the ProducerManager, the consumers receive messages asynchronously via the
 *        receiver callback, which means they don't need to interface with this class.
 * @note Committing messages can also be done via the message object itself.
 */
class ConsumerManager : public Impl<IConsumerManager>
{
public:
    /**
     * @brief Pause all consumption from this topic.
     * @param topic The topic name. If the topic is not specified, the function will apply to all topics.
     */
    void pause() final;
    void pause(const std::string& topic) final;
    
    /**
     * @brief Resume all consumption from this topic.
     * @param topic The topic name. If the topic is not specified, the function will apply to all topics.
     */
    void resume() final;
    void resume(const std::string& topic) final;
    
    /**
     * @brief Subscribe a previously unsubscribed consumer.
     * @param topic The topic name. If the topic is not specified, the function will apply to all topics.
     * @param partitionList The optional partition list assignment.
     * @note This method will only work if the consumer was been previously 'unsubscribed'. All the original
     *       configuration settings will remain the same, including PartitionStrategy (Static or Dynamic).
     *       If a partitionList is empty, the values specified via ConsumerConfiguration::assignInitialPartitions()
     *       shall be used.
     */
    void subscribe(const std::string& topic,
                   const cppkafka::TopicPartitionList& partitionList) final;
    void subscribe(const cppkafka::TopicPartitionList& partitionList) final;
    
    /**
     * @brief Unsubscribe from this topic.
     * @param topic The topic name. If the topic is not specified, the function will apply to all topics.
     */
    void unsubscribe() final;
    void unsubscribe(const std::string& topic) final;
    
    /**
     * @brief Commits an offset. The behavior of this function depends on the 'internal.consumer.offset.persist.strategy' value.
     * @param topicPartition(s) The offset(s) to commit. Must have a valid topic, partition and offset or just a topic (see note).
     * @param opaque Pointer which will be passed as-is via the 'OffsetCommitCallback'.
     * @param execMode If specified, overrides the 'internal.consumer.commit.exec' setting.
     * @return Error object. If the number of retries reach 0, error contains RD_KAFKA_RESP_ERR__FAIL.
     * @note If only the topic is supplied, this API will commit all offsets in the current partition assignment.
     * @note If 'internal.consumer.offset.persist.strategy=store' and 'execMode=Sync', this function will perform
     *       a synchronous commit instead of storing the offset. This is equivalent to having
     *       'internal.consumer.offset.persist.strategy=commit'.
     * @warning If this method is used, 'internal.consumer.auto.offset.persist' must be set to 'false' and NO commits
     *          should be made via the ReceivedMessage::commit() API.
     */
    cppkafka::Error commit(const cppkafka::TopicPartition& topicPartition,
                           const void* opaque = nullptr) final;
    cppkafka::Error commit(const cppkafka::TopicPartition& topicPartition,
                           ExecMode execMode,
                           const void* opaque = nullptr) final;
    cppkafka::Error commit(const cppkafka::TopicPartitionList& topicPartitions,
                           ExecMode execMode,
                           const void* opaque = nullptr) final;
    cppkafka::Error commit(const cppkafka::TopicPartitionList& topicPartitions,
                           const void* opaque = nullptr) final;
    
    /**
     * @brief Gracefully shut down all consumers and unsubscribe from all topics.
     * @remark Note that this method is automatically called in the destructor.
     */
    void shutdown() final;
    
    /**
     * @brief Get Kafka metadata associated with this topic
     * @param topic The topic to query
     * @return The metadata object
     */
    ConsumerMetadata getMetadata(const std::string& topic) final;
    
    /**
     * @brief Enable/disable the preprocessor callback (if registered).
     * @param topic The topic name. If not set, this operation will apply to all topics.
     * @remark Note that the preprocessor is enabled by default
     */
    void enablePreprocessing() final;
    void enablePreprocessing(const std::string& topic) final;
    void disablePreprocessing() final;
    void disablePreprocessing(const std::string& topic) final;
    
    /**
     * @brief Get the configuration associated with this topic.
     * @param topic The topic.
     * @return A reference to the configuration.
     */
    const ConsumerConfiguration& getConfiguration(const std::string& topic) const final;
    
    /**
     * @brief Get all the managed topics
     * @return The topic list.
     */
    std::vector<std::string> getTopics() const final;
    
private:
    friend class ConnectorImpl;
    friend struct mocks::ConnectorMock;
    using ImplType = Impl<IConsumerManager>;
    using ConfigMap = ConfigurationBuilder::ConfigMap<ConsumerConfiguration>;
    
    ConsumerManager(quantum::Dispatcher& dispatcher,
                    const ConnectorConfiguration& connectorConfiguration,
                    const ConfigMap& config,
                    std::atomic_bool& interrupt);
    
    ConsumerManager(quantum::Dispatcher& dispatcher,
                    const ConnectorConfiguration& connectorConfiguration,
                    ConfigMap&& config,
                    std::atomic_bool& interrupt);
    
    /**
     * @brief For mocking only via dependency injection
     */
    using ImplType::ImplType;
    
    void poll() final;
    
    void pollEnd() final;
};

}}

#endif //BLOOMBERG_COROKAFKA_CONSUMER_MANAGER_H
