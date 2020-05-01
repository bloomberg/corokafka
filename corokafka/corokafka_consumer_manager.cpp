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
#include <corokafka/corokafka_consumer_manager.h>
#include <corokafka/impl/corokafka_consumer_manager_impl.h>
#include <memory>

namespace Bloomberg {
namespace corokafka {

ConsumerManager::ConsumerManager(quantum::Dispatcher& dispatcher,
                                 const ConnectorConfiguration& connectorConfiguration,
                                 const ConfigMap& config,
                                 std::atomic_bool& interrupt) :
    ImplType(std::make_shared<ConsumerManagerImpl>(dispatcher, connectorConfiguration, config, interrupt))
{
}

ConsumerManager::ConsumerManager(quantum::Dispatcher& dispatcher,
                                 const ConnectorConfiguration& connectorConfiguration,
                                 ConfigMap&& config,
                                 std::atomic_bool& interrupt) :
    ImplType(std::make_shared<ConsumerManagerImpl>(dispatcher, connectorConfiguration, std::move(config), interrupt))
{
}

ConsumerMetadata ConsumerManager::getMetadata(const std::string& topic)
{
    return impl()->getMetadata(topic);
}

void ConsumerManager::enablePreprocessing()
{
    impl()->enablePreprocessing();
}

void ConsumerManager::enablePreprocessing(const std::string& topic)
{
    impl()->enablePreprocessing(topic);
}

void ConsumerManager::disablePreprocessing()
{
    impl()->disablePreprocessing();
}

void ConsumerManager::disablePreprocessing(const std::string& topic)
{
    impl()->disablePreprocessing(topic);
}

void ConsumerManager::pause()
{
    impl()->pause();
}

void ConsumerManager::pause(const std::string& topic)
{
    impl()->pause(topic);
}

void ConsumerManager::resume()
{
    impl()->resume();
}

void ConsumerManager::resume(const std::string& topic)
{
    impl()->resume(topic);
}

void ConsumerManager::subscribe(const cppkafka::TopicPartitionList& partitionList)
{
    impl()->subscribe(partitionList);
}

void ConsumerManager::subscribe(const std::string& topic,
                                const cppkafka::TopicPartitionList& partitionList)
{
    impl()->subscribe(topic, partitionList);
}

void ConsumerManager::unsubscribe()
{
    impl()->unsubscribe();
}

void ConsumerManager::unsubscribe(const std::string& topic)
{
    impl()->unsubscribe(topic);
}

cppkafka::Error ConsumerManager::commit(const cppkafka::TopicPartition& topicPartition,
                                        ExecMode execMode,
                                        const void* opaque)
{
    return impl()->commit(topicPartition, execMode, opaque);
}

cppkafka::Error ConsumerManager::commit(const cppkafka::TopicPartition& topicPartition,
                                        const void* opaque)
{
    return impl()->commit(topicPartition, opaque);
}

cppkafka::Error ConsumerManager::commit(const cppkafka::TopicPartitionList& topicPartitions,
                                        ExecMode execMode,
                                        const void* opaque)
{
    return impl()->commit(topicPartitions, execMode, opaque);
}

cppkafka::Error ConsumerManager::commit(const cppkafka::TopicPartitionList& topicPartitions,
                                        const void* opaque)
{
    return impl()->commit(topicPartitions, opaque);
}

void ConsumerManager::shutdown()
{
    impl()->shutdown();
}

void ConsumerManager::poll()
{
    impl()->poll();
}

void ConsumerManager::pollEnd()
{
    impl()->pollEnd();
}

const ConsumerConfiguration& ConsumerManager::getConfiguration(const std::string& topic) const
{
    return impl()->getConfiguration(topic);
}

std::vector<std::string> ConsumerManager::getTopics() const
{
    return impl()->getTopics();
}

}
}
