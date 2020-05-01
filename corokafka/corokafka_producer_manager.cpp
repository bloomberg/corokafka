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
#include <corokafka/impl/corokafka_producer_manager_impl.h>
#include <corokafka/corokafka_producer_manager.h>
#include <memory>

namespace Bloomberg {
namespace corokafka {

ProducerManager::ProducerManager(quantum::Dispatcher& dispatcher,
                                 const ConnectorConfiguration& connectorConfiguration,
                                 const ConfigMap& config,
                                 std::atomic_bool& interrupt) :
    ImplType(std::make_shared<ProducerManagerImpl>(dispatcher, connectorConfiguration, config, interrupt)),
    _producerPtr(static_cast<ProducerManagerImpl*>(impl().get()))
{
}

ProducerManager::ProducerManager(quantum::Dispatcher& dispatcher,
                                 const ConnectorConfiguration& connectorConfiguration,
                                 ConfigMap&& config,
                                 std::atomic_bool& interrupt) :
    ImplType(std::make_shared<ProducerManagerImpl>(dispatcher, connectorConfiguration, std::move(config), interrupt)),
    _producerPtr(static_cast<ProducerManagerImpl*>(impl().get()))
{
}

ProducerMetadata ProducerManager::getMetadata(const std::string& topic)
{
    return impl()->getMetadata(topic);
}

const ProducerConfiguration& ProducerManager::getConfiguration(const std::string& topic) const
{
    return impl()->getConfiguration(topic);
}

std::vector<std::string> ProducerManager::getTopics() const
{
    return impl()->getTopics();
}

void ProducerManager::resetQueueFullTrigger(const std::string& topic)
{
    return impl()->resetQueueFullTrigger(topic);
}

bool ProducerManager::waitForAcks(const std::string& topic)
{
    return impl()->waitForAcks(topic);
}

bool ProducerManager::waitForAcks(const std::string& topic,
                                  std::chrono::milliseconds timeout)
{
    return impl()->waitForAcks(topic, timeout);
}

void ProducerManager::shutdown()
{
    impl()->shutdown();
}

void ProducerManager::poll()
{
    impl()->poll();
}

void ProducerManager::pollEnd()
{
    impl()->pollEnd();
}

DeliveryReport ProducerManager::send()
{
    return impl()->send();
}

quantum::GenericFuture<DeliveryReport> ProducerManager::post()
{
    return impl()->post();
}

}
}

