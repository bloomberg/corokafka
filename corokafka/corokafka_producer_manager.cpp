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

namespace Bloomberg {
namespace corokafka {

ProducerManager::ProducerManager(quantum::Dispatcher& dispatcher,
                                 const ConnectorConfiguration& connectorConfiguration,
                                 const ConfigMap& config) :
    _impl(new ProducerManagerImpl(dispatcher, connectorConfiguration, config))
{

}

ProducerManager::ProducerManager(quantum::Dispatcher& dispatcher,
                                 const ConnectorConfiguration& connectorConfiguration,
                                 ConfigMap&& config) :
    _impl(new ProducerManagerImpl(dispatcher, connectorConfiguration, std::move(config)))
{

}

ProducerManager::~ProducerManager()
{

}

ProducerMetadata ProducerManager::getMetadata(const std::string& topic)
{
    return _impl->getMetadata(topic);
}

const ProducerConfiguration& ProducerManager::getConfiguration(const std::string& topic) const
{
    return _impl->getConfiguration(topic);
}

std::vector<std::string> ProducerManager::getTopics() const
{
    return _impl->getTopics();
}

void ProducerManager::resetQueueFullTrigger(const std::string& topic)
{
    return _impl->resetQueueFullTrigger(topic);
}

bool ProducerManager::waitForAcks(const std::string& topic)
{
    return _impl->waitForAcks(topic);
}

bool ProducerManager::waitForAcks(const std::string& topic,
                                  std::chrono::milliseconds timeout)
{
    return _impl->waitForAcks(topic, timeout);
}

void ProducerManager::shutdown()
{
    _impl->shutdown();
}

void ProducerManager::poll()
{
    _impl->poll();
}

void ProducerManager::pollEnd()
{
    _impl->pollEnd();
}

}
}

