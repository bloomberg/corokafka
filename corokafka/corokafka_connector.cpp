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
#include <corokafka/impl/corokafka_consumer_manager_impl.h>
#include <corokafka/impl/corokafka_connector_impl.h>
#include <corokafka/corokafka_connector.h>

namespace Bloomberg {
namespace corokafka {

Connector::Connector(const ConfigurationBuilder& builder) :
    _dispatcherPtr(std::make_shared<quantum::Dispatcher>(builder.connectorConfiguration().getDispatcherConfiguration())),
    _impl(std::make_unique<ConnectorImpl>(builder, *_dispatcherPtr))
{

}

Connector::Connector(ConfigurationBuilder&& builder) :
    _dispatcherPtr(std::make_shared<quantum::Dispatcher>(builder.connectorConfiguration().getDispatcherConfiguration())),
    _impl(std::make_unique<ConnectorImpl>(std::move(builder), *_dispatcherPtr))
{

}

Connector::Connector(const ConfigurationBuilder& builder,
                     quantum::Dispatcher& dispatcher) :
    _impl(std::make_unique<ConnectorImpl>(builder, dispatcher))
{

}

Connector::Connector(ConfigurationBuilder&& builder,
                     quantum::Dispatcher& dispatcher) :
    _impl(std::make_unique<ConnectorImpl>(std::move(builder), dispatcher))
{

}

//Circumvent ConnectorImpl not being defined in the header.
Connector::Connector(Connector&& other) noexcept = default;
Connector& Connector::operator=(Connector&& other) = default;

Connector::~Connector()
{
    shutdown();
}

ConsumerManager& Connector::consumer()
{
    return *_impl;
}

ProducerManager& Connector::producer()
{
    return *_impl;
}

void Connector::shutdown(std::chrono::milliseconds drainTimeout)
{
    //drain only if we own the dispatcher
    _impl->shutdown(_dispatcherPtr != nullptr, drainTimeout);
}

}
}
