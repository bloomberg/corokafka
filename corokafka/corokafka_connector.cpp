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
    ImplType(std::make_shared<ConnectorImpl>(builder))
{
}

Connector::Connector(ConfigurationBuilder&& builder) :
    ImplType(std::make_shared<ConnectorImpl>(std::move(builder)))
{
}

Connector::Connector(const ConfigurationBuilder& builder,
                     quantum::Dispatcher& dispatcher) :
    ImplType(std::make_shared<ConnectorImpl>(builder, dispatcher))
{
}

Connector::Connector(ConfigurationBuilder&& builder,
                     quantum::Dispatcher& dispatcher) :
    ImplType(std::make_shared<ConnectorImpl>(std::move(builder), dispatcher))
{
}

//Circumvent ConnectorImpl not being defined in the header.
Connector::Connector(Connector&& other) noexcept = default;
Connector& Connector::operator=(Connector&& other) = default;

ConsumerManager& Connector::consumer()
{
    return impl()->consumer();
}

ProducerManager& Connector::producer()
{
    return impl()->producer();
}

void Connector::shutdown()
{
    //drain only if we own the dispatcher
    impl()->shutdown();
}

void Connector::shutdown(std::chrono::milliseconds drainTimeout)
{
    //drain only if we own the dispatcher
    impl()->shutdown(drainTimeout);
}

}
}
