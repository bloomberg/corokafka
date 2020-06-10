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
#ifndef BLOOMBERG_COROKAFKA_CONNECTOR_IMPL_H
#define BLOOMBERG_COROKAFKA_CONNECTOR_IMPL_H

#include <corokafka/corokafka_utils.h>
#include <corokafka/corokafka_configuration_builder.h>
#include <corokafka/corokafka_metadata.h>
#include <corokafka/interface/corokafka_iconnector.h>
#include <quantum/quantum.h>
#include <vector>
#include <map>
#include <chrono>

namespace Bloomberg {
namespace corokafka {

class ConnectorImpl : public IConnector,
                      public Interruptible
{
public:
    explicit ConnectorImpl(const ConfigurationBuilder& builder);
    explicit ConnectorImpl(ConfigurationBuilder&& builder);
    ConnectorImpl(const ConfigurationBuilder& builder,
                  quantum::Dispatcher& dispatcher);
    ConnectorImpl(ConfigurationBuilder&& builder,
                  quantum::Dispatcher& dispatcher);
    ~ConnectorImpl() noexcept;
    ConsumerManager& consumer() final;
    ProducerManager& producer() final;
    void shutdown() final;
    void shutdown(std::chrono::milliseconds drainTimeout) final;
    void poll();
    
private:
    // members
    const ConnectorConfiguration            _connectorConfiguration;
    std::unique_ptr<quantum::Dispatcher>    _dispatcherPtr;
    quantum::Dispatcher&                    _dispatcher;
    std::unique_ptr<ProducerManager>        _producerPtr;
    std::unique_ptr<ConsumerManager>        _consumerPtr;
    std::thread                             _pollThread;
    std::atomic_flag                        _shutdownInitiated{false};
};

}}

#endif //BLOOMBERG_COROKAFKA_CONNECTOR_IMPL_H
