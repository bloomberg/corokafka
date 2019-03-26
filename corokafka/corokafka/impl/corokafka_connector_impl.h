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

#include <vector>
#include <map>
#include <chrono>
#include <corokafka/corokafka_utils.h>
#include <corokafka/corokafka_configuration_builder.h>
#include <corokafka/corokafka_metadata.h>
#include <corokafka/corokafka_producer_manager.h>
#include <corokafka/corokafka_consumer_manager.h>
#include <quantum/quantum.h>

using namespace BloombergLP;

namespace Bloomberg {
namespace corokafka {

class ConnectorImpl : public ProducerManager,
                      public ConsumerManager
{
    friend class Connector;
    ConnectorImpl(const ConfigurationBuilder& builder,
                  quantum::Dispatcher& dispatcher);
    ConnectorImpl(ConfigurationBuilder&& builder,
                  quantum::Dispatcher& dispatcher);
    void shutdown();
    void poll();
    void post();
    
    // members
    const ConnectorConfiguration    _config;
    quantum::Dispatcher&            _dispatcher;
    bool                            _interrupt{false};
    std::thread                     _pollThread;
    std::thread                     _postThread;
    std::atomic_flag                _shutdownInitiated ATOMIC_FLAG_INIT;

};

}}

#endif //BLOOMBERG_COROKAFKA_CONNECTOR_IMPL_H
