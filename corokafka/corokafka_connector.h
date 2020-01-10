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
#ifndef BLOOMBERG_COROKAFKA_CONNECTOR_H
#define BLOOMBERG_COROKAFKA_CONNECTOR_H

#include <vector>
#include <map>
#include <chrono>
#include <corokafka/corokafka_utils.h>
#include <corokafka/corokafka_configuration_builder.h>
#include <corokafka/corokafka_metadata.h>
#include <corokafka/corokafka_consumer_manager.h>
#include <corokafka/corokafka_producer_manager.h>

namespace Bloomberg {
namespace corokafka {

class ConnectorImpl;

/**
 * @brief The Connector is the main entry point to the corokafka library. It allows the application to
 *        setup the required topics for publishing/consumption and to get the ProducerManger and/or
 *        ConsumerManager objects for interacting with these topics.
 */
class Connector
{
public:
    /**
     * @brief Creates an instance from a ConfigurationBuilder object using its own internal quantum dispatcher.
     * @param builder The builder object containing producer and consumer configurations, one for each topic.
     */
    Connector(const ConfigurationBuilder& builder);
    Connector(ConfigurationBuilder&& builder);
    
    /**
     * @brief Creates an instance from a ConfigurationBuilder object and using the supplied quantum dispatcher.
     * @param builder The builder object containing producer and consumer configurations, one for each topic.
     * @param dispatcher The coroutine dispatcher to use.
     */
    Connector(const ConfigurationBuilder& builder,
              quantum::Dispatcher& dispatcher);
    Connector(ConfigurationBuilder&& builder,
              quantum::Dispatcher& dispatcher);
    
    Connector(const Connector& other) = delete;
    Connector(Connector&& other) = default;
    Connector& operator=(const Connector& other) = delete;
    Connector& operator=(Connector&& other) = default;
    
    /**
     * @brief Destructor.
     * @remark This calls shutdown().
     */
    ~Connector();
    
    /**
     * @brief Get the ConsumerManager for interacting with Kafka consumers.
     * @return A modifiable ConsumerManager reference.
     */
    ConsumerManager& consumer();
    
    /**
     * @brief Get the ProducerManager for interacting with Kafka producers.
     * @return A modifiable ProducerManager reference.
     */
    ProducerManager& producer();
    
    /**
     * @brief Gracefully shut down the connector.
     * @param drainTimeout Specify a timeout to apply if draining (see below). Set to 0 to wait for all tasks.
     * @details This will purge all internal producer queues and unsubscribe all consumers. Any pending
     *          poll tasks will run to completion including raising appropriate callbacks.
     * @remark Note that shutdown is automatically called in the Connector destructor.
     * @remark If this connector owns the internal dispatcher (i.e. was not constructed using an externally-supplied
     *         dispatcher) it will also drain all running tasks.
     */
    void shutdown(std::chrono::milliseconds drainTimeout = std::chrono::milliseconds::zero());
    
private:
    //members
    std::shared_ptr<quantum::Dispatcher>        _dispatcherPtr;
    std::shared_ptr<ConnectorImpl>              _impl;
};

}
}
#endif //BLOOMBERG_COROKAFKA_CONNECTOR_H
