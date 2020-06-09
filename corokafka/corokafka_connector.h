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

#include <corokafka/corokafka_configuration_builder.h>
#include <corokafka/interface/corokafka_iconnector.h>
#include <corokafka/interface/corokafka_impl.h>
#include <quantum/quantum_dispatcher.h>

namespace Bloomberg {
namespace corokafka {

class ConnectorImpl;

/**
 * @brief The Connector is the main entry point to the corokafka library. It allows the application to
 *        setup the required topics for publishing/consumption and to get the ProducerManger and/or
 *        ConsumerManager objects for interacting with these topics.
 */
class Connector : public Impl<IConnector>
{
public:
    /**
     * @brief Creates an instance from a ConfigurationBuilder object using its own internal quantum dispatcher.
     * @param builder The builder object containing producer and consumer configurations, one for each topic.
     */
    explicit Connector(const ConfigurationBuilder& builder);
    explicit Connector(ConfigurationBuilder&& builder);
    
    /**
     * @brief For mocking only via dependency injection
     */
    using ImplType = Impl<IConnector>;
    using ImplType::ImplType;
    
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
    Connector(Connector&& other) noexcept;
    Connector& operator=(const Connector& other) = delete;
    Connector& operator=(Connector&& other);
    
    /**
     * @brief Destructor.
     * @remark This calls shutdown().
     */
    ~Connector() = default;
    
    /**
     * @brief Get the ConsumerManager for interacting with Kafka consumers.
     * @return A modifiable ConsumerManager reference.
     */
    ConsumerManager& consumer() final;
    
    /**
     * @brief Get the ProducerManager for interacting with Kafka producers.
     * @return A modifiable ProducerManager reference.
     */
    ProducerManager& producer() final;
    
    /**
     * @brief Gracefully shut down the connector.
     * @param drainTimeout Specify a timeout to apply if draining (see below). Set to -1 to wait indefinitely.
     * @details This will purge all internal producer queues and unsubscribe all consumers. Any pending
     *          poll tasks will run to completion including raising appropriate callbacks.
     * @remark Note that shutdown is automatically called in the Connector destructor.
     * @remark If this connector owns the internal dispatcher (i.e. was not constructed using an externally-supplied
     *         dispatcher) it will also drain all running tasks.
     */
    void shutdown() final;
    void shutdown(std::chrono::milliseconds drainTimeout) final;
};

}
}
#endif //BLOOMBERG_COROKAFKA_CONNECTOR_H
