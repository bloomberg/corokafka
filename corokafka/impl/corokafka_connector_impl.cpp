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
#include <corokafka/corokafka_utils.h>
#include <functional>

namespace Bloomberg {
namespace corokafka {

ConnectorImpl::ConnectorImpl(const ConfigurationBuilder& builder,
                             quantum::Dispatcher& dispatcher) :
    ProducerManager(dispatcher, _config, builder.producerConfigurations()),
    ConsumerManager(dispatcher, _config, builder.consumerConfigurations()),
    _config(builder.connectorConfiguration()),
    _dispatcher(dispatcher),
    _pollThread(std::bind(&ConnectorImpl::poll, this))
{
    maxMessageBuilderOutputLength() = _config.getMaxMessagePayloadOutputLength();
}

ConnectorImpl::ConnectorImpl(ConfigurationBuilder&& builder,
                             quantum::Dispatcher& dispatcher) :
    ProducerManager(dispatcher, _config, std::move(builder.producerConfigurations())),
    ConsumerManager(dispatcher, _config, std::move(builder.consumerConfigurations())),
    _config(std::move(builder.connectorConfiguration())),
    _dispatcher(dispatcher),
    _pollThread(std::bind(&ConnectorImpl::poll, this))
{
    maxMessageBuilderOutputLength() = _config.getMaxMessagePayloadOutputLength();
}

void ConnectorImpl::shutdown(bool drain,
                             std::chrono::milliseconds drainTimeout)
{
    if (!_shutdownInitiated.test_and_set())
    {
        _shuttingDown = true;
        ProducerManager::shutdown();
        ConsumerManager::shutdown();
        
        // Wait on the poll thread
        try {
            _pollThread.join();
        }
        catch (const std::system_error& ex) {
            if (_config.getLogCallback()) {
                std::ostringstream oss;
                oss << "Joining on poll thread failed with error: " << ex.what();
                _config.getLogCallback()(cppkafka::LogLevel::LogErr, "corokafka", oss.str());
            }
        }
        
        if (drain) {
            if (drainTimeout.count() < 0) {
                drainTimeout = std::chrono::milliseconds::zero();
            }
            _dispatcher.drain(drainTimeout, true);
        }
    }
}

void ConnectorImpl::poll()
{
    while (!_shuttingDown) {
        auto start = std::chrono::high_resolution_clock::now();
        try {
            ProducerManager::poll(); //flush the producers
            ConsumerManager::poll(); //poll the consumers
        }
        catch (const std::exception& ex) {
            if (_config.getLogCallback()) {
                std::ostringstream oss;
                oss << "Caught exception while polling: " << ex.what();
                _config.getLogCallback()(cppkafka::LogLevel::LogErr, "corokafka", oss.str());
            }
        }
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end-start);
        if (duration < _config.getPollInterval()) {
            std::this_thread::sleep_for(_config.getPollInterval()-duration);
        }
    }
    ProducerManager::pollEnd();
    ConsumerManager::pollEnd();
}

}}
