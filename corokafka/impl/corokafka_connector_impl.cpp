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

ConnectorImpl::ConnectorImpl(const ConfigurationBuilder& builder) :
    _connectorConfiguration(builder.connectorConfiguration()),
    _dispatcherPtr(std::make_unique<quantum::Dispatcher>(_connectorConfiguration.getDispatcherConfiguration())),
    _dispatcher(*_dispatcherPtr),
    _producerPtr(new ProducerManager(_dispatcher,
                _connectorConfiguration,
                builder.producerConfigurations(),
                _interrupt)),
    _consumerPtr(new ConsumerManager(_dispatcher,
                 _connectorConfiguration,
                 builder.consumerConfigurations(),
                 _interrupt)),
    _pollThread(std::bind(&ConnectorImpl::poll, this))
{
    maxMessageBuilderOutputLength() = _connectorConfiguration.getMaxMessagePayloadOutputLength();
#if (__GLIBC__ >= 2) && (__GLIBC_MINOR__ >= 12)
    setThreadName(_pollThread.native_handle(), "poll:", 0);
#endif
}

ConnectorImpl::ConnectorImpl(const ConfigurationBuilder& builder,
                             quantum::Dispatcher& dispatcher) :
    _connectorConfiguration(builder.connectorConfiguration()),
    _dispatcher(dispatcher),
    _producerPtr(new ProducerManager(_dispatcher,
                _connectorConfiguration,
                builder.producerConfigurations(),
                _interrupt)),
    _consumerPtr(new ConsumerManager(_dispatcher,
                 _connectorConfiguration,
                 builder.consumerConfigurations(),
                 _interrupt)),
    _pollThread(std::bind(&ConnectorImpl::poll, this))
{
    maxMessageBuilderOutputLength() = _connectorConfiguration.getMaxMessagePayloadOutputLength();
#if (__GLIBC__ >= 2) && (__GLIBC_MINOR__ >= 12)
    setThreadName(_pollThread.native_handle(), "poll:", 0);
#endif
}

ConnectorImpl::ConnectorImpl(ConfigurationBuilder&& builder) :
    _connectorConfiguration(std::move(builder.connectorConfiguration())),
    _dispatcherPtr(std::make_unique<quantum::Dispatcher>(_connectorConfiguration.getDispatcherConfiguration())),
    _dispatcher(*_dispatcherPtr),
    _producerPtr(new ProducerManager(_dispatcher,
                _connectorConfiguration,
                std::move(builder.producerConfigurations()),
                _interrupt)),
    _consumerPtr(new ConsumerManager(_dispatcher,
                 _connectorConfiguration,
                 std::move(builder.consumerConfigurations()),
                 _interrupt)),
    _pollThread(std::bind(&ConnectorImpl::poll, this))
{
    maxMessageBuilderOutputLength() = _connectorConfiguration.getMaxMessagePayloadOutputLength();
#if (__GLIBC__ >= 2) && (__GLIBC_MINOR__ >= 12)
    setThreadName(_pollThread.native_handle(), "poll:", 0);
#endif
}

ConnectorImpl::ConnectorImpl(ConfigurationBuilder&& builder,
                             quantum::Dispatcher& dispatcher) :
    _connectorConfiguration(std::move(builder.connectorConfiguration())),
    _dispatcher(dispatcher),
    _producerPtr(new ProducerManager(_dispatcher,
                _connectorConfiguration,
                std::move(builder.producerConfigurations()),
                _interrupt)),
    _consumerPtr(new ConsumerManager(_dispatcher,
                 _connectorConfiguration,
                 std::move(builder.consumerConfigurations()),
                 _interrupt)),
    _pollThread(std::bind(&ConnectorImpl::poll, this))
{
    maxMessageBuilderOutputLength() = _connectorConfiguration.getMaxMessagePayloadOutputLength();
#if (__GLIBC__ >= 2) && (__GLIBC_MINOR__ >= 12)
    setThreadName(_pollThread.native_handle(), "poll:", 0);
#endif
}

ConnectorImpl::~ConnectorImpl() noexcept
{
    shutdown();
}

ConsumerManager& ConnectorImpl::consumer()
{
    return *_consumerPtr;
}

ProducerManager& ConnectorImpl::producer()
{
    return *_producerPtr;
}

void ConnectorImpl::shutdown()
{
    shutdown(std::chrono::milliseconds(EnumValue(TimerValues::Unlimited)));
}

void ConnectorImpl::shutdown(std::chrono::milliseconds drainTimeout)
{
    if (!_shutdownInitiated.test_and_set())
    {
        _interrupt = true;
        _producerPtr->shutdown();
        _consumerPtr->shutdown();
        
        // Wait on the poll thread
        try {
            _pollThread.join();
        }
        catch (const std::system_error& ex) {
            if (_connectorConfiguration.getLogCallback()) {
                std::ostringstream oss;
                oss << "Joining on poll thread failed with error: " << ex.what();
                _connectorConfiguration.getLogCallback()(cppkafka::LogLevel::LogErr, "corokafka", oss.str());
            }
        }
        
        if (_dispatcherPtr) {
            //This is an internal dispatcher, therefore we can drain it
            _dispatcher.drain(drainTimeout, true);
        }
    }
}

void ConnectorImpl::poll()
{
    while (!_interrupt) {
        auto start = std::chrono::high_resolution_clock::now();
        try {
            _producerPtr->poll(); //flush the producers
            _consumerPtr->poll(); //poll the consumers
        }
        catch (const std::exception& ex) {
            if (_connectorConfiguration.getLogCallback()) {
                std::ostringstream oss;
                oss << "Caught exception while polling: " << ex.what();
                _connectorConfiguration.getLogCallback()(cppkafka::LogLevel::LogErr, "corokafka", oss.str());
            }
        }
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end-start);
        if (duration < _connectorConfiguration.getPollInterval()) {
            std::this_thread::sleep_for(_connectorConfiguration.getPollInterval()-duration);
        }
    }
    _producerPtr->pollEnd();
    _consumerPtr->pollEnd();
}

}}
