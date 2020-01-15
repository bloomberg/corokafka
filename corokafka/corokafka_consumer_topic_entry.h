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
#ifndef BLOOMBERG_COROKAFKA_CONSUMER_TOPIC_ENTRY_H
#define BLOOMBERG_COROKAFKA_CONSUMER_TOPIC_ENTRY_H

#include <memory>
#include <chrono>
#include <atomic>
#include <corokafka/corokafka_utils.h>
#include <corokafka/corokafka_message.h>
#include <corokafka/corokafka_consumer_configuration.h>
#include <corokafka/corokafka_throttle_control.h>
#include <corokafka/corokafka_connector_configuration.h>
#include <quantum/quantum.h>

namespace Bloomberg {
namespace corokafka {

using ConsumerType = cppkafka::Consumer;
using ConsumerPtr = std::unique_ptr<ConsumerType>;
using CommitterPtr = std::unique_ptr<cppkafka::BackoffCommitter>;
using PollingStrategyPtr = std::unique_ptr<cppkafka::RoundRobinPollStrategy>;

struct ConsumerTopicEntry {
    ConsumerTopicEntry(ConsumerPtr consumer,
               const ConnectorConfiguration& connectorConfiguration,
               const ConsumerConfiguration& configuration,
               int numIoThreads,
               std::pair<int,int> coroQueueIdRangeForAny) :
        _connectorConfiguration(connectorConfiguration),
        _configuration(configuration),
        _consumer(std::move(consumer)),
        _coroQueueIdRangeForAny(coroQueueIdRangeForAny),
        _receiveCallbackThreadRange(0, numIoThreads-1)
    {}
    ConsumerTopicEntry(ConsumerPtr consumer,
               const ConnectorConfiguration& connectorConfiguration,
               ConsumerConfiguration&& configuration,
               int numIoThreads,
               std::pair<int,int> coroQueueIdRangeForAny) :
        _connectorConfiguration(connectorConfiguration),
        _configuration(std::move(configuration)),
        _consumer(std::move(consumer)),
        _coroQueueIdRangeForAny(coroQueueIdRangeForAny),
        _receiveCallbackThreadRange(0, numIoThreads-1)
    {}
    ConsumerTopicEntry(const ConsumerTopicEntry&) = delete;
    ConsumerTopicEntry(ConsumerTopicEntry&& other) :
        _connectorConfiguration(other._connectorConfiguration),
        _configuration(std::move(other._configuration)),
        _consumer(std::move(other._consumer)),
        _coroQueueIdRangeForAny(other._coroQueueIdRangeForAny),
        _receiveCallbackThreadRange(other._receiveCallbackThreadRange)
    {}
    
    //Members
    const ConnectorConfiguration&   _connectorConfiguration;
    ConsumerConfiguration           _configuration;
    ConsumerPtr                     _consumer;
    CommitterPtr                    _committer;
    PollingStrategyPtr              _roundRobin;
    mutable OffsetMap               _offsets;
    std::atomic<bool>               _isPaused{false};
    bool                            _setOffsetsOnStart{true};
    bool                            _isSubscribed{true};
    bool                            _skipUnknownHeaders{true};
    quantum::ThreadContext<int>::Ptr _pollFuture{nullptr};
    size_t                          _batchSize{100};
    std::chrono::milliseconds       _pollTimeout{(int)TimerValues::Disabled};
    std::pair<int,int>              _coroQueueIdRangeForAny;
    std::pair<int,int>              _receiveCallbackThreadRange;
    ExecMode                        _receiveCallbackExec{ExecMode::Async};
    ThreadType                      _receiverThread{ThreadType::IO};
    bool                            _autoOffsetPersist{true};
    bool                            _autoOffsetPersistOnException{false};
    OffsetPersistStrategy           _autoOffsetPersistStrategy{OffsetPersistStrategy::Store};
    ExecMode                        _autoCommitExec{ExecMode::Async};
    bool                            _batchPrefetch{false};
    cppkafka::LogLevel              _logLevel{cppkafka::LogLevel::LogInfo};
    quantum::ICoroFuture<std::vector<cppkafka::Message>>::Ptr _messagePrefetchFuture;
    Callbacks::PreprocessorCallback _preprocessorCallback;
    bool                            _preprocess{true};
    ThreadType                      _preprocessorThread{ThreadType::IO};
    ThrottleControl                 _throttleControl;
};

}}

#endif //BLOOMBERG_COROKAFKA_CONSUMER_TOPIC_ENTRY_H
