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
#include <corokafka/corokafka_utils.h>
#include <corokafka/corokafka_message.h>
#include <corokafka/corokafka_consumer_configuration.h>
#include <corokafka/corokafka_connector_configuration.h>
#include <quantum/quantum.h>

using namespace BloombergLP;

namespace Bloomberg {
namespace corokafka {

using ConsumerType = Consumer;
using ConsumerPtr = std::unique_ptr<ConsumerType>;
using CommitterPtr = std::unique_ptr<BackoffCommitter>;
using PollingStrategyPtr = std::unique_ptr<RoundRobinPollStrategy>;

struct ConsumerTopicEntry : TopicEntry {
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
    //Members
    const ConnectorConfiguration&   _connectorConfiguration;
    ConsumerConfiguration           _configuration;
    ConsumerPtr                     _consumer;
    CommitterPtr                    _committer;
    PollingStrategyPtr              _roundRobin;
    mutable OffsetMap               _offsets;
    bool                            _isPaused{false};
    bool                            _setOffsetsOnStart{true};
    bool                            _pauseOnStart{false};
    bool                            _isSubscribed{true};
    bool                            _skipUnknownHeaders{true};
    quantum::ThreadContext<int>::Ptr _pollFuture{nullptr};
    size_t                          _batchSize{100};
    std::chrono::milliseconds       _pollTimeout{0};
    std::chrono::steady_clock::time_point _throttleTime;
    std::chrono::milliseconds       _throttleDuration{0};
    bool                            _autoThrottle{false};
    uint16_t                        _throttleMultiplier{1};
    std::pair<int,int>              _coroQueueIdRangeForAny;
    std::pair<int,int>              _receiveCallbackThreadRange;
    ExecMode                        _receiveCallbackExec{ExecMode::Async};
    bool                            _receiveOnIoThread{true};
    bool                            _autoOffsetPersist{true};
    OffsetPersistStrategy           _autoOffsetPersistStrategy{OffsetPersistStrategy::Store};
    ExecMode                        _autoCommitExec{ExecMode::Async};
    bool                            _batchPrefetch{false};
    LogLevel                        _logLevel{LogLevel::LogInfo};
    quantum::ICoroFuture<std::vector<Message>>::Ptr _messagePrefetchFuture;
    Callbacks::PreprocessorCallback _preprocessorCallback;
    bool                            _preprocess{true};
    bool                            _preprocessOnIoThread{true};
};

}}

#endif //BLOOMBERG_COROKAFKA_CONSUMER_TOPIC_ENTRY_H
