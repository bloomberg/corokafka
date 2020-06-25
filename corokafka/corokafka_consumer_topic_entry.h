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
#include <corokafka/interface/corokafka_imessage.h>
#include <corokafka/corokafka_consumer_configuration.h>
#include <corokafka/corokafka_throttle_control.h>
#include <corokafka/corokafka_connector_configuration.h>
#include <cppkafka/utils/roundrobin_poll_strategy.h>
#include <quantum/quantum.h>
#include <boost/variant.hpp>

namespace Bloomberg {
namespace corokafka {

using ConsumerType = cppkafka::Consumer;
using ConsumerPtr = std::unique_ptr<ConsumerType>;
using CommitterPtr = std::unique_ptr<cppkafka::BackoffCommitter>;
using PollStrategyBasePtr = std::unique_ptr<cppkafka::PollStrategyBase>;
using DeserializedMessage = std::tuple<boost::any, boost::any, HeaderPack, DeserializerError>;
using MessageBatch = std::vector<cppkafka::Message>;
using Batches = std::vector<MessageBatch>;

enum class Field : int {
    Key = 0,
    Payload = 1,
    Headers = 2,
    Error = 3
};

struct Subscription
{
    void reset() {
        _partitionAssignment.clear();
        _setOffsetsOnStart = true;
        _isSubscribed = false;
    }
    cppkafka::TopicPartitionList    _partitionAssignment;
    bool                            _setOffsetsOnStart{true};
    bool                            _isSubscribed{false};
    mutable quantum::Mutex          _mutex;
};

struct ConsumerTopicEntry {
    ConsumerTopicEntry(ConsumerPtr consumer,
                       const ConnectorConfiguration& connectorConfiguration,
                       ConsumerConfiguration configuration,
                       std::atomic_bool& interrupt,
                       int numIoThreads,
                       std::pair<int,int> coroQueueIdRangeForAny) :
        _connectorConfiguration(connectorConfiguration),
        _configuration(std::move(configuration)),
        _consumer(std::move(consumer)),
        _interrupt(interrupt),
        _coroQueueIdRangeForAny(std::move(coroQueueIdRangeForAny)),
        _numIoThreads(numIoThreads),
        _receiveCallbackThreadRange(0, numIoThreads-1),
        _ioTracker(std::make_shared<int>(0))
    {
        _subscription._partitionAssignment = _configuration.getInitialPartitionAssignment();
    }
    ConsumerTopicEntry(const ConsumerTopicEntry&) = delete;
    ConsumerTopicEntry(ConsumerTopicEntry&& other) noexcept :
        _connectorConfiguration(other._connectorConfiguration),
        _configuration(std::move(other._configuration)),
        _consumer(std::move(other._consumer)),
        _interrupt(other._interrupt),
        _coroQueueIdRangeForAny(std::move(other._coroQueueIdRangeForAny)),
        _numIoThreads(other._numIoThreads),
        _receiveCallbackThreadRange(std::move(other._receiveCallbackThreadRange)),
        _ioTracker(std::move(other._ioTracker))
    {
        _subscription._partitionAssignment = std::move(other._subscription._partitionAssignment);
    }
    
    //Members
    const ConnectorConfiguration&   _connectorConfiguration;
    ConsumerConfiguration           _configuration;
    ConsumerPtr                     _consumer;
    std::atomic_bool&               _interrupt;
    cppkafka::Queue                 _eventQueue; //queue event polling
    CommitterPtr                    _committer;
    PollStrategyBasePtr             _roundRobinPoller;
    PollStrategy                    _pollStrategy{PollStrategy::Serial};
    Subscription                    _subscription;
    OffsetMap                       _offsets;
    OffsetWatermarkList             _watermarks;
    bool                            _enableWatermarkCheck{false};
    std::atomic_bool                _isPaused{false};
    bool                            _skipUnknownHeaders{true};
    quantum::ThreadContextPtr<int>  _pollFuture{nullptr};
    ssize_t                         _readSize{100};
    quantum::IQueue::QueueId        _processCoroThreadId{quantum::IQueue::QueueId::Any};
    quantum::IQueue::QueueId        _pollIoThreadId{quantum::IQueue::QueueId::Any};
    std::chrono::milliseconds       _pollTimeout{EnumValue(TimerValues::Disabled)};
    std::chrono::milliseconds       _minPollInterval{EnumValue(TimerValues::Disabled)};
    std::pair<int,int>              _coroQueueIdRangeForAny;
    int                             _numIoThreads;
    std::pair<int,int>              _receiveCallbackThreadRange;
    ExecMode                        _receiveCallbackExec{ExecMode::Async};
    ThreadType                      _receiverThread{ThreadType::IO};
    bool                            _autoOffsetPersist{true};
    bool                            _autoOffsetPersistOnException{false};
    OffsetPersistStrategy           _autoOffsetPersistStrategy{OffsetPersistStrategy::Store};
    ExecMode                        _autoCommitExec{ExecMode::Async};
    cppkafka::LogLevel              _logLevel{cppkafka::LogLevel::LogInfo};
    bool                            _batchPrefetch{false};
    quantum::ICoroFuture<MessageBatch>::Ptr   _batchPrefetchFuture;
    Callbacks::PreprocessorCallback _preprocessorCallback;
    bool                            _preprocess{false};
    ThrottleControl                 _throttleControl;
    bool                            _preserveMessageOrder{false};
    IoTracker                       _ioTracker;
};

}}

#endif //BLOOMBERG_COROKAFKA_CONSUMER_TOPIC_ENTRY_H
