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
#ifndef BLOOMBERG_COROKAFKA_PRODUCER_TOPIC_ENTRY_H
#define BLOOMBERG_COROKAFKA_PRODUCER_TOPIC_ENTRY_H

#include <memory>
#include <chrono>
#include <corokafka/corokafka_utils.h>
#include <corokafka/corokafka_message.h>
#include <corokafka/corokafka_producer_configuration.h>
#include <corokafka/corokafka_connector_configuration.h>
#include <corokafka/corokafka_throttle_control.h>
#include <corokafka/detail/corokafka_macros.h>
#include <quantum/quantum.h>

namespace Bloomberg {
namespace corokafka {

using ProducerType = cppkafka::BufferedProducer<ByteArray>;
using ProducerPtr = std::unique_ptr<ProducerType>;

enum class QueueFullNotification {
    None,
    OncePerMessage,
    EachOccurence,
    EdgeTriggered
};

struct ProducerTopicEntry {
    ProducerTopicEntry(ProducerPtr producer,
                       const ConnectorConfiguration& connectorConfiguration,
                       const ProducerConfiguration& configuration) :
        _connectorConfiguration(connectorConfiguration),
        _configuration(configuration),
        _producer(std::move(producer))
    {}
    ProducerTopicEntry(ProducerPtr producer,
                       const ConnectorConfiguration& connectorConfiguration,
                       ProducerConfiguration&& configuration) :
        _connectorConfiguration(connectorConfiguration),
        _configuration(std::move(configuration)),
        _producer(std::move(producer))
    {}
    ProducerTopicEntry(const ProducerTopicEntry&) = delete;
    ProducerTopicEntry(ProducerTopicEntry&& other) :
        _connectorConfiguration(other._connectorConfiguration),
        _configuration(std::move(other._configuration)),
        _producer(std::move(other._producer))
    {}
    
    const ConnectorConfiguration&       _connectorConfiguration;
    ProducerConfiguration               _configuration;
    ProducerPtr                         _producer;
    size_t                              _topicHash{0};
    quantum::ThreadFuturePtr<int>       _pollFuture{nullptr};
    std::chrono::milliseconds           _waitForAcksTimeout{(int)TimerValues::Disabled};
    std::chrono::milliseconds           _flushWaitForAcksTimeout{rd_kafka_version() >= RD_KAFKA_ZERO_TIMEOUT_FLUSH_FIX ? (int)TimerValues::Disabled : 100};
    bool                                _forceSyncFlush{false};
    bool                                _preserveMessageOrder{false};
    cppkafka::Producer::PayloadPolicy   _payloadPolicy{cppkafka::Producer::PayloadPolicy::COPY_PAYLOAD};
    size_t                              _maxQueueLength{10000};
    cppkafka::LogLevel                  _logLevel{cppkafka::LogLevel::LogInfo};
    QueueFullNotification               _queueFullNotification{QueueFullNotification::OncePerMessage};
    bool                                _queueFullTrigger{true};
    ThrottleControl                     _throttleControl;
};

}
}

#endif //BLOOMBERG_COROKAFKA_PRODUCER_TOPIC_ENTRY_H
