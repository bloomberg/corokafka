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
#include <corokafka/corokafka_utils.h>
#include <corokafka/corokafka_configuration.h>
#include <corokafka/corokafka_producer_metadata.h>
#include <corokafka/corokafka_consumer_metadata.h>
#include <corokafka/corokafka_producer_topic_entry.h>
#include <corokafka/corokafka_consumer_topic_entry.h>
#include <corokafka/corokafka_exception.h>

namespace Bloomberg {
namespace corokafka {

ssize_t& maxMessageBuilderOutputLength()
{
    static ssize_t messageLen{100};
    return messageLen;
}

void handleException(const std::exception& ex,
                     const Metadata& metadata,
                     const TopicConfiguration& config,
                     cppkafka::LogLevel level)
{
    cppkafka::CallbackInvoker<Callbacks::ErrorCallback> errorCallback("error", config.getErrorCallback(), nullptr);
    if (errorCallback) {
        if (const cppkafka::HandleException* except = dynamic_cast<const cppkafka::HandleException*>(&ex)) {
            errorCallback(metadata, except->get_error(), except->what(), nullptr);
        }
        else if (metadata.getType() == KafkaType::Consumer) {
            if (const cppkafka::ConsumerException *except = dynamic_cast<const cppkafka::ConsumerException *>(&ex)) {
                errorCallback(metadata, except->get_error(), except->what(), nullptr);
            }
            else if (const cppkafka::QueueException *except = dynamic_cast<const cppkafka::QueueException *>(&ex)) {
                errorCallback(metadata, except->get_error(), except->what(), nullptr);
            }
            else {
                errorCallback(metadata, RD_KAFKA_RESP_ERR_UNKNOWN, ex.what(), nullptr);
            }
        }
        else { //KafkaType == Producer
            errorCallback(metadata, RD_KAFKA_RESP_ERR_UNKNOWN, ex.what(), nullptr);
        }
    }
    if (level >= cppkafka::LogLevel::LogErr) {
        cppkafka::CallbackInvoker<Callbacks::LogCallback> logCallback("log", config.getLogCallback(), nullptr);
        if (logCallback) {
            logCallback(metadata, cppkafka::LogLevel::LogErr, "corokafka", ex.what());
        }
    }
}

cppkafka::TopicPartition operator+(const cppkafka::TopicPartition& offset, int value)
{
    if (offset.get_offset() < 0) {
        throw std::invalid_argument("Offset must be >= 0");
    }
    if ((offset.get_offset() + value) < 0) {
        throw std::invalid_argument("Resulting offset must be >= 0");
    }
    return {offset.get_topic(), offset.get_partition(), offset.get_offset() + value};
}

cppkafka::TopicPartition operator-(const cppkafka::TopicPartition& offset, int value)
{
    return offset + (-value);
}

}
}

