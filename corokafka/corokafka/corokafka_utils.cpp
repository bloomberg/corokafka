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

namespace Bloomberg {
namespace corokafka {

ssize_t maxMessageBuilderOutputLength{100};

ssize_t getMaxMessageBuilderOutputLength()
{
    return maxMessageBuilderOutputLength;
}

void setMaxMessageBuilderOutputLength(ssize_t length)
{
    maxMessageBuilderOutputLength = length;
}

LogLevel logLevelFromString(const std::string& level)
{
    StringEqualCompare compare;
    if (compare(level, "emergency")) {
        return LogLevel::LogEmerg;
    }
    if (compare(level, "alert")) {
        return LogLevel::LogAlert;
    }
    if (compare(level, "critical")) {
        return LogLevel::LogCrit;
    }
    if (compare(level, "error")) {
        return LogLevel::LogErr;
    }
    if (compare(level, "warning")) {
        return LogLevel::LogWarning;
    }
    if (compare(level, "notice")) {
        return LogLevel::LogNotice;
    }
    if (compare(level, "info")) {
        return LogLevel::LogInfo;
    }
    if (compare(level, "debug")) {
        return LogLevel::LogDebug;
    }
    throw std::invalid_argument("Unknown log level");
}

std::ostream& operator<<(std::ostream& stream, const MessageBuilder& builder) {
    ssize_t max_len = getMaxMessageBuilderOutputLength();
    size_t payload_len = (max_len == -1) ? builder.payload().get_size() :
                         std::min(builder.payload().get_size(), (size_t)max_len);
    stream << "[topic:" << builder.topic() << "]"
           << "[partition:" << builder.partition() << "]"
           << "[key:" << (std::string)builder.key() << "]"
           << "[length:" << builder.payload().get_size() << "]"
           << "[payload:" << std::string((const char*)builder.payload().get_data(), payload_len) << "]";
    if (builder.timestamp().count() > 0) {
        stream << "[timestamp:" << builder.timestamp().count() << "]";
    }
    return stream;
}

void handleException(const std::exception& ex,
                     const Metadata& metadata,
                     const Configuration& config,
                     LogLevel level)
{
    CallbackInvoker<Callbacks::ErrorCallback> error_cb("error", config.getErrorCallback(), nullptr);
    const HandleException* hex = dynamic_cast<const HandleException*>(&ex);
    if (error_cb) {
        if (hex) {
            error_cb(metadata, hex->get_error(), hex->what(), nullptr);
        }
        else {
            error_cb(metadata, RD_KAFKA_RESP_ERR_UNKNOWN, ex.what(), nullptr);
        }
    }
    if (level >= LogLevel::LogErr) {
        CallbackInvoker<Callbacks::LogCallback> logger_cb("log", config.getLogCallback(), nullptr);
        if (logger_cb) {
            logger_cb(metadata, LogLevel::LogErr, "corokafka", ex.what());
        }
    }
}

bool reduceThrottling(const std::chrono::steady_clock::time_point& currentTime,
                      std::chrono::steady_clock::time_point& throttleTime,
                      std::chrono::milliseconds& throttleDuration)
{
    if (throttleDuration == std::chrono::milliseconds(0)) {
        return true;
    }
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(currentTime-throttleTime);
    if (elapsed > throttleDuration) {
        throttleDuration = std::chrono::milliseconds(0);
        return true;
    }
    throttleDuration -= elapsed;
    return false;
}

}
}

