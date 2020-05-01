/*
** Copyright 2020 Bloomberg Finance L.P.
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
#ifndef BLOOMBERG_COROKAFKA_ICONSUMER_MANAGER_H
#define BLOOMBERG_COROKAFKA_ICONSUMER_MANAGER_H

#include <corokafka/corokafka_utils.h>
#include <corokafka/corokafka_metadata.h>
#include <corokafka/corokafka_consumer_configuration.h>
#include <cppkafka/topic_partition.h>
#include <cppkafka/topic_partition_list.h>
#include <string>
#include <vector>

namespace Bloomberg {
namespace corokafka {

struct IConsumerManager
{
    virtual ~IConsumerManager() = default;
    virtual void pause() = 0;
    virtual void pause(const std::string& topic) = 0;
    virtual void resume() = 0;
    virtual void resume(const std::string& topic) = 0;
    virtual void subscribe(const std::string& topic,
                           const cppkafka::TopicPartitionList& partitionList) = 0;
    virtual void subscribe(const cppkafka::TopicPartitionList& partitionList) = 0;
    virtual void unsubscribe() = 0;
    virtual void unsubscribe(const std::string& topic) = 0;
    virtual cppkafka::Error commit(const cppkafka::TopicPartition& topicPartition,
                                   const void* opaque) = 0;
    virtual cppkafka::Error commit(const cppkafka::TopicPartition& topicPartition,
                                   ExecMode execMode,
                                   const void* opaque) = 0;
    virtual cppkafka::Error commit(const cppkafka::TopicPartitionList& topicPartitions,
                                   ExecMode execMode,
                                   const void* opaque) = 0;
    virtual cppkafka::Error commit(const cppkafka::TopicPartitionList& topicPartitions,
                                   const void* opaque) = 0;
    virtual void shutdown() = 0;
    virtual ConsumerMetadata getMetadata(const std::string& topic) = 0;
    virtual void enablePreprocessing() = 0;
    virtual void enablePreprocessing(const std::string& topic) = 0;
    virtual void disablePreprocessing() = 0;
    virtual void disablePreprocessing(const std::string& topic) = 0;
    virtual const ConsumerConfiguration& getConfiguration(const std::string& topic) const = 0;
    virtual std::vector<std::string> getTopics() const = 0;
    virtual void poll() = 0;
    virtual void pollEnd() = 0;
};

}
}

#endif //BLOOMBERG_COROKAFKA_ICONSUMER_MANAGER_H
