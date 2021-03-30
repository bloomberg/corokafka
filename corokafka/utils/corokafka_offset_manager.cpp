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
#include <corokafka/utils/corokafka_offset_manager.h>
#include <corokafka/impl/corokafka_offset_manager_impl.h>
#include <corokafka/corokafka_utils.h>
#include <corokafka/corokafka_metadata.h>
#include <corokafka/corokafka_exception.h>
#include <corokafka/utils/corokafka_json_builder.h>
#include <cppkafka/detail/callback_invoker.h>
#include <algorithm>
#include <sstream>

namespace Bloomberg {
namespace corokafka {

OffsetManager::OffsetManager(corokafka::ConsumerManager& consumerManager) :
    ImplType(std::make_shared<OffsetManagerImpl>(consumerManager))
{
}

OffsetManager::OffsetManager(corokafka::ConsumerManager& consumerManager,
                             std::chrono::milliseconds brokerTimeout) :
    ImplType(std::make_shared<OffsetManagerImpl>(consumerManager, brokerTimeout))
{
}

cppkafka::Error OffsetManager::saveOffset(const cppkafka::TopicPartition& offset)
{
    return impl()->saveOffset(offset);
}

cppkafka::Error OffsetManager::saveOffset(const cppkafka::TopicPartition& offset,
                                          ExecMode execMode)
{
    return impl()->saveOffset(offset, execMode);
}

cppkafka::Error OffsetManager::saveOffset(const IMessage& message)
{
    return impl()->saveOffset(message);
}

cppkafka::Error OffsetManager::saveOffset(const IMessage& message,
                                          ExecMode execMode)
{
    return impl()->saveOffset(message, execMode);
}

cppkafka::TopicPartition OffsetManager::getCurrentOffset(const cppkafka::TopicPartition& partition)
{
    return impl()->getCurrentOffset(partition);
}

cppkafka::TopicPartition OffsetManager::getBeginOffset(const cppkafka::TopicPartition& partition)
{
    return impl()->getBeginOffset(partition);
}

cppkafka::Error OffsetManager::forceCommit()
{
    return impl()->forceCommit();
}

cppkafka::Error OffsetManager::forceCommit(ExecMode execMode)
{
    return impl()->forceCommit(execMode);
}

cppkafka::Error OffsetManager::forceCommit(const cppkafka::TopicPartition& partition)
{
    return impl()->forceCommit(partition);
}

cppkafka::Error OffsetManager::forceCommit(const cppkafka::TopicPartition& partition,
                                           ExecMode execMode)
{
    return impl()->forceCommit(partition, execMode);
}

cppkafka::Error OffsetManager::forceCommitCurrentOffset()
{
    return impl()->forceCommitCurrentOffset();
}

cppkafka::Error OffsetManager::forceCommitCurrentOffset(ExecMode execMode)
{
    return impl()->forceCommitCurrentOffset(execMode);
}

cppkafka::Error OffsetManager::forceCommitCurrentOffset(const cppkafka::TopicPartition& partition)
{
    return impl()->forceCommitCurrentOffset(partition);
}

cppkafka::Error OffsetManager::forceCommitCurrentOffset(const cppkafka::TopicPartition& partition,
                                                        ExecMode execMode)
{
    return impl()->forceCommitCurrentOffset(partition, execMode);
}

void OffsetManager::resetPartitionOffsets(ResetAction action)
{
    impl()->resetPartitionOffsets(action);
}

void OffsetManager::resetPartitionOffsets(const std::string& topic,
                                          ResetAction action)
{
    impl()->resetPartitionOffsets(topic, action);
}

std::string OffsetManager::toString() const
{
    return impl()->toString();
}

std::string OffsetManager::toString(const std::string& topic) const
{
    return impl()->toString(topic);
}

void OffsetManager::enableCommitTracing(bool enable,
                                        cppkafka::LogLevel level)
{
    impl()->enableCommitTracing(enable, level);
}

std::ostream& operator<<(std::ostream& output, const OffsetManager& rhs)
{
    return output << rhs.toString();
}

}}

