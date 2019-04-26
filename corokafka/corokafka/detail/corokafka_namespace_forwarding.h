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
#ifndef BLOOMBERG_COROKAFKA_NAMESPACE_FORWARDING_H
#define BLOOMBERG_COROKAFKA_NAMESPACE_FORWARDING_H

#include <cppkafka/cppkafka.h>

namespace Bloomberg {
namespace corokafka {

using BackoffCommitter          = cppkafka::BackoffCommitter;
using BackoffPerformer          = cppkafka::BackoffPerformer;
template <typename T, typename U>
using BasicMessageBuilder       = cppkafka::BasicMessageBuilder<T,U>;
using Buffer                    = cppkafka::Buffer;
template <typename T>
using BufferedProducer          = cppkafka::BufferedProducer<T>;
template <typename T>
using CallbackInvoker           = cppkafka::CallbackInvoker<T>;
template <typename T>
using ConcreteMessageBuilder    = cppkafka::ConcreteMessageBuilder<T>;
using ConfigurationOption       = cppkafka::ConfigurationOption;
using Consumer                  = cppkafka::Consumer;
using Error                     = cppkafka::Error;
using GroupInformation          = cppkafka::GroupInformation;
using KafkaConfiguration        = cppkafka::Configuration;
using KafkaHandleBase           = cppkafka::KafkaHandleBase;
using LogLevel                  = cppkafka::LogLevel;
using Message                   = cppkafka::Message;
using MessageBuilder            = cppkafka::MessageBuilder;
using MessageTimestamp          = cppkafka::MessageTimestamp;
using KafkaMetadata             = cppkafka::Metadata;
using Producer                  = cppkafka::Producer;
using RoundRobinPollStrategy    = cppkafka::RoundRobinPollStrategy;
using Topic                     = cppkafka::Topic;
using TopicConfiguration        = cppkafka::TopicConfiguration;
using TopicMetadata             = cppkafka::TopicMetadata;
using TopicPartition            = cppkafka::TopicPartition;
using TopicPartitionList        = cppkafka::TopicPartitionList;
template <typename T>
using Header                    = cppkafka::Header<T>;
template <typename T>
using HeaderList                = cppkafka::HeaderList<T>;

// Exceptions
using ConfigException           = cppkafka::ConfigException;
using ConfigOptionNotFound      = cppkafka::ConfigOptionNotFound;
using ConsumerException         = cppkafka::ConsumerException;
using ElementNotFound           = cppkafka::ElementNotFound;
using Exception                 = cppkafka::Exception;
using HandleException           = cppkafka::HandleException;
using InvalidConfigOptionType   = cppkafka::InvalidConfigOptionType;
using ParseException            = cppkafka::ParseException;
using QueueException            = cppkafka::QueueException;
using ActionTerminatedException = cppkafka::ActionTerminatedException;
using UnexpectedVersion         = cppkafka::UnexpectedVersion;

}
}

#endif //BLOOMBERG_COROKAFKA_NAMESPACE_FORWARDING_H
