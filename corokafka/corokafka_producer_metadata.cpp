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
#include <corokafka/corokafka_producer_metadata.h>
#include <corokafka/corokafka_exception.h>
#include <corokafka/impl/corokafka_producer_metadata_impl.h>

namespace Bloomberg {
namespace corokafka {

//=============================================================================
//                              PRODUCER METADATA
//=============================================================================

ProducerMetadata::ProducerMetadata(const std::string& topic,
                                   cppkafka::BufferedProducer<ByteArray>* producer,
                                   std::chrono::milliseconds brokerTimeout) :
    ImplType(std::make_shared<ProducerMetadataImpl>(topic, producer, brokerTimeout)),
    Metadata(std::static_pointer_cast<ProducerMetadataImpl>(ImplType::impl()))
{
}

ProducerMetadata::ProducerMetadata(const std::string& topic,
                                   const cppkafka::Topic& kafkaTopic,
                                   cppkafka::BufferedProducer<ByteArray>* producer,
                                   std::chrono::milliseconds brokerTimeout) :
    ImplType(std::make_shared<ProducerMetadataImpl>(topic, kafkaTopic, producer, brokerTimeout)),
    Metadata(std::static_pointer_cast<ProducerMetadataImpl>(ImplType::impl()))
{
}

ProducerMetadata::ProducerMetadata(std::shared_ptr<IProducerMetadata> impl) :
    ImplType(std::move(impl)),
    Metadata(std::static_pointer_cast<ProducerMetadataImpl>(ImplType::impl()))
{
}

const cppkafka::TopicPartitionList& ProducerMetadata::getTopicPartitions() const
{
    return ImplType::impl()->getTopicPartitions();
}

size_t ProducerMetadata::getOutboundQueueLength() const
{
    return ImplType::impl()->getOutboundQueueLength();
}

size_t ProducerMetadata::getInternalQueueLength() const
{
    return ImplType::impl()->getInternalQueueLength();
}

}
}
