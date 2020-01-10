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

#include <corokafka/corokafka_exception.h>

namespace Bloomberg {
namespace corokafka {

template <typename TOPIC>
ConsumerConfiguration::ConsumerConfiguration(const TOPIC& topic,
                                             OptionList options,
                                             OptionList topicOptions,
                                             Callbacks::ReceiverCallback<TOPIC> receiver) :
    TopicConfiguration(KafkaType::Consumer, topic.topic(), std::move(options), std::move(topicOptions)),
    _typeErasedDeserializer(topic),
    _receiver(new ConcreteReceiver<TOPIC>(std::move(receiver)))
{
    static_assert(TOPIC::isSerializable(), "Topic contains types which are not serializable");
}

template <typename TOPIC>
ConsumerConfiguration::ConsumerConfiguration(const TOPIC& topic,
                                             OptionInitList options,
                                             OptionInitList topicOptions,
                                             Callbacks::ReceiverCallback<TOPIC> receiver) :
    TopicConfiguration(KafkaType::Consumer, topic.topic(), std::move(options), std::move(topicOptions)),
    _typeErasedDeserializer(topic),
    _receiver(new ConcreteReceiver<TOPIC>(std::move(receiver)))
{
    static_assert(TOPIC::isSerializable(), "Topic contains types which are not serializable");
}

template <typename TOPIC>
void ConsumerConfiguration::setReceiverCallback(const TOPIC& topic,
                                                Callbacks::ReceiverCallback<TOPIC> receiver)
{
    static_assert(TOPIC::isSerializable(), "Topic contains types which are not serializable");
    if (_receiver) {
        throw ConfigurationException(getTopic(), "Receiver callback is already set");
    }
    _typeErasedDeserializer = {topic};
    _receiver.reset(new ConcreteReceiver<TOPIC>(std::move(receiver)));
}

template <typename TOPIC>
const Callbacks::ReceiverCallback<TOPIC>& ConsumerConfiguration::getReceiverCallback() const
{
    return *std::static_pointer_cast<ConcreteReceiver<TOPIC>>(_receiver)->getCallback();
}

}
}
