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
#ifndef BLOOMBERG_COROKAFKA_PRODUCER_CONFIGURATION_IMPL_H
#define BLOOMBERG_COROKAFKA_PRODUCER_CONFIGURATION_IMPL_H

namespace Bloomberg {
namespace corokafka {

// Implementations
template <typename FUNC>
void ProducerConfiguration::setKeyCallback(FUNC&& callback)
{
    using Key = typename std::remove_reference<decltype(argType<0>(callback))>::type;
    _keySerializer.reset(new ConcreteSerializer<Key>(std::forward<FUNC>(callback)));
}

template <typename FUNC>
void ProducerConfiguration::setPayloadCallback(FUNC&& callback)
{
    using Payload = typename std::remove_reference<decltype(argType<1>(callback))>::type;
    _payloadSerializer.reset(new ConcreteSerializerWithHeaders<Payload>(std::forward<FUNC>(callback)));
}

template <typename FUNC>
void ProducerConfiguration::setHeaderCallback(const std::string& name, FUNC&& callback)
{
    using Header = typename std::remove_reference<decltype(argType<0>(callback))>::type;
    _headerSerializers[name].reset(new ConcreteSerializer<Header>(std::forward<FUNC>(callback)));
}

template <typename T>
const Callbacks::KeySerializerCallback<T>& ProducerConfiguration::getKeyCallback() const
{
    return std::static_pointer_cast<ConcreteSerializer<T>>(_keySerializer)->getCallback();
}

template <typename T>
const Callbacks::PayloadSerializerCallback<T>& ProducerConfiguration::getPayloadCallback() const
{
    return std::static_pointer_cast<ConcreteSerializer<T>>(_payloadSerializer)->getCallback();
}

template <typename T>
const Callbacks::HeaderSerializerCallback<T>& ProducerConfiguration::getHeaderCallback(const std::string& name) const
{
    auto it = _headerSerializers.find(name);
    if (it == _headerSerializers.end()) {
        throw std::runtime_error("Invalid header name");
    }
    return std::static_pointer_cast<ConcreteSerializer<T>>(it->second)->getCallback();
}

}
}

#endif //BLOOMBERG_COROKAFKA_PRODUCER_CONFIGURATION_IMPL_H
