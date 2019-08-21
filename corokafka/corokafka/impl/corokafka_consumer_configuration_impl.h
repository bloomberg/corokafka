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

namespace Bloomberg {
namespace corokafka {

template <typename FUNC>
void ConsumerConfiguration::setReceiverCallback(FUNC&& callback)
{
    using Key = typename quantum::Traits::InnerType<decltype(argType<0>(callback))>::Key;
    using Payload = typename quantum::Traits::InnerType<decltype(argType<0>(callback))>::Payload;
    _receiver.reset(new ConcreteReceiver<Key,Payload>(std::move(callback)));
}

template <typename FUNC>
void ConsumerConfiguration::setKeyCallback(FUNC&& callback)
{
    using Key = decltype(returnType(callback));
    _keyDeserializer.reset(new ConcreteDeserializer<Key>(std::forward<FUNC>(callback)));
}

template <typename FUNC>
void ConsumerConfiguration::setPayloadCallback(FUNC&& callback)
{
    using Payload = decltype(returnType(callback));
    _payloadDeserializer.reset(new ConcreteDeserializerWithHeaders<Payload>(std::forward<FUNC>(callback)));
}

template <typename FUNC>
void ConsumerConfiguration::setHeaderCallback(const std::string& name, FUNC&& callback)
{
    using Header = decltype(returnType(callback));
    _headerDeserializers[name].reset(new ConcreteDeserializer<Header>(std::forward<FUNC>(callback)));
}

template <typename T>
const Callbacks::KeyDeserializerCallback<T>& ConsumerConfiguration::getKeyCallback() const
{
    return std::static_pointer_cast<ConcreteDeserializer<T>>(_keyDeserializer)->getCallback();
}

template <typename T>
const Callbacks::PayloadDeserializerCallback<T>& ConsumerConfiguration::getPayloadCallback() const
{
    return std::static_pointer_cast<ConcreteDeserializer<T>>(_payloadDeserializer)->getCallback();
}

template <typename T>
const Callbacks::HeaderDeserializerCallback<T>& ConsumerConfiguration::getHeaderCallback(const std::string& name) const
{
    auto it = _headerDeserializers.find(name);
    if (it == _headerDeserializers.end()) {
        throw std::runtime_error("Invalid header name");
    }
    return std::static_pointer_cast<ConcreteDeserializer<T>>(it->second)->getCallback();
}

template <typename K, typename P>
const Callbacks::ReceiverCallback<K,P>& ConsumerConfiguration::getReceiverCallback() const
{
    return std::static_pointer_cast<ConcreteReceiver<K,P>>(_receiver)->getCallback();
}

}
}
