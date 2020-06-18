#ifndef BLOOMBERG_COROKAFKA_TOPIC_TRAITS_H
#define BLOOMBERG_COROKAFKA_TOPIC_TRAITS_H

#include <corokafka/corokafka_topic.h>
#include <corokafka/corokafka_utils.h>
#include <vector>
#include <tuple>
#include <memory>
#include <string>

namespace Bloomberg {
namespace corokafka {

struct TypeErasedDeserializer
{
    using DeserializerPtr = std::shared_ptr<Deserializer>;
    struct HeaderEntry {
        size_t          _pos{0};
        DeserializerPtr _deserializer;
    };
    using DeserializerMap = std::map<std::string, HeaderEntry>;
    
    TypeErasedDeserializer() = default;
    
    template <typename TOPIC>
    TypeErasedDeserializer(const TOPIC& topic) :
        _keyDeserializer(std::make_shared<ConcreteDeserializer<typename TOPIC::KeyType>>()),
        _payloadDeserializer(std::make_shared<ConcreteDeserializer<typename TOPIC::PayloadType>>()),
        _headerDeserializers(headerDeserializers(topic.headers(), std::make_index_sequence<TOPIC::HeadersType::NumHeaders>{}))
    {
        for (const auto& name : topic.headers().names()) {
            _headerEntries.push_back(_headerDeserializers.find(name));
        }
    }
    
    template <typename HEADERS, size_t...I>
    DeserializerMap headerDeserializers(const HEADERS& headers, std::index_sequence<I...>)
    {
        return {
            {headers.names()[I],
            {I, DeserializerPtr(std::make_shared<ConcreteDeserializer<typename std::tuple_element<I, typename HEADERS::HeaderTypes>::type>>())}}
             ...
        };
    }
    
    DeserializerPtr                               _keyDeserializer;
    DeserializerPtr                               _payloadDeserializer;
    DeserializerMap                               _headerDeserializers;
    std::vector<DeserializerMap::const_iterator>  _headerEntries;
};

}
}

#endif //BLOOMBERG_COROKAFKA_TOPIC_TRAITS_H
