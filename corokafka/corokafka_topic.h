#ifndef BLOOMBERG_COROKAFKA_TOPIC_H
#define BLOOMBERG_COROKAFKA_TOPIC_H

#include <corokafka/corokafka_headers.h>

namespace Bloomberg {
namespace corokafka {

template <typename KEY, typename PAYLOAD, typename HEADERS>
class ReceivedMessage;

/**
 * @brief This class represents a kafka topic.
 * @tparam KEY The key type.
 * @tparam PAYLOAD The payload type
 * @tparam HEADERS A Headers<...> type
 */
template <typename KEY, typename PAYLOAD, typename HEADERS = Headers<>>
struct Topic {
    using KeyType = KEY;
    using PayloadType = PAYLOAD;
    using HeadersType = HEADERS;
    template <size_t I>
    using HeaderType = typename HeadersType::template HeaderType<I>::type;
    using PackedType = ByteArray;
    using ReceivedMessageType = ReceivedMessage<KeyType, PayloadType, HeadersType>;
    
    static constexpr bool isSerializable() {
        return IsSerializable<KeyType>::value &&
               IsSerializable<PayloadType>::value &&
               HeadersType::isSerializable();
    }
    
    /**
     * @brief Construct a Topic with a variable list of Header<> types
     * @tparam T The list of header types.
     * @param topic The topic name.
     * @param headers The list of headers.
     */
    template <typename ... T>
    Topic(std::string topic, Header<T>...headers) :
        _topic(std::move(topic)),
        _headers(makeHeaders(std::move(headers)...))
    {}
    
    /**
     * @brief Construct a Topic with a Headers<...> collection type.
     * @param topic The topic name.
     * @param headers The headers collection
     */
    Topic(std::string topic, HeadersType headers) :
        _topic(std::move(topic)),
        _headers(std::move(headers))
    {}
    
    /**
     * @brief Get the topic name
     * @return The name
     */
    const std::string& topic() const { return _topic; }
    
    /**
     * @brief Get the headers for this topic
     * @return The Headers<...> collection which this topic contains
     */
     const HeadersType& headers() const { return _headers; }
private:
    std::string     _topic;
    HeadersType     _headers;
};

/**
 * @brief Helper function to build a Topic object
 * @tparam KEY The key type
 * @tparam PAYLOAD The payload type
 * @tparam T The header types
 * @param topic The topic name
 * @param headers The variable list of Header objects
 * @return A topic
 */
template <typename KEY, typename PAYLOAD, typename ... T>
Topic<KEY, PAYLOAD, Headers<T...>> makeTopic(std::string topic, Header<T>...headers)
{
    return Topic<KEY, PAYLOAD, Headers<T...>>(std::move(topic), std::move(headers)...);
}

}
}

#endif //BLOOMBERG_COROKAFKA_TOPIC_H
