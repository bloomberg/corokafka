#ifndef BLOOMBERGLP_COROKAFKA_TESTS_TOPICS_H
#define BLOOMBERGLP_COROKAFKA_TESTS_TOPICS_H

#include <corokafka_tests_program_options.h>
#include <string>
#include <chrono>

namespace Bloomberg {
namespace corokafka {
namespace tests {

// Header 1
struct Header1 {
    uint16_t    _senderId{0};
    std::string _to;
    std::string _from;
};

// Header 2
struct Header2 {
    std::chrono::system_clock::time_point _timestamp;
};

struct Message {
    std::string _message;
};

// Topic type definitions
using Key = size_t;
using TestHeaders = Headers<Header1, Header2>;
using TopicWithHeaders = Topic<size_t, Message, TestHeaders>;
using TopicWithoutHeaders = Topic<size_t, Message>;

static constexpr const char* header1Str = "Header-1";
static constexpr const char* header2Str = "Header-2";

// Create a topic which will be shared between producer and consumer.
inline
const TopicWithHeaders& topicWithHeaders() {
    static TopicWithHeaders topic(programOptions()._topicWithHeaders, Header<Header1>(header1Str), Header<Header2>(header2Str));
    return topic;
}
inline
const TopicWithoutHeaders& topicWithoutHeaders() {
    static TopicWithoutHeaders topic(programOptions()._topicWithoutHeaders);
    return topic;
}

using MessageWithHeaders = ReceivedMessage<Key, Message, TestHeaders>;
using MessageWithoutHeaders = ReceivedMessage<Key, Message>;

}}}

namespace Bloomberg { namespace corokafka {
//======================================================================================================================
//                                              Serializers
//======================================================================================================================
template <>
struct Serialize<tests::Key>
{
    ByteArray operator()(const tests::Key &key)
    {
        uint8_t *it = (uint8_t *) &key;
        return {it, it + sizeof(tests::Key)};
    }
};

template <>
struct Serialize<tests::Header1>
{
    ByteArray operator()(const tests::Header1 &header)
    {
        ByteArray ret;
        ret.reserve(sizeof(uint16_t) + header._to.size() + header._from.size() + 2);
        uint8_t *it = (uint8_t *) &header._senderId;
        ret.insert(ret.end(), it, it + sizeof(uint16_t)); //senderId
        ret.push_back(header._to.size());
        ret.insert(ret.end(), header._to.begin(), header._to.end()); //to
        ret.push_back(header._from.size());
        ret.insert(ret.end(), header._from.begin(), header._from.end()); //from
        return ret;
    }
};

template <>
struct Serialize<tests::Header2>
{
    ByteArray operator()(const tests::Header2 &payload)
    {
        using Rep = std::chrono::system_clock::time_point::duration::rep;
        Rep count = payload._timestamp.time_since_epoch().count();
        uint8_t *it = (uint8_t *) &count;
        return {it, it + sizeof(Rep)};
    }
};

template <>
struct Serialize<tests::Message>
{
    ByteArray operator()(const tests::Message &payload)
    {
        return {payload._message.begin(), payload._message.end()};
    }
};

//======================================================================================================================
//                                              Deserializers
//======================================================================================================================
template <>
struct Deserialize<tests::Key>
{
    tests::Key operator()(const cppkafka::TopicPartition &, const cppkafka::Buffer &buf)
    {
        return *(tests::Key*) buf.get_data();
    }
};

template <>
struct Deserialize<tests::Header1>
{
    tests::Header1 operator()(const cppkafka::TopicPartition &, const cppkafka::Buffer &buf)
    {
        tests::Header1 header;
        header._senderId = *(uint16_t *) buf.get_data();
        uint8_t len = *(buf.get_data() + sizeof(uint16_t)); //to len
        unsigned const char *start = buf.get_data() + sizeof(uint16_t) + 1; //to start
        header._to = {start, start + len};
        len = *(start + len); //from len
        start = start + len + 1; //from start
        header._from = {start, start + len};
        return header;
    }
};

template <>
struct Deserialize<tests::Header2>
{
    tests::Header2 operator()(const cppkafka::TopicPartition &, const cppkafka::Buffer &buf)
    {
        using Rep = std::chrono::system_clock::time_point::duration::rep;
        Rep rep = *(Rep *) buf.get_data();
        return {std::chrono::system_clock::time_point(std::chrono::system_clock::duration(rep))};
    }
};

template <>
struct Deserialize<tests::Message>
{
    tests::Message operator()(const cppkafka::TopicPartition &, const cppkafka::Buffer &buf)
    {
        return {{buf.get_data(), buf.get_data() + buf.get_size()}};
    }
};

}} //Bloomberg::corokafka

#endif //BLOOMBERGLP_COROKAFKA_TESTS_TOPICS_H
