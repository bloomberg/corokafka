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
#ifndef BLOOMBERG_COROKAFKA_UTILS_H
#define BLOOMBERG_COROKAFKA_UTILS_H

#include <corokafka/utils/corokafka_json_builder.h>
#include <corokafka/third_party/cppkafka/cppkafka.h>
#include <quantum/quantum.h>
#include <functional>
#include <algorithm>
#include <cctype>
#include <vector>
#include <chrono>
#include <atomic>
#include <typeinfo>
#include <cstring>
#if (__GLIBC__ >= 2) && (__GLIBC_MINOR__ >= 12)
#include <pthread.h> //pthread_setname_np()
#endif

//======================================================================================================================
//                                               Disjunction port
//======================================================================================================================
#if (__cplusplus < 201703L)
namespace std
{
    template <class...> struct disjunction : std::false_type
    {
    };
    template <class B1> struct disjunction<B1> : B1
    {
    };
    template <class B1, class... Bn>
    struct disjunction<B1, Bn...> : std::conditional_t<bool(B1::value), B1, disjunction<Bn...>>
    {
    };
} //namespace std
#endif

namespace Bloomberg {
namespace corokafka {

// Forward types
class TopicConfiguration;
class Metadata;

enum class KafkaType : char
{
    Consumer,
    Producer
};
enum class PartitionStrategy : char
{
    Static,  ///< Manually assigned partitions
    Dynamic  ///< Partitions are assigned by Kafka
};
enum class ExecMode : char
{
    Sync,    ///< Execute synchronously
    Async    ///< Execute asynchronously
};
enum class OffsetPersistStrategy : char
{
    Commit,  ///< Commits the offset to the broker
    Store    ///< Stores locally in rdkafka
};
enum class ThreadType : char
{
    Coro,  ///< Thread used to run coroutines
    IO     ///< Thread for IO completion
};
enum class TimerValues : char
{
    Disabled = -2,  ///< Not taking effect
    Unlimited = -1  ///< Blocks indefinitely
};
enum class SizeLimits : char
{
    Unlimited = -1 ///< Unlimited size or unbounded
};
enum class PollStrategy : char
{
    Batch,          ///< Reads messages in batches
    RoundRobin,     ///< Reads messages from each partition at a time, in round-robin fashion
    Serial          ///< Default consumer. Reads messages as they arrive.
};
enum class OptionsPermission : char
{
    RdKafkaDisallow,
    RdKafkaAllow
};

template <typename Enum>
constexpr auto EnumValue(Enum e) noexcept
{
    return static_cast<std::underlying_type_t<Enum>>(e);
}

ssize_t& maxMessageBuilderOutputLength();

struct Interruptible
{
    std::atomic_bool _interrupt{false};
};

void handleException(const std::exception &ex,
                     const Metadata &metadata,
                     const TopicConfiguration &config,
                     cppkafka::LogLevel level);

using ByteArray = std::vector<uint8_t>;

//======================================================================================================================
//                                               String comparisons
//======================================================================================================================
struct StringLessCompare
{
    struct CharLessCompare
    {
        bool operator()(unsigned char c1, unsigned char c2) const
        {
            return tolower(c1) < tolower(c2);
        }
    };
    
    bool operator()(const std::string &s1, const std::string &s2) const
    {
        return std::lexicographical_compare(s1.begin(), s1.end(), s2.begin(), s2.end(), CharLessCompare());
    }
};

struct StringEqualCompare
{
    struct CharEqualCompare
    {
        bool operator()(unsigned char c1, unsigned char c2) const
        {
            return tolower(c1) == tolower(c2);
        }
    };
    
    bool operator()(const std::string &s1, const std::string &s2) const
    {
#if (__cplusplus == 201103L)
        if (s1.size() == s2.size()) {
            return std::equal(s1.begin(), s1.end(), s2.begin(), CharEqualCompare());
        }
        return false;
#else
        return std::equal(s1.begin(), s1.end(), s2.begin(), s2.end(), CharEqualCompare());
#endif
    }
    
    // Compare the first 'n' characters
    bool operator()(const std::string &s1, const std::string &s2, size_t num) const
    {
        if ((s1.size() < num) || (s2.size() < num)) {
            return false;
        }
#if (__cplusplus == 201103L)
        return std::equal(s1.begin(), s1.begin()+num, s2.begin(), CharEqualCompare());
#else
        return std::equal(s1.begin(), s1.begin() + num, s2.begin(), s2.begin() + num, CharEqualCompare());
#endif
    }
};

// Trim whitespaces at both ends
static inline void trim(std::string& str) {
    str.erase(str.begin(), std::find_if(str.begin(), str.end(), [](int c)->bool {
        return !std::isspace(c);
    }));
    str.erase(std::find_if(str.rbegin(), str.rend(), [](int c)->bool {
        return !std::isspace(c);
    }).base(), str.end());
}

//======================================================================================================================
//                                               Unique pointer casts
//======================================================================================================================
using VoidPtr = std::unique_ptr<void, std::function<void(void *)>>;

/*
 * Cast from type U -> T
 */
template <typename T, typename DT,
          typename U, typename DU>
std::unique_ptr<T, DT> unique_pointer_cast(std::unique_ptr<U, DU> &&base, DT &&deleter = DT())
{
    return std::unique_ptr<T, DT>(static_cast<T *>(base.release()), std::forward<DT>(deleter));
}

/*
 * Partial specialization : U -> void
 */
template <typename U, typename DU>
VoidPtr unique_pointer_cast(std::unique_ptr<U, DU> &&base)
{
    return VoidPtr(static_cast<void *>(base.release()),
                   [](void *p)
                   { typename std::unique_ptr<U, DU>::deleter_type()(static_cast<U *>(p)); });
}

/*
 * Partial specialization : void -> T
 */
template <typename T, typename DT = std::default_delete<T>>
std::unique_ptr<T, DT> unique_pointer_cast(VoidPtr &&base, DT &&d = DT())
{
    return std::unique_ptr<T, DT>(static_cast<T *>(base.release()), std::forward<DT>(d));
}

//======================================================================================================================
//                                               Stream operators
//======================================================================================================================
template <typename C>
std::ostream &operator<<(std::ostream &stream, const cppkafka::BasicMessageBuilder<std::string, C> &builder)
{
    ssize_t max_len = maxMessageBuilderOutputLength();
    size_t payload_len = (max_len == -1) ? builder.payload().size() :
                         std::min(builder.payload().size(), static_cast<size_t>(max_len));
    JsonBuilder json(stream);
    json.startMember("messageBuilder").
        tag("topic", builder.topic()).
        tag("partition", builder.partition()).
        tag("key", builder.key()).
        tag("length", builder.payload().size()).
        tag("payload", builder.payload().substr(0, payload_len));
    if (builder.timestamp().count() > 0) {
        json.tag("timestamp", builder.timestamp().count());
    }
    json.endMember().end();
    return stream;
}

template <typename C>
std::ostream &operator<<(std::ostream &stream,
                         const cppkafka::BasicMessageBuilder<std::vector<unsigned char>, C> &builder)
{
    ssize_t max_len = maxMessageBuilderOutputLength();
    size_t payload_len = (max_len == -1) ? builder.payload().size() :
                         std::min(builder.payload().size(), static_cast<size_t>(max_len));
    JsonBuilder json(stream);
    json.startMember("messageBuilder").
        tag("topic", builder.topic()).
        tag("partition", builder.partition()).
        tag("key", std::string(builder.key().data(), builder.key().size())).
        tag("length", std::string(builder.payload().data(), payload_len)).
        tag("payload", builder.payload().substr(0, payload_len));
    if (builder.timestamp().count() > 0) {
        json.tag("timestamp", builder.timestamp().count());
    }
    json.endMember().end();
    return stream;
}

template <typename C>
std::ostream& operator<<(std::ostream& stream,
                         const cppkafka::BasicMessageBuilder<cppkafka::Buffer, C> &builder) {
    ssize_t max_len = maxMessageBuilderOutputLength();
    size_t payload_len = (max_len == -1) ? builder.payload().get_size() :
                         std::min(builder.payload().get_size(), static_cast<size_t>(max_len));
    JsonBuilder json(stream);
    json.startMember("messageBuilder").
        tag("topic", builder.topic()).
        tag("partition", builder.partition()).
        tag("key", (std::string)builder.key()).
        tag("length", builder.payload().get_size()).
        tag("payload", std::string((const char*)builder.payload().get_data(), payload_len));
    if (builder.timestamp().count() > 0) {
        json.tag("timestamp", builder.timestamp().count());
    }
    json.endMember().end();
    return stream;
}

//======================================================================================================================
//                                               Serializable Concept
//======================================================================================================================
template <bool B = false>
struct CheckBoolean : std::false_type
{
};

template <>
struct CheckBoolean<true> : std::true_type
{
};

template <bool... Bs>
constexpr bool forAll()
{
    bool values[sizeof...(Bs)]{Bs...};
    for (auto b : values)
        if (!b) return false;
    return true;
}

// Generic implementations
template<typename T>
struct Serialize {
    ByteArray operator()(const T&);
};

template <typename T>
struct Deserialize {
    T operator()(const cppkafka::TopicPartition&, const cppkafka::Buffer&);
};

template <class T>
static auto checkSerialize(T*)
-> CheckBoolean<std::is_same<decltype(Serialize<T>{}(std::declval<T>())), ByteArray>::value &&
                std::is_same<decltype(Deserialize<T>{}(cppkafka::TopicPartition{}, cppkafka::Buffer{})), T>::value>;

template <class>
static auto checkSerialize(...) -> std::false_type;

template <class T>
struct IsSerializable : decltype(checkSerialize<T>((T*)0))
{
};

//======================================================================================================================
//                                               Traits
//======================================================================================================================
template <typename FUNC>
auto returnType(FUNC &&func) ->
    typename quantum::FunctionArguments<decltype(quantum::Callable::ref(func))>::RetType;

template <size_t N, typename FUNC>
auto argType(FUNC &&func) ->
    typename quantum::FunctionArguments<decltype(quantum::Callable::ref(func))>::template ArgType<N>;

template <typename T, typename... U>
struct Includes : std::disjunction<std::is_same<T, U>...>
{
};

template <typename T, typename Tuple>
struct TupleIncludes;

template <typename T, typename...U>
struct TupleIncludes<T, std::tuple<U...>> : Includes<T, U...>
{
};

struct NullHeader{};

template <typename Tup1, typename Tup2, size_t ... I>
constexpr bool matchAllTypes(std::index_sequence<I...>)
{
    return forAll<std::is_same<std::tuple_element_t<I,Tup1>, std::tuple_element_t<I,Tup2>>::value ||
                  std::is_same<NullHeader, std::tuple_element_t<I,Tup2>>::value...>();
}

template <typename Tup, typename ... T>
constexpr bool matchAllTypes()
{
    using Tup2 = std::tuple<T...>;
    if (std::tuple_size<Tup>::value != sizeof...(T)) return false;
    return matchAllTypes<Tup,Tup2>(std::make_index_sequence<sizeof...(T)>{});
}

//Wrapper for ProducerMessageBuilder<T> to allow for default construction needed in futures
template <typename T>
struct ProducerMessageBuilder : public cppkafka::ConcreteMessageBuilder<T>
{
    ProducerMessageBuilder() : cppkafka::ConcreteMessageBuilder<T>("") {}
    using cppkafka::ConcreteMessageBuilder<T>::ConcreteMessageBuilder;
};

using IoTracker = std::shared_ptr<int>;

//======================================================================================================================
//                                               Misc
//======================================================================================================================
/**
 * name: 8 letters if threadId==-1, 7 letters if threadId < 10, 6 letters if threadId > 9
 * threadId: 0-99
 */
#if (__GLIBC__ >= 2) && (__GLIBC_MINOR__ >= 12)
inline
void setThreadName(std::thread::native_handle_type threadHandle,
                   const char* name, //max 7 letters. 6 letters if threadId >= 10
                   int threadId = -1) //0-99
{
    int idx = 0;
    char buf[16] = {0};
    std::strncpy(buf, "corokf:", 7); idx += 7;
    if (threadId == -1) {
        //no digits used for thread id
        int len = std::min(8, (int)std::strlen(name));
        std::strncpy(buf+idx, name, len); idx += len;
    }
    else {
        //use 1 digit for string
        std::string id = std::to_string(threadId);
        int len = std::min((int)(8-id.length()), (int)std::strlen(name));
        std::strncpy(buf+idx, name, len); idx += len;
        std::strncpy(buf+idx, id.c_str(), id.length()); idx += id.length();
    }
    buf[idx]=0;
    pthread_setname_np(threadHandle, buf);
}
#endif

} //namespace corokafka
} //namespace Bloomberg

#endif //BLOOMBERG_COROKAFKA_UTILS_H
