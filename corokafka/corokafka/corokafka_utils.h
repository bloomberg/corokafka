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

#include <corokafka/detail/corokafka_namespace_forwarding.h>
#include <corokafka/corokafka_utils.h>
#include <functional>
#include <algorithm>
#include <ctype.h>
#include <vector>
#include <chrono>

namespace Bloomberg {
namespace corokafka {

class Configuration;
class Metadata;
enum class KafkaType : char { Consumer, Producer };
enum class PartitionStrategy : char { Static,     ///< Manually assigned partitions
                                      Dynamic };  ///< Partitions are assigned by Kafka
enum class ExecMode : char { Sync,    ///< Execute synchronously
                             Async }; ///< Execute asynchronously
enum class OffsetPersistStrategy : char { Commit,  ///< Commits the offset to the broker
                                          Store }; ///< Stores locally in rdkafka
                             
LogLevel logLevelFromString(const std::string& level);

struct Empty{};

template <typename T>
struct is_empty
{
    constexpr static bool value{false};
};

template <>
struct is_empty<Empty>
{
    constexpr static bool value{true};
};

ssize_t getMaxMessageBuilderOutputLength();
void setMaxMessageBuilderOutputLength(ssize_t length);

struct TopicEntry {};
void handleException(const std::exception& ex,
                     const Metadata& metadata,
                     const Configuration& config,
                     LogLevel level);

using ByteArray = std::vector<uint8_t>;

// Throttling helper
// Returns true if throttling is over, false is we are still throttling
bool reduceThrottling(const std::chrono::steady_clock::time_point& currentTime,
                      std::chrono::steady_clock::time_point& throttleTime,
                      std::chrono::milliseconds& throttleDuration);

//======================================================================================================================
//                                               String comparisons
//======================================================================================================================
struct StringLessCompare
{
    struct CharLessCompare
    {
        bool operator() (const unsigned char& c1, const unsigned char& c2) const {
            return tolower (c1) < tolower (c2);
        }
    };
    bool operator() (const std::string & s1, const std::string & s2) const {
        return std::lexicographical_compare(s1.begin (), s1.end (), s2.begin (), s2.end (), CharLessCompare());
    }
};

struct StringEqualCompare
{
    struct CharEqualCompare
    {
        bool operator() (const unsigned char& c1, const unsigned char& c2) const {
            return tolower (c1) == tolower (c2);
        }
    };
    bool operator() (const std::string & s1, const std::string & s2) const {
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
    bool operator() (const std::string & s1, const std::string & s2, size_t num) const {
        if ((s1.size() < num) || (s2.size() < num)) {
            return false;
        }
#if (__cplusplus == 201103L)
        return std::equal(s1.begin(), s1.begin()+num, s2.begin(), CharEqualCompare());
#else
        return std::equal(s1.begin(), s1.begin()+num, s2.begin(), s2.begin()+num, CharEqualCompare());
#endif
    }
};

//======================================================================================================================
//                                               Unique pointer casts
//======================================================================================================================
using VoidPtr = std::unique_ptr<void, std::function<void(void*)>>;

/*
 * Cast from type U -> T
 */
template<typename T, typename DT,
         typename U, typename DU>
std::unique_ptr<T, DT> unique_pointer_cast(std::unique_ptr<U, DU>&& base, DT&& deleter = DT())
{
    return std::unique_ptr<T, DT>(static_cast<T*>(base.release()), std::forward<DT>(deleter));
}

/*
 * Partial specialization : U -> void
 */
template<typename U, typename DU>
VoidPtr unique_pointer_cast(std::unique_ptr<U, DU>&& base)
{
    return VoidPtr(static_cast<void*>(base.release()),
                   [](void* p){ typename std::unique_ptr<U, DU>::deleter_type()(static_cast<U*>(p)); });
}

/*
 * Partial specialization : void -> T
 */
template<typename T, typename DT = std::default_delete<T>>
std::unique_ptr<T, DT> unique_pointer_cast(VoidPtr&& base, DT&& d = DT())
{
    return std::unique_ptr<T, DT>(static_cast<T*>(base.release()), std::forward<DT>(d));
}

//======================================================================================================================
//                                               Stream operators
//======================================================================================================================
// Users may override for specific BufferTypes
template <typename T, typename C>
std::ostream& operator<<(std::ostream& stream, const BasicMessageBuilder<T, C>&) {
    return stream; //don't print anything by default
}

template <typename C>
std::ostream& operator<<(std::ostream& stream, const BasicMessageBuilder<std::string, C>& builder) {
    ssize_t max_len = getMaxMessageBuilderOutputLength();
    size_t payload_len = (max_len == -1) ? builder.payload().size() :
                         std::min(builder.payload().size(), (size_t)max_len);
    stream << "[topic:" << builder.topic() << "]"
           << "[partition:" << builder.partition() << "]"
           << "[key:" << builder.key() << "]"
           << "[length:" << builder.payload().size() << "]"
           << "[payload:" << builder.payload().substr(0, payload_len) << "]";
    if (builder.timestamp().count() > 0) {
        stream << "[timestamp:" << builder.timestamp().count() << "]";
    }
    return stream;
}

template <typename C>
std::ostream& operator<<(std::ostream& stream, const BasicMessageBuilder<std::vector<unsigned char>, C>& builder) {
    ssize_t max_len = getMaxMessageBuilderOutputLength();
    size_t payload_len = (max_len == -1) ? builder.payload().size() :
                          std::min(builder.payload().size(), (size_t)max_len);
    stream << "[topic:" << builder.topic() << "]"
           << "[partition:" << builder.partition() << "]"
           << "[key:" << std::string(builder.key().data(), builder.key().size()) << "]"
           << "[length:" << builder.payload().size() << "]"
           << "[payload:" << std::string(builder.payload().data(), payload_len) << "]";
    if (builder.timestamp().count() > 0) {
        stream << "[timestamp:" << builder.timestamp().count() << "]";
    }
    return stream;
}

// Specialized for Buffer container
std::ostream& operator<<(std::ostream& stream, const MessageBuilder& builder);

}
}

#endif //BLOOMBERG_COROKAFKA_UTILS_H
