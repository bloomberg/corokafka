#ifndef BLOOMBERG_COROKAFKA_HEADERS_H
#define BLOOMBERG_COROKAFKA_HEADERS_H

#include <string>
#include <tuple>
#include <type_traits>

namespace Bloomberg {
namespace corokafka {

/**
 * @brief The Header class represents a single rdkafka header.
 * @tparam T The type of the header
 */
template <typename T>
struct Header
{
    using HeaderType = T;
    static_assert(IsSerializable<T>::value, "Header is not serializable");
    
    /**
     * @brief Builds a header
     * @param name The name associated with this header
     */
    explicit Header(std::string name) : _name(std::move(name))
    {}
    
    /**
     * @brief Get the name of this header
     * @return The name
     */
    const std::string &name() const
    { return _name; }

private:
    std::string _name;
};

/**
 * @brief This class represents a collection of headers which will be produced/consumed in a topic.
 * @tparam T A list of header types.
 */
template <typename ... T>
struct Headers
{
    using HeaderTypes = std::tuple<T...>;
    template <size_t I>
    using HeaderType = typename std::tuple_element<I, Headers>::type;
    static constexpr size_t NumHeaders = sizeof...(T);
    static_assert(forAll<IsSerializable<T>::value...>(), "Headers are not serializable");
    
    /**
     * @brief Builds this object using a variable list of 'Header' objects.
     * @param headers The headers comprising this collection.
     */
    Headers(Header<T>...headers) : _names{std::move(headers.name())...}
    {}
    
    /**
     * @brief Get all the header names
     * @return The names
     */
    const std::vector<std::string> &names() const
    { return _names; }

private:
    std::vector<std::string> _names;
};

/**
 * @brief Helper function to make a Headers object from a list of Header objects.
 * @tparam T The list of header types
 * @param headers The headers.
 * @return A Headers collection.
 */
template <typename... T>
Headers<T...> makeHeaders(Header<T>...headers)
{
    return Headers<T...>(std::move(headers)...);
}

}}

#endif //BLOOMBERG_COROKAFKA_HEADERS_H
