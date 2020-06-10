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
#ifndef BLOOMBERG_COROKAFKA_HEADER_PACK_H
#define BLOOMBERG_COROKAFKA_HEADER_PACK_H

#include <corokafka/corokafka_utils.h>
#include <corokafka/corokafka_header_ref.h>
#include <boost/any.hpp>
#include <string>
#include <map>
#include <deque>
#include <iterator>
#include <initializer_list>

namespace Bloomberg {
namespace corokafka {

/**
 * @brief The HeaderPack class wraps the functionality of rdkafka message headers.
 *        The message header pack behaves essentially like a vector and the application can insert, view
 *        or delete headers at will using a vector-like syntax. Message headers are indexed either by
 *        name or by position in the list, however names are not unique in which case an index can be
 *        specified to differentiate.
 */
class HeaderPack
{
public:
    using HeaderNode = std::pair<std::string, boost::any>;
    using ListType = std::deque<HeaderNode>;
    
    /**
     * @brief Creates an empty header pack.
     */
    explicit HeaderPack(size_t numElements = 0);
    
    /**
     * @brief Initialize the HeaderPack with a list of headers.
     * @param list The initializer list
     */
    HeaderPack(std::initializer_list<HeaderNode> list);
    
    /**
     * @brief Remove a header from the front of the pack.
     * @return A reference to the header pack.
     */
    HeaderPack& pop_front();
    
    /**
     * @brief Remove a header from the back of the pack.
     * @return A reference to the header pack.
     */
    HeaderPack& pop_back();
    
    /**
     * @brief Delete the header(s) whose name is specified.
     * @param name The name of the header.
     * @param relativePosition (1-based) The N-th header to be checked if the name is not unique.
     *        If (relativePosition == 0), then all headers with the corresponding names will be erased.
     */
    void erase(const std::string& name, size_t relativePosition = 0);
    
    /**
     * @brief Erase a specific header using its iterator.
     * @param it The iterator.
     */
    void erase(const ListType::iterator& it);
    
    /**
     * @brief Get the size of the pack.
     * @return The size.
     */
    size_t size() const noexcept;
    
    /**
     * @brief Check if this pack is empty.
     * @return True if empty, false otherwise.
     */
    bool empty() const noexcept;
    
    /**
     * @brief If true, the pack contains headers
     */
    explicit operator bool() const noexcept;
    
    /**
     * @brief Iterator to the beginning of the header pack.
     * @return An header iterator.
     * @remark If the pack is empty, begin()==end()
     */
    ListType::iterator begin() noexcept;
    
    /**
     * @brief Const iterator to the beginning of the header pack.
     * @return An header const iterator.
     * @remark If the pack is empty, cbegin()==cend()
     */
    ListType::const_iterator cbegin() const noexcept;
    
    /**
     * @brief Iterator to the end of the header pack.
     * @return An header iterator.
     */
    ListType::iterator end() noexcept;
    
    /**
     * @brief Const iterator to the end of the header pack.
     * @return An header const iterator.
     */
    ListType::const_iterator cend() const noexcept;
    
    /**
     * @brief Get all the header names.
     * @return A vector containing the names.
     */
    std::vector<std::string> getHeaderNames() const;
    
    /**
     * @brief Get the header by name.
     * @tparam H The header type.
     * @param name The name of the header.
     * @param relativePosition (1-based) The N-th header to be returned if the name is not unique.
     * @return The header object.
     */
    template <typename H>
    const H& get(const std::string& name, size_t relativePosition = 1) const &;
    
    /**
     * @brief Non-const version of the above.
     */
    template <typename H>
    H& get(const std::string& name, size_t relativePosition = 1) &;
    
    /**
     * @brief Move-able version of the above.
     */
    template <typename H>
    H&& get(const std::string& name, size_t relativePosition = 1) &&;
    
    /**
     * @brief Header accessors by index.
     * @tparam H The header type.
     * @param index The index where the header is at (>= 0).
     * @return A header reference wrapper.
     * @note The header must be valid otherwise this call with throw.
     */
    template <typename H>
    HeaderRef<const H&> getAt(size_t index) const &;
    
    /**
     * @brief Non-const version of the above.
     */
    template <typename H>
    HeaderRef<H&> getAt(size_t index) &;
    
    /**
     * @brief Move-able version of the above.
     */
    template <typename H>
    HeaderRef<H&&> getAt(size_t index) &&;
    
    /**
     * @brief Returns the number of valid headers (i.e. non-empty)
     * @return The size.
     */
    size_t numValidHeaders() const;
    
    /**
     * @brief Checks if the header at position 'index' is valid (i.e. non-empty)
     * @return True if the header is valid
     */
    bool isValidAt(size_t index) const;
    
    /**
     * @brief Checks if the header with specified name and index is valid.
     * @param name The header name
     * @param relativePosition (1-based) The N-th header to be validated if the name is not unique.
     * @return True if the header is valid
     */
    bool isValid(const std::string& name, size_t relativePosition = 1) const;
    
private:
    friend class ConsumerManagerImpl;
    template <typename H>
    HeaderPack& push_front(const std::string& name, H&& header);
    template <typename H>
    HeaderPack& push_back(const std::string& name, H&& header);
    HeaderPack& push_back(const std::string& name, boost::any&& header);
    HeaderPack& push_front(const std::string& name, boost::any&& header);
    ListType::const_iterator getImpl(const std::string& name, size_t relativePosition) const;
    ListType::iterator getImpl(const std::string& name, size_t relativePosition);
    HeaderNode& operator[](size_t index);
    
    // Members
    ListType    _headers;
};


}
}

#include <corokafka/impl/corokafka_header_pack_impl.h>

#endif //BLOOMBERG_COROKAFKA_HEADER_PACK_H
