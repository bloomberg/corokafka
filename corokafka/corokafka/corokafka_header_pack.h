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
    friend class ConsumerManagerImpl;
public:
    using HeaderNode = std::pair<std::string, boost::any>;
    using ListType = std::deque<HeaderNode>;
    
    /**
     * @brief Creates an empty header pack.
     */
    HeaderPack() = default;
    
    /**
     * @brief Initialize the HeaderPack with a list of headers.
     * @param list The initializer list
     */
    HeaderPack(std::initializer_list<HeaderNode> list);
    
    /**
     * @brief Add a header to the front of the pack.
     * @param name The header name.
     * @param header The header.
     * @return
     */
    template <typename H>
    HeaderPack& push_front(const std::string& name, H&& header);
    
    /**
     * @brief Add a header to the back of the pack.
     * @param name The header name.
     * @param header The header.
     * @return
     */
    template <typename H>
    HeaderPack& push_back(const std::string& name, H&& header);
    
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
     * @brief Delete the header.
     * @param name The name of the header.
     */
    void erase(const std::string& name);
    
    /**
     * @brief Get the size of the pack.
     * @return The size.
     */
    size_t size() const;
    
    /**
     * @brief Check if this pack is empty.
     * @return True if empty, false otherwise.
     */
    bool empty() const;
    
    /**
     * @brief If true, the pack contains headers
     */
    explicit operator bool() const;
    
    /**
     * @brief Iterator to the beginning of the header pack.
     * @return An header iterator.
     * @remark If the pack is empty, begin()==end()
     */
    ListType::iterator begin();
    
    /**
     * @brief Const iterator to the beginning of the header pack.
     * @return An header const iterator.
     * @remark If the pack is empty, cbegin()==cend()
     */
    ListType::const_iterator cbegin() const;
    
    /**
     * @brief Iterator to the end of the header pack.
     * @return An header iterator.
     */
    ListType::iterator end();
    
    /**
     * @brief Const iterator to the end of the header pack.
     * @return An header const iterator.
     */
    ListType::const_iterator cend() const;
    
    /**
     * @brief Get all the header names.
     * @return A vector containing the names.
     */
    std::vector<std::string> getHeaderNames() const;
    
    /**
     * @brief Get the header by name.
     * @tparam H The header type.
     * @param name The name of the header.
     * @param nameIndex (optional) The index of the header name if not unique. If not specified (i.e. == -1) the first
     *                  header matching this name is returned.
     * @return The header object.
     */
    template <typename H>
    const H& get(const std::string& name, int nameIndex = -1) const &;
    
    /**
     * @brief Non-const version of the above.
     */
    template <typename H>
    H& get(const std::string& name, int nameIndex = -1) &;
    
    /**
     * @brief Move-able version of the above.
     */
    template <typename H>
    H&& get(const std::string& name, int nameIndex = -1) &&;
    
    /**
     * @brief Header accessors by index.
     * @tparam H The header type.
     * @param index The index where the header is at (>= 0).
     * @return A header reference wrapper..
     */
    template <typename H>
    HeaderRef<const H&> getAt(int index) const &;
    
    /**
     * @brief Non-const version of the above.
     */
    template <typename H>
    HeaderRef<H&> getAt(int index) &;
    
    /**
     * @brief Move-able version of the above.
     */
    template <typename H>
    HeaderRef<H&&> getAt(int index) &&;
    
private:
    HeaderPack& push_front(const std::string& name, boost::any&& header);
    
    HeaderPack& push_back(const std::string& name, boost::any&& header);
    
    ListType::const_iterator getImpl(const std::string& name, int nameIndex) const;
    
    ListType::iterator getImpl(const std::string& name, int nameIndex);
    
    // Members
    ListType    _headers;
};


}
}

#include <corokafka/impl/corokafka_header_pack_impl.h>

#endif //BLOOMBERG_COROKAFKA_HEADER_PACK_H
