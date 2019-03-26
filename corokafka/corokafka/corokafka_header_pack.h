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
#ifndef BLOOMBERG_COROKAFKA_HEADER_MAP_H
#define BLOOMBERG_COROKAFKA_HEADER_MAP_H

#include <corokafka/corokafka_utils.h>
#include <boost/any.hpp>
#include <string>
#include <map>
#include <deque>
#include <iterator>

namespace Bloomberg {
namespace corokafka {

template <typename HEADER>
class HeaderRef
{
public:
    HeaderRef(const std::string& name,
              HEADER value) :
        _name(name),
        _value(std::forward<HEADER>(value))
    {}
    const std::string& name() const { return _name; }
    HEADER value() const { return std::forward<HEADER>(_value); }
private:
    const std::string& _name;
    HEADER             _value;
};

class HeaderPack
{
public:
    using ListType = std::deque<std::pair<std::string, boost::any>>;
    
    HeaderPack& push_front(const std::string& name, boost::any&& header) {
        if (name.empty()) {
            throw std::invalid_argument("Header name cannot be empty");
        }
        _headers.emplace_front(name, std::move(header));
        return *this;
    }
    
    HeaderPack& push_front(const std::string& name, const char* header) {
        return push_front(name, boost::any(std::string(header)));
    }
    
    template <typename H>
    HeaderPack& push_front(const std::string& name, H&& header) {
        return push_front(name, boost::any(std::forward<H>(header)));
    }
    
    HeaderPack& push_back(const std::string& name, boost::any&& header) {
        if (name.empty()) {
            throw std::invalid_argument("Header name cannot be empty");
        }
        _headers.emplace_back(name, std::move(header));
        return *this;
    }
    
    HeaderPack& push_back(const std::string& name, const char* header) {
        return push_back(name, boost::any(std::string(header)));
    }
    
    template <typename H>
    HeaderPack& push_back(const std::string& name, H&& header) {
        return push_back(name, boost::any(std::forward<H>(header)));
    }
    
    template <typename H>
    HeaderPack& pop_front() {
        _headers.pop_front();
        return *this;
    }
    
    template <typename H>
    HeaderPack& pop_back() {
        _headers.pop_back();
        return *this;
    }
    
    void erase(const std::string& name) {
        _headers.erase(std::remove_if(_headers.begin(), _headers.end(), [&name](const auto& entry)->bool {
            return StringEqualCompare()(entry.first, name);
        }), _headers.end());
    }
    
    size_t size() const {
        return _headers.size();
    }
    
    bool empty() const {
        return _headers.size() == 0;
    }
    
    explicit operator bool() const {
        return !empty();
    }
    
    // Iterator access to underlying container
    ListType::iterator begin() { return _headers.begin(); }
    ListType::const_iterator cbegin() const { return _headers.cbegin(); }
    ListType::iterator end() { return _headers.end(); }
    ListType::const_iterator cend() const { return _headers.cend(); }
    
    std::vector<std::string> getHeaderNames() const {
        std::vector<std::string> names;
        names.reserve(size());
        for (const auto& header : _headers) {
            names.emplace_back(header.first);
        }
        return names;
    }
    
    template <typename H>
    const H& get(const std::string& name, size_t nameIndex = 0) const & {
        return boost::any_cast<const H&>(getImpl(name, nameIndex)->second);
    }
    
    template <typename H>
    H& get(const std::string& name, size_t nameIndex = 0) & {
        return boost::any_cast<H&>(getImpl(name, nameIndex)->second);
    }
    
    template <typename H>
    H&& get(const std::string& name, size_t nameIndex = 0) && {
        auto it = getImpl(name, nameIndex);
        return boost::any_cast<H&&>(std::move(it->second));
    }
    
    template <typename H>
    HeaderRef<const H&> getAt(size_t index) const & {
        auto entry = _headers.at(index);
        return HeaderRef<const H&>(entry.first, boost::any_cast<const H&>(entry.second));
    }
    
    template <typename H>
    HeaderRef<H&> getAt(size_t index) & {
        auto entry = _headers.at(index);
        return HeaderRef<H&>(entry.first, boost::any_cast<H&>(entry.second));
    }
    
    template <typename H>
    HeaderRef<H&&> getAt(size_t index) && {
        auto entry = _headers.at(index);
        return HeaderRef<H&&>(entry.first, boost::any_cast<H&&>(std::move(entry.second)));
    }
    
private:
    ListType::const_iterator getImpl(const std::string& name, size_t nameIndex) const {
        return const_cast<HeaderPack*>(this)->getImpl(name, nameIndex);
    }
    
    ListType::iterator getImpl(const std::string& name, size_t nameIndex) {
        if (nameIndex >= _headers.size()) {
            throw std::out_of_range("Invalid position");
        }
        size_t index = -1;
        return std::find_if(_headers.begin(), _headers.end(), [&](const auto& entry)->bool {
            if (StringEqualCompare()(entry.first, name)) {
                return (++index == nameIndex);
            }
            return false;
        });
    }
    
    // Members
    ListType    _headers;
};


}
}

#endif //BLOOMBERG_COROKAFKA_HEADER_MAP_H
