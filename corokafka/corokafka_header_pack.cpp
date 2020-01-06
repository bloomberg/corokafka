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
#include <corokafka/corokafka_header_pack.h>
#include <corokafka/corokafka_exception.h>

namespace Bloomberg {
namespace corokafka {

HeaderPack::HeaderPack(size_t numElements) :
    _headers(numElements)
{}

HeaderPack::HeaderPack(std::initializer_list<HeaderNode> list) :
    _headers(list)
{

}

HeaderPack& HeaderPack::push_front(const std::string& name, boost::any&& header) {
    if (name.empty()) {
        throw InvalidArgumentException(0, "Header name cannot be empty");
    }
    _headers.emplace_front(name, std::move(header));
    return *this;
}

HeaderPack& HeaderPack::push_back(const std::string& name, boost::any&& header) {
    if (name.empty()) {
        throw InvalidArgumentException(0, "Header name cannot be empty");
    }
    _headers.emplace_back(name, std::move(header));
    return *this;
}

HeaderPack& HeaderPack::pop_front() {
    _headers.pop_front();
    return *this;
}

HeaderPack& HeaderPack::pop_back() {
    _headers.pop_back();
    return *this;
}

void HeaderPack::erase(const std::string& name) {
    _headers.erase(std::remove_if(_headers.begin(), _headers.end(), [&name](const HeaderNode& entry)->bool {
        return StringEqualCompare()(entry.first, name);
    }), _headers.end());
}

size_t HeaderPack::size() const {
    return _headers.size();
}

bool HeaderPack::empty() const {
    return _headers.size() == 0;
}

HeaderPack::operator bool() const {
    return !empty();
}

// Iterator access to underlying container
HeaderPack::ListType::iterator HeaderPack::begin() {
    return _headers.begin();
}

HeaderPack::ListType::const_iterator HeaderPack::cbegin() const {
    return _headers.cbegin();
}

HeaderPack::ListType::iterator HeaderPack::end() {
    return _headers.end();
}

HeaderPack::ListType::const_iterator HeaderPack::cend() const {
    return _headers.cend();
}

std::vector<std::string> HeaderPack::getHeaderNames() const {
    std::vector<std::string> names;
    names.reserve(size());
    for (const auto& header : _headers) {
        names.emplace_back(header.first);
    }
    return names;
}

HeaderPack::ListType::const_iterator HeaderPack::getImpl(const std::string& name, int nameIndex) const {
    return const_cast<HeaderPack*>(this)->getImpl(name, nameIndex);
}

HeaderPack::ListType::iterator HeaderPack::getImpl(const std::string& name, int nameIndex) {
    if (nameIndex == -1) {
        return std::find_if(_headers.begin(), _headers.end(), [&](const HeaderNode &header) -> bool {
            return StringEqualCompare()(header.first, name);
        });
    }
    else {
        auto it = _headers.begin() + nameIndex;
        if (!StringEqualCompare()(it->first, name)) {
            throw InvalidArgumentException(0, "Header name mismatch. Name found is:" + it->first);
        }
        return it;
    }
}

HeaderPack::HeaderNode& HeaderPack::operator[](size_t index)
{
    return _headers[index];
}

size_t HeaderPack::numValidHeaders() const {
    size_t num = 0;
    for (const auto& h : _headers) {
        if (!h.second.empty()) ++num;
    }
    return num;
}

bool HeaderPack::isValidAt(int index) const
{
    const auto& entry = _headers.at(index);
    return !entry.second.empty();
}

bool HeaderPack::isValid(const std::string& name, int nameIndex) const
{
    return !getImpl(name, nameIndex)->second.empty();
}

}
}
