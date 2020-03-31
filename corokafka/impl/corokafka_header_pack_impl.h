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
#ifndef BLOOMBERG_COROKAFKA_HEADER_PACK_IMPL_H
#define BLOOMBERG_COROKAFKA_HEADER_PACK_IMPL_H

namespace Bloomberg {
namespace corokafka {

template <typename H>
HeaderPack& HeaderPack::push_front(const std::string& name, H&& header) {
    return push_front(name, boost::any(std::forward<H>(header)));
}

template <typename H>
HeaderPack& HeaderPack::push_back(const std::string& name, H&& header) {
    return push_back(name, boost::any(std::forward<H>(header)));
}

template <typename H>
const H& HeaderPack::get(const std::string& name, size_t relativePosition) const & {
    return boost::any_cast<const H&>(getImpl(name, relativePosition)->second);
}

template <typename H>
H& HeaderPack::get(const std::string& name, size_t relativePosition) & {
    return boost::any_cast<H&>(getImpl(name, relativePosition)->second);
}

template <typename H>
H&& HeaderPack::get(const std::string& name, size_t relativePosition) && {
    return boost::any_cast<H&&>(std::move(getImpl(name, relativePosition)->second));
}

template <typename H>
HeaderRef<const H&> HeaderPack::getAt(size_t index) const & {
    const auto& entry = _headers.at(index);
    return HeaderRef<const H&>(entry.first, boost::any_cast<const H&>(entry.second));
}

template <typename H>
HeaderRef<H&> HeaderPack::getAt(size_t index) & {
    auto& entry = _headers.at(index);
    return HeaderRef<H&>(entry.first, boost::any_cast<H&>(entry.second));
}

template <typename H>
HeaderRef<H&&> HeaderPack::getAt(size_t index) && {
    auto& entry = _headers.at(index);
    return HeaderRef<H&&>(entry.first, boost::any_cast<H&&>(std::move(entry.second)));
}

}
}

#endif //BLOOMBERG_COROKAFKA_HEADER_PACK_IMPL_H
