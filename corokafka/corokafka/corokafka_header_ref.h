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
#ifndef BLOOMBERG_COROKAFKA_HEADER_REF_H
#define BLOOMBERG_COROKAFKA_HEADER_REF_H

namespace Bloomberg {
namespace corokafka {

/**
 * @brief The HeaderRef wraps a header reference allowing access to the name of the header as well.
 *        In the case where the header is only accessed by index, the name is not accessible unless
 *        it's also wrapped here.
 * @tparam HEADER The header type.
 */
template <typename HEADER>
class HeaderRef
{
    friend class HeaderPack;
public:
    /**
     * @brief Get the name of the header
     * @return The name
     */
    const std::string &name() const { return _name; }
    
    /**
     * @brief Get the header reference
     * @return The reference
     */
    HEADER value() const { return std::forward<HEADER>(_value); }

private:
    HeaderRef(const std::string &name,
              HEADER value) :
    _name(name),
    _value(std::forward<HEADER>(value))
    {}
    
    const std::string& _name;
    HEADER _value;
};

}}

#endif //BLOOMBERG_COROKAFKA_HEADER_REF_H
