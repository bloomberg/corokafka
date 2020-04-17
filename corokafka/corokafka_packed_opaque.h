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
#ifndef BLOOMBERG_COROKAFKA_PACKED_OPAQUE_H
#define BLOOMBERG_COROKAFKA_PACKED_OPAQUE_H

#include <corokafka/corokafka_delivery_report.h>
#include <quantum/quantum.h>
#include <utility>
#include <future>

namespace Bloomberg {
namespace corokafka {

/**
 * @brief Represents the packed opaque data structure a pointer to
 * which is associated with every sent message
 */
struct PackedOpaque
{
    void* operator new(size_t);
    void operator delete(void* p);
    
    //members
    const void*                         _opaque{nullptr};
    quantum::Promise<DeliveryReport>    _deliveryReportPromise;
};
    
}}
    
#endif //BLOOMBERG_COROKAFKA_PACKED_OPAQUE_H
