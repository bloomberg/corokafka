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
#include <corokafka/corokafka_packed_opaque.h>
#include <quantum/quantum.h>

#define __COROKAFKA_PACKEDOPAQUE_DEFAULT_ALLOC_SIZE 1000
#ifndef __COROKAFKA_PACKEDOPAQUE_ALLOC_SIZE
#define __COROKAFKA_PACKEDOPAQUE_ALLOC_SIZE __COROKAFKA_PACKEDOPAQUE_DEFAULT_ALLOC_SIZE
#endif

namespace Bloomberg {
namespace corokafka {

//===========================================================================================
//                               class PackedOpaque
//===========================================================================================

using PackedOpaqueAllocator = Bloomberg::quantum::StackAllocator<PackedOpaque, __COROKAFKA_PACKEDOPAQUE_ALLOC_SIZE>;
  
PackedOpaque::PackedOpaque(void* opaque, std::promise<DeliveryReport>&& promise) :
    std::pair<void*, std::promise<DeliveryReport>>(opaque, std::move(promise))
{
}

void* PackedOpaque::operator new(size_t)
{
    return Bloomberg::quantum::Allocator<PackedOpaqueAllocator>::instance(__COROKAFKA_PACKEDOPAQUE_ALLOC_SIZE).allocate();
}
  
void PackedOpaque::operator delete(void* p)
{
    Bloomberg::quantum::Allocator<PackedOpaqueAllocator>::instance(__COROKAFKA_PACKEDOPAQUE_ALLOC_SIZE).deallocate(reinterpret_cast<PackedOpaque*>(p));
} 

}
}
