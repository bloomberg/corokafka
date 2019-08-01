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
#ifndef BLOOMBERG_COROKAFKA_OFFSET_MAP_H
#define BLOOMBERG_COROKAFKA_OFFSET_MAP_H

#include <map>
#include <mutex>
#include <memory>
#include <corokafka/corokafka_utils.h>

namespace Bloomberg {
namespace corokafka {

class OffsetMap {
public:
    OffsetMap() :
        _mutex(new std::mutex())
    {}
    
    void insert(const TopicPartition& position, const void* opaque) {
        if (opaque == nullptr) return;
        std::lock_guard<std::mutex> lock(*_mutex);
        _map[position] = opaque;
    }
    
    void insert(TopicPartition&& position, const void* opaque) {
        if (opaque == nullptr) return;
        std::lock_guard<std::mutex> lock(*_mutex);
        _map[std::move(position)] = opaque;
    }
    
    const void* find(const TopicPartition& position) const {
        if (_map.empty()) return nullptr;
        std::lock_guard<std::mutex> lock(*_mutex);
        auto it = _map.find(position);
        if (it == _map.end()) {
            return nullptr;
        }
        return it->second;
    }

    const void* remove(const TopicPartition& position) {
        if (_map.empty()) return nullptr;
        std::lock_guard<std::mutex> lock(*_mutex);
        auto it = _map.find(position);
        if (it == _map.end()) {
            return nullptr;
        }
        const void* ptr = it->second;
        _map.erase(it);
        return ptr;
    }
    
    void clear_by_partition(int partition) {
        std::lock_guard<std::mutex> lock(*_mutex);
        for (auto it = _map.begin(); it != _map.end();) {
            if (it->first.get_partition() == partition) {
                it = _map.erase(it);
            }
            else {
                ++it;
            }
        }
    }
    
    void clear() {
        std::lock_guard<std::mutex> lock(*_mutex);
        _map.clear();
    }
    
    size_t size() const { return _map.size(); }
    
    bool empty() const { return _map.empty(); }
    
private:
    struct TopicPartitionComparator
    {
        bool operator()(const TopicPartition& lhs, const TopicPartition& rhs) const {
            int lhsPartition = lhs.get_partition(), rhsPartition = rhs.get_partition();
            int64_t lhsOffset = lhs.get_offset(), rhsOffset = rhs.get_offset();
            return std::tie(lhs.get_topic(), lhsPartition, lhsOffset) <
                   std::tie(rhs.get_topic(), rhsPartition, rhsOffset);
        }
    };
    // Indexed by partition and offset number
    using MapType = std::map<TopicPartition, const void*, TopicPartitionComparator>;
    
    MapType                             _map;
    mutable std::unique_ptr<std::mutex> _mutex;
};
using OffsetMapPtr = std::unique_ptr<OffsetMap>;

}
}

#endif //BLOOMBERG_COROKAFKA_OFFSET_MAP_H
