/*
** Copyright 2020 Bloomberg Finance L.P.
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
#ifndef BLOOMBERG_COROKAFKA_ICONNECTOR_H
#define BLOOMBERG_COROKAFKA_ICONNECTOR_H

#include <corokafka/corokafka_consumer_manager.h>
#include <corokafka/corokafka_producer_manager.h>
#include <chrono>

namespace Bloomberg {
namespace corokafka {

struct IConnector
{
    virtual ~IConnector() = default;
    virtual ConsumerManager& consumer() = 0;
    virtual ProducerManager& producer() = 0;
    virtual void shutdown() = 0;
    virtual void shutdown(std::chrono::milliseconds drainTimeout) = 0;
};

}
}
#endif //BLOOMBERG_COROKAFKA_ICONNECTOR_H
