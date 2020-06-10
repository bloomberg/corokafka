#ifndef BLOOMBERG_COROKAFKA_IPRODUCER_MANAGER_H
#define BLOOMBERG_COROKAFKA_IPRODUCER_MANAGER_H

#include <corokafka/corokafka_producer_configuration.h>
#include <corokafka/corokafka_producer_metadata.h>
#include <corokafka/corokafka_delivery_report.h>
#include <quantum/util/quantum_generic_future.h>
#include <string>
#include <chrono>
#include <vector>

namespace Bloomberg {
namespace corokafka {

struct IProducerManager
{
    virtual ~IProducerManager() = default;
    virtual DeliveryReport send() = 0;
    virtual quantum::GenericFuture<DeliveryReport> post() = 0;
    virtual bool waitForAcks(const std::string& topic) = 0;
    virtual bool waitForAcks(const std::string& topic,
                             std::chrono::milliseconds timeout) = 0;
    virtual void shutdown() = 0;
    virtual ProducerMetadata getMetadata(const std::string& topic) = 0;
    virtual const ProducerConfiguration& getConfiguration(const std::string& topic) const = 0;
    virtual std::vector<std::string> getTopics() const = 0;
    virtual void resetQueueFullTrigger(const std::string& topic) = 0;
    virtual void poll() = 0;
    virtual void pollEnd() = 0;
};

}
}

#endif //BLOOMBERG_COROKAFKA_IPRODUCER_MANAGER_H
