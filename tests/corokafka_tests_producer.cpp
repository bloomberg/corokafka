#include <corokafka/corokafka.h>
#include <gtest/gtest.h>
#include <corokafka_tests_topics.h>

namespace Bloomberg {
namespace corokafka {
namespace tests {

void makeConnectorWithMissingBroker() {
    ck::ProducerConfiguration config(topic1Str, {}, {}); //use all defaults
    ck::ConfigurationBuilder builder;
    builder(config);
    ck::Connector connector(std::move(builder));
}

TEST(Producer, MissingBrokerList)
{
    ck::ProducerConfiguration config(topic1Str, {}, {}); //use all defaults
    ck::ConfigurationBuilder builder;
    builder(config);
    ASSERT(ck::Connector connector(std::move(builder));
}

}
}
}

