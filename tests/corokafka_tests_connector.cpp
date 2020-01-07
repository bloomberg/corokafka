#ifndef BLOOMBERG_COROKAFKA_TESTS_CONNECTOR_H
#define BLOOMBERG_COROKAFKA_TESTS_CONNECTOR_H

#include <gtest/gtest.h>
#include <corokafka/corokafka.h>
#include <corokafka_tests_utils.h>

namespace Bloomberg {
namespace corokafka {
namespace tests {

TEST(ConnectorConfiguration, InternalConnectorPollIntervalMs)
{
    testConnectorOption<InvalidOptionException>("InvalidOptionException", ConnectorConfiguration::Options::pollIntervalMs,
        {{"0", true},{"1", false},{"2", false}});
}

TEST(ConnectorConfiguration, InternalConnectorMaxPayloadOutputLength)
{
    testConnectorOption<InvalidOptionException>("InvalidOptionException", ConnectorConfiguration::Options::maxPayloadOutputLength,
        {{"-2", true},{"-1", false},{"0", false}});
}

}}}

#endif //BLOOMBERG_COROKAFKA_TESTS_CONNECTOR_H
