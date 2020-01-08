#ifndef BLOOMBERG_COROKAFKA_TESTS_UTILS_H
#define BLOOMBERG_COROKAFKA_TESTS_UTILS_H

#include <corokafka_tests_topics.h>
#include <corokafka_tests_callbacks.h>
#include <utility>
#include <vector>

namespace Bloomberg {
namespace corokafka {
namespace tests {

using ValueTestList = std::vector<std::pair<std::string,bool>>;

template <typename EX>
void testConnectorOption(const char* exName, const char* opName, const ValueTestList& values)
{
    for (auto&& value : values) {
        if (value.second) { //throws
            ASSERT_THROW(ConnectorConfiguration({{opName, value.first}}), EX);
            try { ConnectorConfiguration connector({{opName, value.first}}); }
            catch (const EX& ex) {
                ASSERT_STREQ(exName, ex.name());
                std::string op = opName;
                trim(op);
                ASSERT_TRUE(StringEqualCompare()(op, ex.option()));
            }
        }
        else {
            ASSERT_NO_THROW(ConnectorConfiguration({{opName, value.first}}));
        }
    }
}

template <typename EX, typename CONFIG>
void testOption(CONFIG&& config, const char* exName, const char* opName, const std::pair<std::string,bool>& value)
{
    ConfigurationBuilder builder; builder(config);
    if (value.second) { //throws
        ASSERT_THROW(Connector connector(builder), EX);
        try { Connector connector(builder); }
        catch (const EX& ex) {
            ASSERT_STREQ(exName, ex.name());
            std::string op = opName;
            trim(op);
            ASSERT_TRUE(StringEqualCompare()(op, ex.option()));
        }
    }
    else {
        ASSERT_NO_THROW(Connector connector(builder));
    }
}

template <typename EX>
void testProducerOption(const char* exName, const char* opName, const ValueTestList& values)
{
    for (auto&& value : values) {
        ProducerConfiguration config(topicWithHeaders().topic(),
            {{opName, value.first},
             {"metadata.broker.list", programOptions()._broker}}, {});
        testOption<EX>(config, exName, opName, value);
    }
}

template <typename EX>
void testConsumerOption(const char* exName, const char* opName, const ValueTestList& values)
{
    for (auto&& value : values) {
        ConsumerConfiguration config(topicWithHeaders(),
            {{opName, value.first},
             {"metadata.broker.list", programOptions()._broker},
             {"group.id","test-group"},
             {"internal.consumer.pause.on.start", true},
             {"internal.consumer.read.size",1}}, {}, Callbacks::messageReceiverWithHeaders);
        testOption<EX>(config, exName, opName, value);
    }
}


}}}

#endif //BLOOMBERG_COROKAFKA_TESTS_UTILS_H
