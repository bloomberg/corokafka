#include <corokafka/corokafka.h>
#include <gtest/gtest.h>
#include <corokafka_tests_utils.h>

namespace Bloomberg {
namespace corokafka {
namespace tests {

TEST(ProducerConfiguration, MissingBrokerList)
{
    ProducerConfiguration config(topicWithHeaders().topic(), {}, {}); //use all defaults
    ConfigurationBuilder builder;
    builder(config);
    ASSERT_THROW(Connector connector(builder), InvalidOptionException);
    try { Connector connector(builder); }
    catch(const InvalidOptionException& ex) {
        ASSERT_STREQ("metadata.broker.list", ex.option());
    }
}

TEST(ProducerConfiguration, UnknownOption)
{
    ProducerConfiguration config(topicWithHeaders().topic(),
        {{"metadata.broker.list", programOptions()._broker},
         {"somebadoption", "bad"}}, {});
    ConfigurationBuilder builder;
    builder(config);
    ASSERT_THROW(Connector connector(builder), InvalidOptionException);
}

TEST(ProducerConfiguration, UnknownInternalOption)
{
    ASSERT_THROW(ProducerConfiguration config(topicWithHeaders().topic(),
        {{"metadata.broker.list", programOptions()._broker},
         {"internal.producer.unknown.option", "bad"}}, {}), InvalidOptionException);
}

TEST(ProducerConfiguration, InternalProducerTimeoutMs)
{
    //add spaces and upper case
    testProducerOption<InvalidOptionException>("InvalidOptionException", " internal.producer.TIMEOUT.ms ",
        {{"bad",true},{"-2",true},{"-1",false},{"0",false}});
}

TEST(ProducerConfiguration, InternalProducerRetries)
{
    testProducerOption<InvalidOptionException>("InvalidOptionException", "internal.producer.retries",
        {{"bad",true},{"-1",true},{"0",false},{" 1 ",false}});
}

TEST(ProducerConfiguration, InternalProducerPayloadPolicy)
{
    testProducerOption<InvalidOptionException>("InvalidOptionException", "internal.producer.payload.policy",
        {{"bad",true},{"passthrough",false},{"copy",false},{" COPY ",false}});
}

TEST(ProducerConfiguration, InternalProducerPreserveMessageOrder)
{
    testProducerOption<InvalidOptionException>("InvalidOptionException", "internal.producer.preserve.message.order",
        {{"bad",true},{" true ",false},{"false",false},{"FALSE",false}});
}

TEST(ProducerConfiguration, InternalProducerMaxQueueLength)
{
    testProducerOption<InvalidOptionException>("InvalidOptionException", "internal.producer.max.queue.length",
        {{"bad",true},{"0",true},{"1",false},{"2",false}});
}

TEST(ProducerConfiguration, InternalProducerWaitForAcksTimeoutMs)
{
    testProducerOption<InvalidOptionException>("InvalidOptionException", "internal.producer.wait.for.acks.timeout.ms",
        {{"bad",true},{"-2",true},{"-1",false},{"0",false}});
}

TEST(ProducerConfiguration, InternalProducerFlushWaitForAcksTimeoutMs)
{
    testProducerOption<InvalidOptionException>("InvalidOptionException", "internal.producer.flush.wait.for.acks.timeout.ms",
        {{"bad",true},{"-2",true},{"-1",false},{"0",false}});
}

TEST(ProducerConfiguration, InternalProducerLogLevel)
{
    testProducerOption<InvalidOptionException>("InvalidOptionException", "internal.producer.log.level",
        {{"bad",true},{"emergency",false},{"CRITICAL",false},{" error ",false},{"warning",false},{"notice",false},
         {"info",false},{"debug",false}});
}

TEST(ProducerConfiguration, InternalProducerAutoThrottle)
{
    testProducerOption<InvalidOptionException>("InvalidOptionException", "internal.producer.auto.throttle",
        {{"bad",true},{"true",false},{"false",false}});
}

TEST(ProducerConfiguration, InternalProducerAutoThrottleMultiplier)
{
    testProducerOption<InvalidOptionException>("InvalidOptionException", "internal.producer.auto.throttle.multiplier",
        {{"bad",true},{"0",true},{"1",false},{"2",false}});
}

TEST(ProducerConfiguration, InternalProducerQueueFullNotification)
{
    testProducerOption<InvalidOptionException>("InvalidOptionException", "internal.producer.queue.full.notification",
        {{"bad",true},{"edgeTriggered",false},{"oncePerMessage",false},{" eachOccurence ",false}});
}

}
}
}

