#include <corokafka/corokafka.h>
#include <gtest/gtest.h>
#include <corokafka_tests_utils.h>
#include <limits>

namespace Bloomberg {
namespace corokafka {
namespace tests {

TEST(ConsumerConfiguration, MissingBrokerList)
{
    ConsumerConfiguration config(topicWithHeaders().topic(), {}, {}); //use all defaults
    ConfigurationBuilder builder;
    builder(config);
    ASSERT_THROW(Connector connector(builder), InvalidOptionException);
    try { Connector connector(builder); }
    catch(const InvalidOptionException& ex) {
        ASSERT_STREQ("metadata.broker.list", ex.option());
    }
}

TEST(ConsumerConfiguration, MissingGroupId)
{
    ConsumerConfiguration config(topicWithHeaders().topic(), {{"metadata.broker.list", programOptions()._broker}}, {}); //use all defaults
    ConfigurationBuilder builder;
    builder(config);
    ASSERT_THROW(Connector connector(builder), InvalidOptionException);
    try { Connector connector(builder); }
    catch(const InvalidOptionException& ex) {
        ASSERT_STREQ("group.id", ex.option());
    }
}

TEST(ConsumerConfiguration, UnknownOption)
{
    ConsumerConfiguration config(topicWithHeaders(),
        {{"metadata.broker.list", programOptions()._broker},
         {"group.id", "test-group"},
         {"somebadoption", "bad"}}, {},
         Callbacks::messageReceiverWithHeaders);
    ConfigurationBuilder builder;
    builder(config);
    ASSERT_THROW(Connector connector(builder), InvalidOptionException);
}

TEST(ConsumerConfiguration, UnknownInternalOption)
{
    ASSERT_THROW(ConsumerConfiguration config(topicWithHeaders().topic(),
        {{"metadata.broker.list", programOptions()._broker},
         {"internal.consumer.unknown.option", "bad"}}, {}), InvalidOptionException);
}

TEST(ConsumerConfiguration, InternalConsumerPauseOnStart)
{
    testConsumerOption<InvalidOptionException>("InvalidOptionException", "internal.consumer.pause.on.start",
        {{"bad",true},{"true",false},{"false",false}});
}

TEST(ConsumerConfiguration, InternalConsumerTimeoutMs)
{
    testConsumerOption<InvalidOptionException>("InvalidOptionException", "internal.consumer.timeout.ms",
        {{"-2",true},{"1000",false}});
}

TEST(ConsumerConfiguration, InternalConsumerPollTimeoutMs)
{
    testConsumerOption<InvalidOptionException>("InvalidOptionException", "internal.consumer.poll.timeout.ms",
        {{"-2",true},{"1000",false}});
}

TEST(ConsumerConfiguration, InternalConsumerAutoOffsetPersist)
{
    testConsumerOption<InvalidOptionException>("InvalidOptionException", "internal.consumer.auto.offset.persist",
        {{"bad",true},{"true",false},{"false",false}});
}

TEST(ConsumerConfiguration, InternalConsumerAutoOffsetPersistOnException)
{
    testConsumerOption<InvalidOptionException>("InvalidOptionException", "internal.consumer.auto.offset.persist.on.exception",
        {{"bad",true},{"true",false},{"false",false}});
}

TEST(ConsumerConfiguration, InternalConsumerOffsetPersistStrategy)
{
    testConsumerOption<InvalidOptionException>("InvalidOptionException", "internal.consumer.offset.persist.strategy",
        {{"bad",true},{"commit",false},{"store",false}});
}

TEST(ConsumerConfiguration, InternalConsumerCommitExec)
{
    testConsumerOption<InvalidOptionException>("InvalidOptionException", "internal.consumer.commit.exec",
        {{"bad",true},{"sync",false},{"async",false}});
}

TEST(ConsumerConfiguration, InternalConsumerCommitNumRetries)
{
    testConsumerOption<InvalidOptionException>("InvalidOptionException", "internal.consumer.commit.num.retries",
        {{"-1",true},{"0",false},{"1",false}});
}

TEST(ConsumerConfiguration, InternalConsumerCommitBackoffStrategy)
{
    testConsumerOption<InvalidOptionException>("InvalidOptionException", "internal.consumer.commit.backoff.strategy",
        {{"bad",true},{"linear",false},{"exponential",false}});
}

TEST(ConsumerConfiguration, InternalConsumerCommitBackoffIntervalMs)
{
    testConsumerOption<InvalidOptionException>("InvalidOptionException", "internal.consumer.commit.backoff.interval.ms",
        {{"0",true},{"1",false},{"2",false}});
}

TEST(ConsumerConfiguration, InternalConsumerCommitMaxBackoffMs)
{
    testConsumerOption<InvalidOptionException>("InvalidOptionException", "internal.consumer.commit.max.backoff.ms",
        {{"49",true},{"50",false},{"51",false}});
}

TEST(ConsumerConfiguration, InternalConsumerPollStrategy)
{
    testConsumerOption<InvalidOptionException>("InvalidOptionException", "internal.consumer.poll.strategy",
        {{"bad",true},{"batch",false},{"roundRobin",false}});
}

TEST(ConsumerConfiguration, InternalConsumerReadSize)
{
    testConsumerOption<InvalidOptionException>("InvalidOptionException", "internal.consumer.read.size",
        {{"0",true},{"1",false},{"2",false}});
}

TEST(ConsumerConfiguration, InternalConsumerBatchPrefetch)
{
    testConsumerOption<InvalidOptionException>("InvalidOptionException", "internal.consumer.batch.prefetch",
        {{"bad",true},{"true",false},{"false",false}});
}

TEST(ConsumerConfiguration, InternalConsumerReceiveCallbackThreadRangeLow)
{
    testConsumerOption<InvalidOptionException>("InvalidOptionException", "internal.consumer.receive.callback.thread.range.low",
        {{"-1",true},{"0",false},{"1",false},{"5",true}});
}

TEST(ConsumerConfiguration, InternalConsumerReceiveCallbackThreadRangeHigh)
{
    testConsumerOption<InvalidOptionException>("InvalidOptionException", "internal.consumer.receive.callback.thread.range.high",
        {{"-1",true},{"0",false},{"1",false},{"5",true}});
}

TEST(ConsumerConfiguration, InternalConsumerReceiveCallbackExec)
{
    testConsumerOption<InvalidOptionException>("InvalidOptionException", "internal.consumer.receive.callback.exec",
        {{"bad",true},{"sync",false},{"async",false}});
}

TEST(ConsumerConfiguration, InternalConsumerReceiveInvokeThread)
{
    testConsumerOption<InvalidOptionException>("InvalidOptionException", "internal.consumer.receive.invoke.thread",
        {{"bad",true},{"io",false},{"coro",false}});
}

TEST(ConsumerConfiguration, InternalConsumerLogLevel)
{
    testConsumerOption<InvalidOptionException>("InvalidOptionException", "internal.consumer.log.level",
        {{"bad",true},{"emergency",false},{"CRITICAL",false},{" error ",false},{"warning",false},{"notice",false},
         {"info",false},{"debug",false}});
}

TEST(ConsumerConfiguration, InternalConsumerSkipUnknownHeaders)
{
    testConsumerOption<InvalidOptionException>("InvalidOptionException", "internal.consumer.skip.unknown.headers",
        {{"bad",true},{"true",false},{"false",false}});
}

TEST(ConsumerConfiguration, InternalConsumerAutoThrottle)
{
    testConsumerOption<InvalidOptionException>("InvalidOptionException", "internal.consumer.auto.throttle",
        {{"bad",true},{"true",false},{"false",false}});
}

TEST(ConsumerConfiguration, InternalConsumerAutoThrottleMultiplier)
{
    testConsumerOption<InvalidOptionException>("InvalidOptionException", "internal.consumer.auto.throttle.multiplier",
        {{"0",true},{"1",false},{"2",false}});
}

TEST(ConsumerConfiguration, InternalConsumerPreprocessMessages)
{
    testConsumerOption<InvalidOptionException>("InvalidOptionException", "internal.consumer.preprocess.messages",
        {{"bad",true},{"true",false},{"false",false}});
}

TEST(ConsumerConfiguration, InternalConsumerPreprocessInvokeThread)
{
    testConsumerOption<InvalidOptionException>("InvalidOptionException", "internal.consumer.preprocess.invoke.thread",
        {{"bad",true},{"io",false},{"coro",false}});
}

}
}
}

