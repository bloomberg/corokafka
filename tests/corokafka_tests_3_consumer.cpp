#include <corokafka/corokafka.h>
#include <gtest/gtest.h>
#include <corokafka_tests_utils.h>

namespace Bloomberg {
namespace corokafka {
namespace tests {

const int maxLoops = 10; //arbitrary wait value

std::string getNewGroupName()
{
    static int i = 0;
    return "group_" + std::to_string(i++);
}

//paused on start
Configuration::OptionList config1 = {
    {"enable.partition.eof", true},
    {"enable.auto.offset.store", false},
    {"enable.auto.commit", true}, //automatically commit stored offset every 100ms
    {"auto.offset.reset","beginning"},
    {"auto.commit.interval.ms", 10},
    {ConsumerConfiguration::Options::timeoutMs, 10},
    {ConsumerConfiguration::Options::pauseOnStart, true},
    {ConsumerConfiguration::Options::readSize, 100},
    {ConsumerConfiguration::Options::pollStrategy, "batch"},
    {ConsumerConfiguration::Options::offsetPersistStrategy, "store"},
    {ConsumerConfiguration::Options::commitExec, "sync"},
    {ConsumerConfiguration::Options::autoOffsetPersist, "true"},
    {ConsumerConfiguration::Options::receiveInvokeThread, "coro"},
    {ConsumerConfiguration::Options::preprocessMessages, "false"},
    {ConsumerConfiguration::Options::receiveCallbackThreadRangeLow, 1},
    {ConsumerConfiguration::Options::receiveCallbackThreadRangeHigh, 1},
    {ConsumerConfiguration::Options::preserveMessageOrder, true},
};

Configuration::OptionList config2 = {
    {"enable.partition.eof", true},
    {"enable.auto.offset.store", false},
    {"enable.auto.commit", false}, //do not auto-commit. The application will do it.
    {"auto.offset.reset","beginning"},
    {"auto.commit.interval.ms", 10},
    {ConsumerConfiguration::Options::pauseOnStart, false},
    {ConsumerConfiguration::Options::readSize, 100},
    {ConsumerConfiguration::Options::pollStrategy, "roundrobin"},
    {ConsumerConfiguration::Options::offsetPersistStrategy, "commit"},
    {ConsumerConfiguration::Options::commitExec, "async"},
    {ConsumerConfiguration::Options::autoOffsetPersist, "false"},
    {ConsumerConfiguration::Options::receiveInvokeThread, "coro"},
    {ConsumerConfiguration::Options::preprocessMessages, "true"},
    {ConsumerConfiguration::Options::preserveMessageOrder, true},
};

//same as group 2 but pre-processing done on coroutine thread & round-robin
Configuration::OptionList config3 = {
    {"enable.partition.eof", true},
    {"enable.auto.offset.store", false},
    {"enable.auto.commit", false}, //do not auto-commit. The application will do it.
    {"auto.offset.reset","beginning"},
    {"auto.commit.interval.ms", 10},
    {ConsumerConfiguration::Options::pauseOnStart, false},
    {ConsumerConfiguration::Options::readSize, 100},
    {ConsumerConfiguration::Options::pollStrategy, "roundrobin"},
    {ConsumerConfiguration::Options::offsetPersistStrategy, "commit"},
    {ConsumerConfiguration::Options::commitExec, "async"},
    {ConsumerConfiguration::Options::autoOffsetPersist, "false"},
    {ConsumerConfiguration::Options::receiveInvokeThread, "io"},
    {ConsumerConfiguration::Options::preprocessMessages, "true"},
    {ConsumerConfiguration::Options::preserveMessageOrder, true},
};

//same as config 2 but poll strategy is serial
Configuration::OptionList config4 = {
    {"enable.partition.eof", true},
    {"enable.auto.offset.store", false},
    {"enable.auto.commit", false}, //do not auto-commit. The application will do it.
    {"auto.offset.reset","beginning"},
    {"auto.commit.interval.ms", 10},
    {ConsumerConfiguration::Options::pauseOnStart, true},
    {ConsumerConfiguration::Options::readSize, 100},
    {ConsumerConfiguration::Options::pollStrategy, "serial"},
    {ConsumerConfiguration::Options::offsetPersistStrategy, "commit"},
    {ConsumerConfiguration::Options::commitExec, "async"},
    {ConsumerConfiguration::Options::autoOffsetPersist, "false"},
    {ConsumerConfiguration::Options::receiveInvokeThread, "coro"},
    {ConsumerConfiguration::Options::preprocessMessages, "true"},
    {ConsumerConfiguration::Options::preserveMessageOrder, true},
};

TEST(ConsumerConfiguration, MissingBrokerList)
{
    ConsumerConfiguration config(topicWithHeaders().topic(), {}, {}); //use all defaults
    ConfigurationBuilder builder;
    builder(config);
    ASSERT_THROW(Connector connector(builder, dispatcher()), InvalidOptionException);
    try { Connector connector(builder, dispatcher()); }
    catch(const InvalidOptionException& ex) {
        ASSERT_STREQ("metadata.broker.list", ex.option());
    }
}

TEST(ConsumerConfiguration, MissingGroupId)
{
    ConsumerConfiguration config(topicWithHeaders().topic(), {{"metadata.broker.list", programOptions()._broker}}, {}); //use all defaults
    ConfigurationBuilder builder;
    builder(config);
    ASSERT_THROW(Connector connector(builder, dispatcher()), InvalidOptionException);
    try { Connector connector(builder, dispatcher()); }
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
    ASSERT_THROW(Connector connector(builder, dispatcher()), InvalidOptionException);
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

TEST(ConsumerConfiguration, InternalConsumerRoundRobinMinPollTimeoutMs)
{
    testConsumerOption<InvalidOptionException>("InvalidOptionException", "internal.consumer.min.roundrobin.poll.timeout.ms",
        {{"0",true},{"10",false}});
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
    //Negative test
    testConsumerOption<InvalidOptionException>("InvalidOptionException", "internal.consumer.commit.max.backoff.ms",
        {{"100",false},{"101",false}});
    
    //Positive (< backoff.interval)
    ConsumerConfiguration config(topicWithHeaders(),
                    {{"internal.consumer.commit.backoff.interval.ms", "50"},
                     {"internal.consumer.commit.max.backoff.ms", "49"},
                     {"metadata.broker.list", programOptions()._broker},
                     {"group.id","test-group"},
                     {ConsumerConfiguration::Options::pauseOnStart, true},
                     {ConsumerConfiguration::Options::readSize, 1}}, {}, Callbacks::messageReceiverWithHeaders);
    testConnectorOption<InvalidOptionException>(config, "InvalidOptionException", "internal.consumer.commit.max.backoff.ms", true);
}

TEST(ConsumerConfiguration, InternalConsumerPollStrategy)
{
    testConsumerOption<InvalidOptionException>("InvalidOptionException", "internal.consumer.poll.strategy",
        {{"bad",true},{"batch",false},{"roundRobin",false},{"serial", false}});
}

TEST(ConsumerConfiguration, InternalConsumerReadSize)
{
    testConsumerOption<InvalidOptionException>("InvalidOptionException", "internal.consumer.read.size",
        {{"-2",true},{"-1",false},{"1",false}});
}

TEST(ConsumerConfiguration, InternalConsumerBatchPrefetch)
{
    testConsumerOption<InvalidOptionException>("InvalidOptionException", "internal.consumer.batch.prefetch",
        {{"bad",true},{"true",false},{"false",false}});
}

TEST(ConsumerConfiguration, InternalConsumerReceiveCallbackThreadRangeLow)
{
    //Negative test
    testConsumerOption<InvalidOptionException>("InvalidOptionException", "internal.consumer.receive.callback.thread.range.low",
        {{"-1",true},{"0",false},{"1",false},{"4",false}});
    
    //Positive
    ConsumerConfiguration config(topicWithHeaders(),
                    {{"internal.consumer.receive.callback.thread.range.low", "5"},
                     {"internal.consumer.receive.callback.thread.range.high", "4"},
                     {"metadata.broker.list", programOptions()._broker},
                     {"group.id","test-group"},
                     {ConsumerConfiguration::Options::pauseOnStart, true},
                     {ConsumerConfiguration::Options::readSize, 1}}, {}, Callbacks::messageReceiverWithHeaders);
    testConnectorOption<InvalidOptionException>(config, "InvalidOptionException", "internal.consumer.receive.callback.thread.range.low", true);
}

TEST(ConsumerConfiguration, InternalConsumerReceiveCallbackThreadRangeHigh)
{
    //Negative test
    testConsumerOption<InvalidOptionException>("InvalidOptionException", "internal.consumer.receive.callback.thread.range.high",
        {{"-1",true},{"0",false},{"1",false},{"4",false}});
    
    //Positive
    ConsumerConfiguration config(topicWithHeaders(),
                    {{"internal.consumer.receive.callback.thread.range.low", "3"},
                     {"internal.consumer.receive.callback.thread.range.high", "2"},
                     {"metadata.broker.list", programOptions()._broker},
                     {"group.id","test-group"},
                     {ConsumerConfiguration::Options::pauseOnStart, true},
                     {ConsumerConfiguration::Options::readSize, 1}}, {}, Callbacks::messageReceiverWithHeaders);
    testConnectorOption<InvalidOptionException>(config, "InvalidOptionException", "internal.consumer.receive.callback.thread.range.high", true);
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

TEST(ConsumerConfiguration, InternalConsumerPreserveMessageOrder)
{
    testConsumerOption<InvalidOptionException>("InvalidOptionException", "internal.consumer.preserve.message.order",
        {{"bad",true},{" true ",false},{"false",false},{"FALSE",false}});
}

TEST(Consumer, ValidatePauseOnStart)
{
    callbackCounters().reset();
    Connector connector = makeConsumerConnector(
        config1,
        getNewGroupName(),
        topicWithoutHeaders(),
        Callbacks::messageReceiverWithoutHeaders,
        PartitionStrategy::Static,
        {{programOptions()._topicWithoutHeaders, 0, (int)OffsetPoint::AtEnd},
         {programOptions()._topicWithoutHeaders, 1, (int)OffsetPoint::AtEnd},
         {programOptions()._topicWithoutHeaders, 2, (int)OffsetPoint::AtEnd},
         {programOptions()._topicWithoutHeaders, 3, (int)OffsetPoint::AtEnd}});
    
    std::this_thread::sleep_for(std::chrono::seconds(5));
    
    //make sure the receiver callback nor the rebalance callbacks have been invoked
    EXPECT_EQ(0, callbackCounters()._receiver);
    EXPECT_EQ(1, callbackCounters()._assign);
    EXPECT_EQ(0, callbackCounters()._revoke);
    EXPECT_EQ(0, callbackCounters()._rebalance);
    EXPECT_EQ(0, callbackCounters()._preprocessor);
    
    //enable consuming
    connector.consumer().resume(topicWithoutHeaders().topic());
    
    int loops = maxLoops;
    while (callbackCounters()._eof < 4 && loops--) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    EXPECT_EQ(0, callbackCounters()._messageErrors);
    EXPECT_EQ(4, callbackCounters()._receiver);
    EXPECT_EQ(1, callbackCounters()._assign);
    EXPECT_EQ(0, callbackCounters()._revoke);
    EXPECT_EQ(0, callbackCounters()._rebalance);
    EXPECT_EQ(0, callbackCounters()._preprocessor);
    dispatcher().drain();
}

TEST(Consumer, ValidateDynamicAssignment)
{
    callbackCounters().reset();
    {
        Connector connector = makeConsumerConnector(
            config2,
            getNewGroupName(),
            topicWithoutHeaders(),
            Callbacks::messageReceiverWithoutHeaders,
            PartitionStrategy::Dynamic);
        int loops = maxLoops;
        while (callbackCounters()._assign == 0 && loops--) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        loops = maxLoops;
        while (callbackCounters()._eof < 4 && loops--) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        EXPECT_EQ(0, callbackCounters()._messageErrors);
        EXPECT_EQ(4, callbackCounters()._receiver);
        EXPECT_EQ(1, callbackCounters()._assign);
        EXPECT_EQ(0, callbackCounters()._rebalance);
        callbackCounters().reset();
    }
    int loops = maxLoops;
    while (callbackCounters()._revoke == 0 && loops--) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    EXPECT_EQ(1, callbackCounters()._revoke);
    dispatcher().drain();
}

TEST(Consumer, ReadTopicWithoutHeadersUsingConfig1)
{
    callbackCounters().reset();
    Connector connector = makeConsumerConnector(
        config1,
        getNewGroupName(),
        topicWithoutHeaders(),
        Callbacks::messageReceiverWithoutHeaders,
        PartitionStrategy::Static,
        {{programOptions()._topicWithoutHeaders, 0, (int)OffsetPoint::AtBeginning},
         {programOptions()._topicWithoutHeaders, 1, (int)OffsetPoint::AtBeginning},
         {programOptions()._topicWithoutHeaders, 2, (int)OffsetPoint::AtBeginning},
         {programOptions()._topicWithoutHeaders, 3, (int)OffsetPoint::AtBeginning}});
    connector.consumer().resume();
    int loops = maxLoops;
    while (callbackCounters()._eof < 4 && loops--) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    std::this_thread::sleep_for(std::chrono::seconds(1));

    EXPECT_LE(10, callbackCounters()._offsetCommit);
    EXPECT_FALSE(callbackCounters()._receiverIoThread);
    EXPECT_EQ(1, callbackCounters()._assign);
    EXPECT_EQ(0, callbackCounters()._revoke);
    EXPECT_EQ(0, callbackCounters()._rebalance);
    
    //Check message validity
    EXPECT_EQ(messageWithoutHeadersTracker(), consumerMessageWithoutHeadersTracker());
    
    //Check commits
    EXPECT_EQ(callbackCounters()._offsetCommitPartitions,
              consumerMessageWithoutHeadersTracker()._offsets);
    
    ConsumerMetadata meta = connector.consumer().getMetadata(topicWithoutHeaders().topic());
    Metadata::OffsetWatermarkList water1 = meta.getOffsetWatermarks();
    Metadata::OffsetWatermarkList water2 = meta.queryOffsetWatermarks();
    auto off1 = meta.getOffsetPositions();
    auto off2 = meta.queryCommittedOffsets();
    
    //clear everything
    consumerMessageWithoutHeadersTracker().clear();
    dispatcher().drain();
}

TEST(Consumer, ReadTopicWithHeadersUsingConfig2)
{
    callbackCounters().reset();
    Connector connector = makeConsumerConnector(
        config2,
        getNewGroupName(),
        topicWithHeaders(),
        Callbacks::messageReceiverWithHeadersManualCommit,
        PartitionStrategy::Static,
        {{programOptions()._topicWithHeaders, 0, (int)OffsetPoint::AtBeginning},
         {programOptions()._topicWithHeaders, 1, (int)OffsetPoint::AtBeginning},
         {programOptions()._topicWithHeaders, 2, (int)OffsetPoint::AtBeginning},
         {programOptions()._topicWithHeaders, 3, (int)OffsetPoint::AtBeginning}});
    int loops = maxLoops;
    while (callbackCounters()._eof < 4 && loops--) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    EXPECT_EQ(messageTracker().totalMessages(), callbackCounters()._preprocessor);
    EXPECT_EQ(messageTracker().totalMessages(), callbackCounters()._receiver-callbackCounters()._eof);
    EXPECT_FALSE(callbackCounters()._receiverIoThread);
    
    //Check message validity
    EXPECT_EQ(messageTracker(), consumerMessageTracker());
    
    //Check async commits
    loops = maxLoops;
    while ((size_t)callbackCounters()._offsetCommit < messageTracker().totalMessages() && loops--) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    EXPECT_EQ(messageTracker().totalMessages(), callbackCounters()._offsetCommit);
    EXPECT_EQ(callbackCounters()._offsetCommitPartitions, consumerMessageTracker()._offsets);
    
    //clear everything
    consumerMessageTracker().clear();
    dispatcher().drain();
}

TEST(Consumer, ReadTopicWithHeadersUsingConfig3)
{
    callbackCounters().reset();
    Connector connector = makeConsumerConnector(
        config3,
        getNewGroupName(),
        topicWithHeaders(),
        Callbacks::messageReceiverWithHeadersManualCommit,
        PartitionStrategy::Static,
        {{programOptions()._topicWithHeaders, 0, (int)OffsetPoint::AtBeginning},
         {programOptions()._topicWithHeaders, 1, (int)OffsetPoint::AtBeginning},
         {programOptions()._topicWithHeaders, 2, (int)OffsetPoint::AtBeginning},
         {programOptions()._topicWithHeaders, 3, (int)OffsetPoint::AtBeginning}});
    int loops = maxLoops;
    while (callbackCounters()._eof < 4 && loops--) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    EXPECT_EQ(messageTracker().totalMessages(), callbackCounters()._preprocessor);
    EXPECT_EQ(messageTracker().totalMessages(), callbackCounters()._receiver-callbackCounters()._eof);
    EXPECT_TRUE(callbackCounters()._receiverIoThread);
    
    //Check message validity
    EXPECT_EQ(messageTracker(), consumerMessageTracker());
    
    //Check async commits
    loops = maxLoops;
    while ((size_t)callbackCounters()._offsetCommit < messageTracker().totalMessages() && loops--) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    EXPECT_EQ(messageTracker().totalMessages(), callbackCounters()._offsetCommit);
    EXPECT_EQ(callbackCounters()._offsetCommitPartitions, consumerMessageTracker()._offsets);
    
    //clear everything
    consumerMessageTracker().clear();
    dispatcher().drain();
}

TEST(Consumer, ReadTopicWithHeadersUsingConfig4)
{
    callbackCounters().reset();
    Connector connector = makeConsumerConnector(
        config4,
        getNewGroupName(),
        topicWithHeaders(),
        Callbacks::messageReceiverWithHeadersManualCommit,
        PartitionStrategy::Static,
        {{programOptions()._topicWithHeaders, 0, (int)OffsetPoint::AtBeginning},
         {programOptions()._topicWithHeaders, 1, (int)OffsetPoint::AtBeginning},
         {programOptions()._topicWithHeaders, 2, (int)OffsetPoint::AtBeginning},
         {programOptions()._topicWithHeaders, 3, (int)OffsetPoint::AtBeginning}});

    //enable consuming
    connector.consumer().resume(topicWithHeaders().topic());

    int loops = maxLoops;
    while (callbackCounters()._eof < 4 && loops--) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    EXPECT_EQ(messageTracker().totalMessages(), callbackCounters()._preprocessor);
    EXPECT_EQ(messageTracker().totalMessages(), callbackCounters()._receiver-callbackCounters()._eof);
    EXPECT_FALSE(callbackCounters()._receiverIoThread);
    
    //Check message validity
    EXPECT_EQ(messageTracker(), consumerMessageTracker());
    
    //Check async commits
    loops = maxLoops;
    while ((size_t)callbackCounters()._offsetCommit < messageTracker().totalMessages() && loops--) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    EXPECT_EQ(messageTracker().totalMessages(), callbackCounters()._offsetCommit);
    EXPECT_EQ(callbackCounters()._offsetCommitPartitions, consumerMessageTracker()._offsets);
    
    //clear everything
    consumerMessageTracker().clear();
    dispatcher().drain();
}

TEST(Consumer, SkipMessagesWithRelativeOffsetUsingConfig2)
{
    callbackCounters().reset();
    callbackCounters()._forceSkip = true;
    int msgPerPartition = 5;
    int totalMessages = msgPerPartition*4;
    
    Connector connector = makeConsumerConnector(
        config2,
        getNewGroupName(),
        topicWithHeaders(),
        Callbacks::messageReceiverWithHeadersManualCommit,
        PartitionStrategy::Static,
        {{programOptions()._topicWithHeaders, 0, (int)OffsetPoint::AtEndRelative-msgPerPartition},
         {programOptions()._topicWithHeaders, 1, (int)OffsetPoint::AtEndRelative-msgPerPartition},
         {programOptions()._topicWithHeaders, 2, (int)OffsetPoint::AtEndRelative-msgPerPartition},
         {programOptions()._topicWithHeaders, 3, (int)OffsetPoint::AtEndRelative-msgPerPartition}});
    int loops = maxLoops;
    while (callbackCounters()._eof < 4 && loops--) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    EXPECT_EQ(totalMessages, callbackCounters()._preprocessor);
    EXPECT_EQ(totalMessages+4, callbackCounters()._receiver); //including EOFs
    EXPECT_EQ(totalMessages, callbackCounters()._skip);
    EXPECT_EQ(0, consumerMessageTracker().totalMessages());
    
    //clear everything
    consumerMessageTracker().clear();
    dispatcher().drain();
}

TEST(Consumer, OffsetCommitManager)
{
    callbackCounters().reset();
    Connector connector = makeConsumerConnector(
            config4,
            getNewGroupName(),
            topicWithHeaders(),
            Callbacks::messageReceiverWithHeadersUsingCommitGuard,
            PartitionStrategy::Static,
            {{programOptions()._topicWithHeaders, 0, (int)OffsetPoint::AtBeginning},
             {programOptions()._topicWithHeaders, 1, (int)OffsetPoint::AtBeginning},
             {programOptions()._topicWithHeaders, 2, (int)OffsetPoint::AtBeginning},
             {programOptions()._topicWithHeaders, 3, (int)OffsetPoint::AtBeginning}});

    //Create the offset manager
    offsetManagerPtr = std::make_shared<OffsetManager>(connector.consumer());

    //enable consuming
    connector.consumer().resume(topicWithHeaders().topic());

    int loops = maxLoops;
    while (callbackCounters()._eof < 4 && loops--) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    //Check commits via offset manager
    loops = maxLoops;
    while ((size_t)callbackCounters()._offsetCommit < consumerMessageTracker().totalMessages() && loops--) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    EXPECT_EQ(callbackCounters()._offsetCommit, consumerMessageTracker().totalMessages());
    EXPECT_EQ(callbackCounters()._offsetCommitPartitions, consumerMessageTracker()._offsets);

    //clear everything
    consumerMessageTracker().clear();
    dispatcher().drain();
    offsetManagerPtr.reset();
}


}
}
}

