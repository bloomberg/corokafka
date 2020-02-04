#include <corokafka/corokafka.h>
#include <gtest/gtest.h>
#include <corokafka_tests_utils.h>
#include <future>

namespace Bloomberg {
namespace corokafka {
namespace tests {

//Global producer configurations
const int MaxMessages = 10;

//sync
Configuration::OptionList syncConfig = {
    {"enable.idempotence", false},
    {ProducerConfiguration::Options::payloadPolicy, "copy"},
    {ProducerConfiguration::Options::preserveMessageOrder, "true"},
    {ProducerConfiguration::Options::waitForAcksTimeoutMs, 1000}
};

Configuration::OptionList syncUnorderedConfig = {
    {"enable.idempotence", false},
    {ProducerConfiguration::Options::payloadPolicy, "passthrough"},
    {ProducerConfiguration::Options::preserveMessageOrder, "false"},
    {ProducerConfiguration::Options::waitForAcksTimeoutMs, -1}
};

Configuration::OptionList syncIdempotentConfig = {
    {"enable.idempotence", true},
    {ProducerConfiguration::Options::payloadPolicy, "passthrough"},
    {ProducerConfiguration::Options::preserveMessageOrder, "true"},
    {ProducerConfiguration::Options::waitForAcksTimeoutMs, -1}
};

//async
Configuration::OptionList asyncConfig = {
    {ProducerConfiguration::Options::payloadPolicy, "copy"},
    {ProducerConfiguration::Options::preserveMessageOrder, "true"},
    {ProducerConfiguration::Options::waitForAcksTimeoutMs, 0}
};

//async - no order preservation
Configuration::OptionList asyncUnorderedConfig = {
    {ProducerConfiguration::Options::payloadPolicy, "copy"},
    {ProducerConfiguration::Options::preserveMessageOrder, "false"},
    {ProducerConfiguration::Options::waitForAcksTimeoutMs, 0}
};

TEST(ProducerConfiguration, MissingBrokerList)
{
    ProducerConfiguration config(topicWithHeaders().topic(), {}, {}); //use all defaults
    ConfigurationBuilder builder;
    builder(config);
    ASSERT_THROW(Connector connector(builder, dispatcher()), InvalidOptionException);
    try { Connector connector(builder, dispatcher()); }
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
    ASSERT_THROW(Connector connector(builder, dispatcher()), InvalidOptionException);
}

TEST(ProducerConfiguration, UnknownInternalOption)
{
    testProducerOption<InvalidOptionException>("InvalidOptionException", "internal.producer.unknown.option",
        {{"bad",true}});
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
        {{"bad",true},{"-2",true},{"-1",false},{"0",false},{"1",false}});
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

TEST(ProducerConfiguration, InternalProducerPollIoThreadId)
{
    testProducerOption<InvalidOptionException>("InvalidOptionException", "internal.producer.poll.io.thread.id",
        {{"-2",true},{"-1",false},{"0",false},{"1",false}});
}

TEST(Producer, SendSyncWithoutHeaders)
{
    Connector connector = makeProducerConnector(syncConfig, programOptions()._topicWithoutHeaders);
    
    //Send messages
    SenderId id = SenderId::SyncWithoutHeaders;
    for (size_t i = 0; i < MaxMessages; ++i) {
        Message message{(int)i, getSenderStr(id)};
        messageWithoutHeadersTracker().add({id, message});
        if (programOptions()._kafkaType == KafkaType::Producer) {
            auto dr = connector.producer().send(topicWithoutHeaders(), nullptr, (Key) id, message);
            ASSERT_GT(dr.getNumBytesWritten(), 0);
        }
    }
}

TEST(Producer, SendSyncWithHeaders)
{
    Connector connector = makeProducerConnector(syncConfig, programOptions()._topicWithHeaders);
    
    //Send messages
    SenderId id = SenderId::Sync;
    for (size_t i = 0; i < MaxMessages; ++i) {
        Header1 header1{(Key)id, "test:consumer", "test:producer"};
        Header2 header2{std::chrono::system_clock::now()};
        Message message{(int)i, getSenderStr(id)};
        messageTracker().add({id, header1, header2, message});
        if (programOptions()._kafkaType == KafkaType::Producer) {
            auto dr = connector.producer().send(topicWithHeaders(), nullptr, header1._senderId, message, header1, header2);
            ASSERT_GT(dr.getNumBytesWritten(), 0);
        }
    }
}

TEST(Producer, SendSyncSkippingOneHeader)
{
    Connector connector = makeProducerConnector(syncConfig, programOptions()._topicWithHeaders);
    
    //Send messages
    SenderId id = SenderId::SyncSecondHeaderMissing;
    for (size_t i = 0; i < MaxMessages; ++i) {
        Header1 header1{(Key)id, "test:consumer", "test:producer"};
        Message message{(int)i, getSenderStr(id)};
        messageTracker().add({id, header1, message});
        if (programOptions()._kafkaType == KafkaType::Producer) {
            auto dr = connector.producer().send(topicWithHeaders(), nullptr, header1._senderId, message, header1, NullHeader{});
            ASSERT_GT(dr.getNumBytesWritten(), 0);
        }
    }
}

TEST(Producer, SendSyncSkippingBothHeaders)
{
    Connector connector = makeProducerConnector(syncConfig, programOptions()._topicWithHeaders);
    
    //Send messages
    SenderId id = SenderId::SyncBothHeadersMissing;
    for (size_t i = 0; i < MaxMessages; ++i) {
        Message message{(int)i, getSenderStr(id)};
        messageTracker().add({id, message});
        if (programOptions()._kafkaType == KafkaType::Producer) {
            auto dr = connector.producer().send(topicWithHeaders(), nullptr, (Key)id, message, NullHeader{}, NullHeader{});
            ASSERT_GT(dr.getNumBytesWritten(), 0);
        }
    }
}

TEST(Producer, SendSyncUnorderedWithHeaders)
{
    Connector connector = makeProducerConnector(syncUnorderedConfig, programOptions()._topicWithHeaders);
    
    //Send messages
    SenderId id = SenderId::SyncUnordered;
    for (size_t i = 0; i < MaxMessages; ++i) {
        Header1 header1{(Key)id, "test:consumer", "test:producer"};
        Header2 header2{std::chrono::system_clock::now()};
        Message message{(int)i, getSenderStr(id)};
        messageTracker().add({id, header1, header2, message});
        if (programOptions()._kafkaType == KafkaType::Producer) {
            auto dr = connector.producer().send(topicWithHeaders(), nullptr, header1._senderId, message, header1, header2);
            ASSERT_GT(dr.getNumBytesWritten(), 0);
        }
    }
}

TEST(Producer, SendSyncIdempotent)
{
    Connector connector = makeProducerConnector(syncIdempotentConfig, programOptions()._topicWithHeaders);
    
    //Send messages
    SenderId id = SenderId::SyncIdempotent;
    for (size_t i = 0; i < MaxMessages; ++i) {
        Header1 header1{(Key)id, "test:consumer", "test:producer"};
        Header2 header2{std::chrono::system_clock::now()};
        Message message{(int)i, getSenderStr(id)};
        messageTracker().add({id, header1, header2, message});
        if (programOptions()._kafkaType == KafkaType::Producer) {
            auto dr = connector.producer().send(topicWithHeaders(), nullptr, header1._senderId, message, header1, header2);
            ASSERT_GT(dr.getNumBytesWritten(), 0);
        }
    }
}

TEST(Producer, SendAsync)
{
    Connector connector = makeProducerConnector(asyncConfig, programOptions()._topicWithHeaders);
    
    std::vector<quantum::GenericFuture<DeliveryReport>> futures;
    //Send messages
    SenderId id = SenderId::Async;
    for (size_t i = 0; i < MaxMessages; ++i) {
        Header1 header1{(Key)id, "test:consumer", "test:producer"};
        Header2 header2{std::chrono::system_clock::now()};
        Message message{(int)i, getSenderStr(id)};
        messageTracker().add({id, header1, header2, message});
        if (programOptions()._kafkaType == KafkaType::Producer) {
            futures.push_back(connector.producer().post(topicWithHeaders(), nullptr, header1._senderId, std::move(message), std::move(header1), std::move(header2)));
        }
    }
    
    //get all the results
    if (programOptions()._kafkaType == KafkaType::Producer) {
        for (auto&& f : futures) {
            DeliveryReport dr = f.get();
            EXPECT_FALSE((bool)dr.getError());
            EXPECT_GT(dr.getNumBytesWritten(), 0);
        }
    }
}

TEST(Producer, SendAsyncUnordered)
{
    Connector connector = makeProducerConnector(asyncUnorderedConfig, programOptions()._topicWithHeaders);
    
    std::vector<quantum::GenericFuture<DeliveryReport>> futures;
    //Send messages
    SenderId id = SenderId::AsyncUnordered;
    for (size_t i = 0; i < MaxMessages; ++i) {
        Header1 header1{(Key)id, "test:consumer", "test:producer"};
        Header2 header2{std::chrono::system_clock::now()};
        Message message{(int)i, getSenderStr(id)};
        messageTracker().add({id, header1, header2, message});
        if (programOptions()._kafkaType == KafkaType::Producer) {
            futures.push_back(connector.producer().post(topicWithHeaders(), nullptr, header1._senderId, std::move(message), std::move(header1), std::move(header2)));
        }
    }
    
    //get all the results
    if (programOptions()._kafkaType == KafkaType::Producer) {
        for (auto&& f : futures) {
            DeliveryReport dr = f.get();
            EXPECT_FALSE((bool)dr.getError());
            EXPECT_GT(dr.getNumBytesWritten(), 0);
        }
    }
}

TEST(Producer, ValidateCallbacks)
{
    int opaque;
    callbackCounters().reset();
    
    Configuration::OptionList options = syncConfig;
    options.push_back({"metadata.broker.list", programOptions()._broker});
    options.push_back({"statistics.interval.ms", 100});
    options.push_back({"debug", "all"});
    options.push_back({"internal.producer.log.level", "warning"});
    ProducerConfiguration config(programOptions()._topicWithHeaders, options, {});
    config.setStatsCallback(Callbacks::handleStats);
    config.setDeliveryReportCallback(Callbacks::handleDeliveryReport);
    config.setPartitionerCallback(Callbacks::partitioner);
    config.setLogCallback(Callbacks::kafkaLogger);
    config.setErrorCallback(Callbacks::handleKafkaError);
    ConfigurationBuilder builder;
    builder(config);
    Connector connector{builder, dispatcher()};
    
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    
    //Send messages
    SenderId id = SenderId::Callbacks;
    for (size_t i = 0; i < MaxMessages/2; ++i) {
        Header1 header1{(Key)id, "test:consumer", "test:producer"};
        Header2 header2{std::chrono::system_clock::now()};
        Message message{(int)i, getSenderStr(id)};
        messageTracker().add({id, header1, header2, message});
        if (programOptions()._kafkaType == KafkaType::Producer) {
            auto dr = connector.producer().send(topicWithHeaders(), &opaque, header1._senderId, message, header1, header2);
            ASSERT_GT(dr.getNumBytesWritten(), 0);
        }
    }
    for (size_t i = MaxMessages/2; i < MaxMessages; ++i) {
        Header1 header1{(Key)id, "test:consumer", "test:producer"};
        Header2 header2{std::chrono::system_clock::now()};
        Message message{(int)i, getSenderStr(id)};
        messageTracker().add({id, header1, header2, message});
        if (programOptions()._kafkaType == KafkaType::Producer) {
            DeliveryReport dr = connector.producer().post(topicWithHeaders(), &opaque, header1._senderId, message, header1, header2).get();
            ASSERT_GT(dr.getNumBytesWritten(), 0);
            ASSERT_EQ(&opaque, dr.getOpaque());
        }
    }
    
    if (programOptions()._kafkaType == KafkaType::Producer) {
        CallbackCounters& counters = callbackCounters();
        int loop = 30;
        while (counters._deliveryReport < MaxMessages && loop--) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        //assert
        EXPECT_EQ(MaxMessages, counters._partitioner);
        EXPECT_EQ(MaxMessages, counters._deliveryReport);
        EXPECT_LT(1, counters._stats);
        EXPECT_LT(1, counters._logger);
        EXPECT_EQ(0, counters._error);
        EXPECT_EQ(&opaque, counters._opaque);
        counters.reset();
    }
}

TEST(Producer, ValidateErrorCallback)
{
    int opaque;
    Configuration::OptionList options = syncConfig;
    options.push_back({"metadata.broker.list", "bad:1111"});
    options.push_back({"retries", 0});
    options.push_back({"api.version.request", true});
    Configuration::OptionList topicOptions = {{"message.timeout.ms", 2000}};
    ProducerConfiguration config(programOptions()._topicWithHeaders, options, topicOptions);
    config.setErrorCallback(Callbacks::handleKafkaError, &opaque);
    ConfigurationBuilder builder;
    builder(config);
    Connector connector{builder, dispatcher()};
    
    //Send a message (this will not actually send anything because broker is unreachable)
    for (size_t i = 0; i < 1; ++i) {
        Header1 header1{(uint16_t)SenderId::Callbacks, "test:consumer", "test:producer"};
        Header2 header2{std::chrono::system_clock::now()};
        Message message{(int)i, "Validating error callback"};
        auto dr = connector.producer().send(topicWithHeaders(), nullptr, header1._senderId, message, header1, header2);
        ASSERT_NE(0, dr.getError().get_error());
    }
    
    //assert
    CallbackCounters& counters = callbackCounters();
    EXPECT_LT(1, counters._error);
    EXPECT_EQ(&opaque, counters._opaque);
    counters.reset();
}


}
}
}

