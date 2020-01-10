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
    {ProducerConfiguration::Options::payloadPolicy, "copy"},
    {ProducerConfiguration::Options::preserveMessageOrder, "false"},
    {ProducerConfiguration::Options::waitForAcksTimeoutMs, -1}
};

Configuration::OptionList syncIdempotentConfig = {
    {"enable.idempotence", true},
    {ProducerConfiguration::Options::payloadPolicy, "copy"},
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

TEST(Producer, SendSyncWithoutHeaders)
{
    Connector connector = makeProducerConnector(syncConfig, programOptions()._topicWithoutHeaders);
    
    //Send messages
    for (size_t i = 0; i < MaxMessages; ++i) {
        Message message{(int)i, "Sync message without headers"};
        int num = connector.producer().send(topicWithoutHeaders(), nullptr, i, message);
        ASSERT_GT(num, 0);
    }
}

TEST(Producer, SendSyncWithHeaders)
{
    Connector connector = makeProducerConnector(syncConfig, programOptions()._topicWithHeaders);
    
    //Send messages
    for (size_t i = 0; i < MaxMessages; ++i) {
        Header1 header1{(uint16_t)SenderId::Sync, "test:consumer", "test:producer"};
        Header2 header2{std::chrono::system_clock::now()};
        Message message{(int)i, "Sync message with 2 headers"};
        int num = connector.producer().send(topicWithHeaders(), nullptr, i, message, header1, header2);
        ASSERT_GT(num, 0);
    }
}

TEST(Producer, SendSyncSkippingOneHeader)
{
    Connector connector = makeProducerConnector(syncConfig, programOptions()._topicWithHeaders);
    
    //Send messages
    for (size_t i = 0; i < MaxMessages; ++i) {
        Header1 header1{(uint16_t)SenderId::SyncOneHeaderMissing, "test:consumer", "test:producer"};
        Message message{(int)i, "Sync message with one header missing"};
        int num = connector.producer().send(topicWithHeaders(), nullptr, i, message, header1, NullHeader{});
        ASSERT_GT(num, 0);
    }
}

TEST(Producer, SendSyncSkippingBothHeaders)
{
    Connector connector = makeProducerConnector(syncConfig, programOptions()._topicWithHeaders);
    
    //Send messages
    for (size_t i = 0; i < MaxMessages; ++i) {
        Message message{(int)i, "Sync message with both headers missing"};
        int num = connector.producer().send(topicWithHeaders(), nullptr, i, message, NullHeader{}, NullHeader{});
        ASSERT_GT(num, 0);
    }
}

TEST(Producer, SendSyncUnorderedWithHeaders)
{
    Connector connector = makeProducerConnector(syncUnorderedConfig, programOptions()._topicWithHeaders);
    
    //Send messages
    for (size_t i = 0; i < MaxMessages; ++i) {
        Header1 header1{(uint16_t)SenderId::SyncUnordered, "test:consumer", "test:producer"};
        Header2 header2{std::chrono::system_clock::now()};
        Message message{(int)i, "Sync unordered message"};
        int num = connector.producer().send(topicWithHeaders(), nullptr, i, message, header1, header2);
        ASSERT_GT(num, 0);
    }
}

TEST(Producer, SendSyncIdempotent)
{
    Connector connector = makeProducerConnector(syncIdempotentConfig, programOptions()._topicWithHeaders);
    
    //Send messages
    for (size_t i = 0; i < MaxMessages; ++i) {
        Header1 header1{(uint16_t)SenderId::SyncIdempotent, "test:consumer", "test:producer"};
        Header2 header2{std::chrono::system_clock::now()};
        Message message{(int)i, "Sync idempotent message"};
        int num = connector.producer().send(topicWithHeaders(), nullptr, i, message, header1, header2);
        ASSERT_GT(num, 0);
    }
}

TEST(Producer, SendAsync)
{
    Connector connector = makeProducerConnector(asyncConfig, programOptions()._topicWithHeaders);
    
    std::vector<quantum::GenericFuture<DeliveryReport>> futures;
    //Send messages
    for (size_t i = 0; i < MaxMessages; ++i) {
        Header1 header1{(uint16_t)SenderId::Async, "test:consumer", "test:producer"};
        Header2 header2{std::chrono::system_clock::now()};
        Message message{(int)i, "Async message"};
        futures.push_back(connector.producer().post(topicWithHeaders(), nullptr, i, std::move(message), std::move(header1), std::move(header2)));
    }
    
    //get all the results
    for (auto&& f : futures) {
        DeliveryReport dr = f.get();
        ASSERT_FALSE((bool)dr.getError());
        ASSERT_GT(dr.getNumBytesWritten(), 0);
    }
}

TEST(Producer, SendAsyncUnordered)
{
    Connector connector = makeProducerConnector(asyncUnorderedConfig, programOptions()._topicWithHeaders);
    
    std::vector<quantum::GenericFuture<DeliveryReport>> futures;
    //Send messages
    for (size_t i = 0; i < MaxMessages; ++i) {
        Header1 header1{(uint16_t)SenderId::AsyncUnordered, "test:consumer", "test:producer"};
        Header2 header2{std::chrono::system_clock::now()};
        Message message{(int)i, "Async unordered message"};
        futures.push_back(connector.producer().post(topicWithHeaders(), nullptr, i, std::move(message), std::move(header1), std::move(header2)));
    }
    
    //get all the results
    for (auto&& f : futures) {
        DeliveryReport dr = f.get();
        ASSERT_FALSE((bool)dr.getError());
        ASSERT_GT(dr.getNumBytesWritten(), 0);
    }
}

TEST(Producer, ValidateCallbacks)
{
    int opaque;
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
    Connector connector{builder};
    
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    
    //Send messages
    for (size_t i = 0; i < MaxMessages/2; ++i) {
        Header1 header1{(uint16_t)SenderId::Callbacks, "test:consumer", "test:producer"};
        Header2 header2{std::chrono::system_clock::now()};
        Message message{(int)i, "Validating callbacks"};
        int num = connector.producer().send(topicWithHeaders(), &opaque, i, message, header1, header2);
        ASSERT_GT(num, 0);
    }
    for (size_t i = MaxMessages/2; i < MaxMessages; ++i) {
        Header1 header1{(uint16_t)SenderId::Callbacks, "test:consumer", "test:producer"};
        Header2 header2{std::chrono::system_clock::now()};
        Message message{(int)i, "Validating callbacks"};
        DeliveryReport dr = connector.producer().post(topicWithHeaders(), &opaque, i, message, header1, header2).get();
        ASSERT_GT(dr.getNumBytesWritten(), 0);
        ASSERT_EQ(&opaque, dr.getOpaque());
    }
    
    //assert
    CallbackCounters& counters = callbackCounters();
    ASSERT_EQ(MaxMessages, counters._partitioner);
    ASSERT_EQ(MaxMessages, counters._deliveryReport);
    ASSERT_LT(1, counters._stats);
    ASSERT_LT(1, counters._logger);
    ASSERT_EQ(0, counters._error);
    ASSERT_EQ(&opaque, counters._opaque);
    counters.reset();
}

TEST(Producer, ValidateErrorCallback)
{
    int opaque;
    Configuration::OptionList options = syncConfig;
    options.push_back({"metadata.broker.list", "bad:1111"});
    options.push_back({"retries",0});
    options.push_back({"api.version.request",true});
    ProducerConfiguration config(programOptions()._topicWithHeaders, options, {});
    config.setErrorCallback(Callbacks::handleKafkaError, &opaque);
    ConfigurationBuilder builder;
    builder(config);
    Connector connector{builder};
    
    //Send a message (this will not actually send anything because broker is unreachable)
    for (size_t i = 0; i < 1; ++i) {
        Header1 header1{(uint16_t)SenderId::Callbacks, "test:consumer", "test:producer"};
        Header2 header2{std::chrono::system_clock::now()};
        Message message{(int)i, "Validating error callback"};
        int num = connector.producer().send(topicWithHeaders(), nullptr, i, message, header1, header2);
        ASSERT_GT(num, 0);
    }
    
    //assert
    CallbackCounters& counters = callbackCounters();
    ASSERT_LT(1, counters._error);
    ASSERT_EQ(&opaque, counters._opaque);
    counters.reset();
}


}
}
}

