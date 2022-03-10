#include <corokafka/impl/corokafka_offset_manager_impl.h>
#include <corokafka/mock/corokafka_consumer_manager_mock.h>

#include <corokafka/corokafka_connector.h>
#include <corokafka/utils/corokafka_offset_manager.h>
#include <corokafka_tests_utils.h>

#include <cppkafka/topic_partition_list.h>

#include <chrono>
#include <functional>
#include <memory>

using testing::_;
using testing::Return;
using testing::ReturnRef;
using testing::SetArgReferee;
using testing::Throw;
using testing::IsTrue;
using testing::IsFalse;
using testing::Matcher;
using testing::NiceMock;

namespace Bloomberg::corokafka::tests
{

const std::string TopicName = "MockTopic";
const int Partition = 0;

//<partition, {low, high}>
const OffsetWatermarkList Watermarks = {
    {Partition, {2, 10}}
};
//<topic, partition, offset>
const cppkafka::TopicPartitionList Committed = {
    {TopicName, Partition, 5}
};
//<topic, partition, offset>
const cppkafka::TopicPartitionList CommittedBelowWatermark = {
    {TopicName, Partition, 1}
};
//<topic, partition, offset>
const cppkafka::TopicPartitionList CommittedInvalid = {
    {TopicName, Partition, cppkafka::TopicPartition::Offset::OFFSET_INVALID}
};
//<topic, partition, offset>
const cppkafka::TopicPartitionList InvalidAssignment = {
    {TopicName, Partition, cppkafka::TopicPartition::Offset::OFFSET_INVALID}
};
//<topic, partition, offset>
const cppkafka::TopicPartitionList BeginningAssignment = {
    {TopicName, Partition, cppkafka::TopicPartition::Offset::OFFSET_BEGINNING}
};
//<topic, partition, offset>
const cppkafka::TopicPartitionList EndAssignment = {
    {TopicName, Partition, cppkafka::TopicPartition::Offset::OFFSET_END}
};
//<topic, partition, offset>
const cppkafka::TopicPartitionList StoredAssignment = {
    {TopicName, Partition, cppkafka::TopicPartition::Offset::OFFSET_STORED}
};
//<topic, partition, offset>
const cppkafka::TopicPartitionList TailAssignment = {
    {TopicName, Partition, RD_KAFKA_OFFSET_TAIL_BASE-2}
};
//Used for partition queries
cppkafka::TopicPartition QueryPartition = {
    TopicName, Partition, cppkafka::TopicPartition::Offset::OFFSET_INVALID};

std::map<std::string, bool> autoOffsetResetMap = {
    {"beginning", false}, {"earliest", false}, {"smallest", false},
    {"end", true}, {"latest", true}, {"largest", true},
    {"badvalue", true}
};

// map<offset, tuple{assignment, committed, expectedCurrentOffset, auto.offset.reset}>
using OffsetSetting = std::tuple<cppkafka::TopicPartitionList, cppkafka::TopicPartitionList, int64_t, std::string>;
const std::map<int64_t, OffsetSetting>
OffsetSelectionMap = {
    {cppkafka::TopicPartition::Offset::OFFSET_INVALID,
        OffsetSetting{InvalidAssignment, Committed, 5, "beginning"}},
    {cppkafka::TopicPartition::Offset::OFFSET_INVALID,
        OffsetSetting{InvalidAssignment, CommittedBelowWatermark, 2, "beginning"}},
    {cppkafka::TopicPartition::Offset::OFFSET_INVALID,
        OffsetSetting{InvalidAssignment, CommittedInvalid, 2, "beginning"}},
    {cppkafka::TopicPartition::Offset::OFFSET_INVALID,
        OffsetSetting{InvalidAssignment, CommittedInvalid, 10, "end"}},
    {cppkafka::TopicPartition::Offset::OFFSET_STORED,
        OffsetSetting{StoredAssignment, Committed, 5, "beginning"}},
    {cppkafka::TopicPartition::Offset::OFFSET_STORED,
        OffsetSetting{StoredAssignment, CommittedBelowWatermark, 2, "beginning"}},
    {cppkafka::TopicPartition::Offset::OFFSET_STORED,
        OffsetSetting{StoredAssignment, CommittedInvalid, 2, "beginning"}},
    {cppkafka::TopicPartition::Offset::OFFSET_STORED,
        OffsetSetting{StoredAssignment, CommittedInvalid, 10, "end"}},
    {cppkafka::TopicPartition::Offset::OFFSET_BEGINNING,
        OffsetSetting{BeginningAssignment, Committed, 2, "end"}},
    {cppkafka::TopicPartition::Offset::OFFSET_END,
        OffsetSetting{EndAssignment, Committed, 10, "beginning"}},
    {RD_KAFKA_OFFSET_TAIL_BASE,
        OffsetSetting{TailAssignment, Committed, 8, "end"}}
};

struct OffsetManagerTestAdapter : public OffsetManagerImpl
{
    OffsetManagerTestAdapter(IConsumerManager& consumerManager) :
        OffsetManagerImpl(consumerManager)
    {}
    TopicSettings& topicSettings(const std::string& topicName) { return _topicMap[topicName]; }
};

TEST(OffsetManager, AutoResetOption)
{
    for (const auto& [value, atEnd] : autoOffsetResetMap) {
        NiceMock<mocks::ConsumerManagerMock> consumerManagerMock(mocks::MockTopic{TopicName},
                                                                 Configuration::OptionList{{"auto.offset.reset", value}},
                                                                 Configuration::OptionList{});
        //Expectations
        auto &metadataMock = consumerManagerMock.consumerMetadataMock();
        EXPECT_CALL(metadataMock, queryCommittedOffsets()).WillOnce(Return(Committed));
        EXPECT_CALL(metadataMock, queryOffsetWatermarks()).WillOnce(Return(Watermarks));
        EXPECT_CALL(metadataMock, getPartitionAssignment()).WillOnce(ReturnRef(StoredAssignment));
    
        OffsetManagerTestAdapter offsetManager(consumerManagerMock);
        //Check if auto reset is set
        EXPECT_EQ(atEnd, offsetManager.topicSettings(TopicName)._autoResetAtEnd);
    }
}

TEST(OffsetManager, SetCurrentOffset)
{
    for (const auto& [offset, setting] : OffsetSelectionMap) {
        const auto& [assignment, committed, expectedCurrent, autoOffsetReset] = setting;
        
        NiceMock<mocks::ConsumerManagerMock> consumerManagerMock(mocks::MockTopic{TopicName},
                                                                 Configuration::OptionList{{"auto.offset.reset", autoOffsetReset}},
                                                                 Configuration::OptionList{});
        //Expectations
        auto &metadataMock = consumerManagerMock.consumerMetadataMock();
        EXPECT_CALL(metadataMock, queryCommittedOffsets()).WillOnce(Return(committed));
        EXPECT_CALL(metadataMock, queryOffsetWatermarks()).WillOnce(Return(Watermarks));
        EXPECT_CALL(metadataMock, getPartitionAssignment()).WillOnce(ReturnRef(assignment));
    
        OffsetManagerTestAdapter offsetManager(consumerManagerMock);
        //validate
        EXPECT_EQ(expectedCurrent, offsetManager.getCurrentOffset(
                {TopicName, Partition, cppkafka::TopicPartition::Offset::OFFSET_INVALID}).get_offset());
        EXPECT_EQ(expectedCurrent, offsetManager.getBeginOffset(
                {TopicName, Partition, cppkafka::TopicPartition::Offset::OFFSET_INVALID}).get_offset());
    }
}

TEST(OffsetManager, CommitAllCurrentOffsets)
{
    NiceMock<mocks::ConsumerManagerMock> consumerManagerMock(mocks::MockTopic{TopicName},
                                                             Configuration::OptionList{},
                                                             Configuration::OptionList{});
    //Expectations
    auto &metadataMock = consumerManagerMock.consumerMetadataMock();
    EXPECT_CALL(metadataMock, queryCommittedOffsets()).WillOnce(Return(Committed));
    EXPECT_CALL(metadataMock, queryOffsetWatermarks()).WillOnce(Return(Watermarks));
    EXPECT_CALL(metadataMock, getPartitionAssignment()).WillOnce(ReturnRef(StoredAssignment));

    OffsetManagerTestAdapter offsetManager(consumerManagerMock);
    
    auto current = offsetManager.getCurrentOffset(QueryPartition);
    cppkafka::TopicPartition nextCurrent = current + 1;
    cppkafka::TopicPartitionList currentList = {nextCurrent};
    EXPECT_CALL(consumerManagerMock, commit(currentList, nullptr)).Times(1);
    offsetManager.forceCommitCurrentOffset();
    
    //Make sure the current offset has also been changed but not the beginning offset
    EXPECT_EQ(nextCurrent, offsetManager.getCurrentOffset(QueryPartition));
    EXPECT_EQ(current, offsetManager.getBeginOffset(QueryPartition));
}

TEST(OffsetManager, CommitSpecificCurrentOffset)
{
    NiceMock<mocks::ConsumerManagerMock> consumerManagerMock(mocks::MockTopic{TopicName},
                                                             Configuration::OptionList{},
                                                             Configuration::OptionList{});
    //Expectations
    auto &metadataMock = consumerManagerMock.consumerMetadataMock();
    EXPECT_CALL(metadataMock, queryCommittedOffsets()).WillOnce(Return(Committed));
    EXPECT_CALL(metadataMock, queryOffsetWatermarks()).WillOnce(Return(Watermarks));
    EXPECT_CALL(metadataMock, getPartitionAssignment()).WillOnce(ReturnRef(StoredAssignment));

    OffsetManagerTestAdapter offsetManager(consumerManagerMock);
    
    auto current = offsetManager.getCurrentOffset(QueryPartition);
    cppkafka::TopicPartition nextCurrent = current + 1;
    EXPECT_CALL(consumerManagerMock, commit(nextCurrent, nullptr)).Times(1);
    offsetManager.forceCommitCurrentOffset(QueryPartition);
    
    //Make sure the current offset has also been changed but not the beginning offset
    EXPECT_EQ(nextCurrent, offsetManager.getCurrentOffset(QueryPartition));
    EXPECT_EQ(current, offsetManager.getBeginOffset(QueryPartition));
}

TEST(OffsetManager, SaveOffsetsWithGaps)
{
    NiceMock<mocks::ConsumerManagerMock> consumerManagerMock(mocks::MockTopic{TopicName},
                                                             Configuration::OptionList{},
                                                             Configuration::OptionList{});
    //Expectations
    auto &metadataMock = consumerManagerMock.consumerMetadataMock();
    EXPECT_CALL(metadataMock, queryCommittedOffsets()).WillOnce(Return(Committed));
    EXPECT_CALL(metadataMock, queryOffsetWatermarks()).WillOnce(Return(Watermarks));
    EXPECT_CALL(metadataMock, getPartitionAssignment()).WillOnce(ReturnRef(StoredAssignment));

    OffsetManagerTestAdapter offsetManager(consumerManagerMock);
    
    auto current = offsetManager.getCurrentOffset(QueryPartition);
    
    //Create next offsets
    cppkafka::TopicPartition offsetLow = current + 2;
    cppkafka::TopicPartition offsetMiddle = current + 3;
    cppkafka::TopicPartition offsetHigh = current + 4;
    
    //Make sure we don't get any commits when we insert the offsets
    EXPECT_CALL(consumerManagerMock, commit(Matcher<const cppkafka::TopicPartition&>(_), nullptr)).Times(0);
    offsetManager.saveOffset(offsetLow);
    //Insert the next offset with a gap of 1
    offsetManager.saveOffset(offsetHigh);
    
    //Get the low and high offsets and make sure they correspond with what we entered
    std::pair<int64_t, int64_t> lowHigh{offsetLow.get_offset(), offsetHigh.get_offset()};
    auto uncommittedOffsets = offsetManager.getUncommittedOffsetMargins(QueryPartition);
    std::pair<int64_t, int64_t> lowHighMargins = {uncommittedOffsets.first.get_offset(), uncommittedOffsets.second.get_offset()};
    EXPECT_EQ(lowHigh, lowHighMargins);
    
    //Insert a middle offset and make sure we still don't get any commits
    offsetManager.saveOffset(offsetHigh);
    auto uncommittedOffsets2 = offsetManager.getUncommittedOffsetMargins(QueryPartition);
    EXPECT_EQ(uncommittedOffsets, uncommittedOffsets2);
    
    //Current offset remained the same
    EXPECT_EQ(current, offsetManager.getCurrentOffset(QueryPartition));
}

TEST(OffsetManager, SaveOffsetsAndFillGap)
{
    NiceMock<mocks::ConsumerManagerMock> consumerManagerMock(mocks::MockTopic{TopicName},
                                                             Configuration::OptionList{},
                                                             Configuration::OptionList{});
    //Expectations
    auto &metadataMock = consumerManagerMock.consumerMetadataMock();
    EXPECT_CALL(metadataMock, queryCommittedOffsets()).WillOnce(Return(Committed));
    EXPECT_CALL(metadataMock, queryOffsetWatermarks()).WillOnce(Return(Watermarks));
    EXPECT_CALL(metadataMock, getPartitionAssignment()).WillOnce(ReturnRef(StoredAssignment));

    OffsetManagerTestAdapter offsetManager(consumerManagerMock);
    
    auto current = offsetManager.getCurrentOffset(QueryPartition);
    
    //Create next offsets
    cppkafka::TopicPartition nextCurrent = current + 1;
    cppkafka::TopicPartition offsetLow = current + 2;
    cppkafka::TopicPartition offsetMiddle = current + 3;
    cppkafka::TopicPartition offsetHigh = current + 4;
    
    //Make sure we don't get any commits when we insert the offsets
    EXPECT_CALL(consumerManagerMock, commit(offsetHigh, nullptr)).Times(1);
    offsetManager.saveOffset(offsetLow);
    //Insert the next offset with a gap of 1
    offsetManager.saveOffset(offsetHigh);
    //Fill the gap
    offsetManager.saveOffset(offsetMiddle);
    
    //save the offset immediately after current which will trigger a commit
    offsetManager.saveOffset(nextCurrent);
    
    //Make sure the new current has been updated with the high offset value
    EXPECT_EQ(offsetHigh, offsetManager.getCurrentOffset(QueryPartition));
}

TEST(OffsetManager, ForceCommitLowestOffset)
{
    NiceMock<mocks::ConsumerManagerMock> consumerManagerMock(mocks::MockTopic{TopicName},
                                                             Configuration::OptionList{},
                                                             Configuration::OptionList{});
    //Expectations
    auto &metadataMock = consumerManagerMock.consumerMetadataMock();
    EXPECT_CALL(metadataMock, queryCommittedOffsets()).WillOnce(Return(Committed));
    EXPECT_CALL(metadataMock, queryOffsetWatermarks()).WillOnce(Return(Watermarks));
    EXPECT_CALL(metadataMock, getPartitionAssignment()).WillOnce(ReturnRef(StoredAssignment));

    OffsetManagerTestAdapter offsetManager(consumerManagerMock);
    
    auto current = offsetManager.getCurrentOffset(QueryPartition);
    
    //Create next offsets
    cppkafka::TopicPartition offsetLow = current + 2;
    cppkafka::TopicPartition offsetMiddle = current + 3;
    cppkafka::TopicPartition offsetHigh = current + 4;
    
    //Make sure we don't get any commits when we insert the offsets
    EXPECT_CALL(consumerManagerMock, commit(offsetHigh, nullptr)).Times(1);
    offsetManager.saveOffset(offsetLow);
    //Insert the next offset with a gap of 1
    offsetManager.saveOffset(offsetHigh);
    //Fill the gap
    offsetManager.saveOffset(offsetMiddle);
    
    //commit the highest offset in the lowest range which is 'offsetHigh'.
    offsetManager.forceCommit(QueryPartition);
    
    //Make sure the new current has been updated with the high offset value
    EXPECT_EQ(offsetHigh, offsetManager.getCurrentOffset(QueryPartition));
}

TEST(OffsetManager, SaveInvalidOffset)
{
    NiceMock<mocks::ConsumerManagerMock> consumerManagerMock(mocks::MockTopic{TopicName},
                                                             Configuration::OptionList{},
                                                             Configuration::OptionList{});
    //Expectations
    auto &metadataMock = consumerManagerMock.consumerMetadataMock();
    EXPECT_CALL(metadataMock, queryCommittedOffsets()).WillOnce(Return(Committed));
    EXPECT_CALL(metadataMock, queryOffsetWatermarks()).WillOnce(Return(Watermarks));
    EXPECT_CALL(metadataMock, getPartitionAssignment()).WillOnce(ReturnRef(StoredAssignment));

    OffsetManagerTestAdapter offsetManager(consumerManagerMock);
    
    auto current = offsetManager.getCurrentOffset(QueryPartition);
    EXPECT_CALL(consumerManagerMock, commit(Matcher<const cppkafka::TopicPartition&>(_), nullptr)).Times(0);
    
    //Offsets must be > current
    offsetManager.saveOffset(current);
    offsetManager.saveOffset(current-1);
}

TEST(OffsetManager, ResetPartitionAndFetchOffsets)
{
    NiceMock<mocks::ConsumerManagerMock> consumerManagerMock(mocks::MockTopic{TopicName},
                                                             Configuration::OptionList{},
                                                             Configuration::OptionList{});
    //Expectations
    auto &metadataMock = consumerManagerMock.consumerMetadataMock();
    EXPECT_CALL(metadataMock, queryCommittedOffsets()).WillOnce(Return(Committed));
    EXPECT_CALL(metadataMock, queryOffsetWatermarks()).WillOnce(Return(Watermarks));
    EXPECT_CALL(metadataMock, getPartitionAssignment()).WillOnce(ReturnRef(StoredAssignment));

    OffsetManagerTestAdapter offsetManager(consumerManagerMock);
    
    //Commit the current partition
    auto current = offsetManager.getCurrentOffset(QueryPartition);
    cppkafka::TopicPartition nextCurrent = current + 1;
    EXPECT_CALL(consumerManagerMock, commit(nextCurrent, nullptr)).Times(1);
    offsetManager.forceCommitCurrentOffset(QueryPartition);
    
    //Make sure the current offset has also been changed
    EXPECT_EQ(nextCurrent, offsetManager.getCurrentOffset(QueryPartition));
    
    //Reset the partition and fetch again
    EXPECT_CALL(metadataMock, queryCommittedOffsets()).WillOnce(Return(Committed));
    EXPECT_CALL(metadataMock, queryOffsetWatermarks()).WillOnce(Return(Watermarks));
    EXPECT_CALL(metadataMock, getPartitionAssignment()).WillOnce(ReturnRef(StoredAssignment));
    
    //Reset all partitions
    offsetManager.resetPartitionOffsets(TopicName, IOffsetManager::ResetAction::FetchOffsets);
    
    //Current offset is now reset
    EXPECT_EQ(Committed.front(), offsetManager.getCurrentOffset(QueryPartition));
}

TEST(OffsetManager, ResetPartitionNoFetch)
{
    NiceMock<mocks::ConsumerManagerMock> consumerManagerMock(mocks::MockTopic{TopicName},
                                                             Configuration::OptionList{},
                                                             Configuration::OptionList{});
    //Expectations
    auto &metadataMock = consumerManagerMock.consumerMetadataMock();
    EXPECT_CALL(metadataMock, queryCommittedOffsets()).WillOnce(Return(Committed));
    EXPECT_CALL(metadataMock, queryOffsetWatermarks()).WillOnce(Return(Watermarks));
    EXPECT_CALL(metadataMock, getPartitionAssignment()).WillOnce(ReturnRef(StoredAssignment));

    OffsetManagerTestAdapter offsetManager(consumerManagerMock);
    
    //Commit the current partition
    auto current = offsetManager.getCurrentOffset(QueryPartition);
    cppkafka::TopicPartition nextCurrent = current + 1;
    EXPECT_CALL(consumerManagerMock, commit(nextCurrent, nullptr)).Times(1);
    offsetManager.forceCommitCurrentOffset(QueryPartition);
    
    //Make sure the current offset has also been changed
    EXPECT_EQ(nextCurrent, offsetManager.getCurrentOffset(QueryPartition));
    
    //Reset the partition and fetch again
    EXPECT_CALL(metadataMock, queryCommittedOffsets()).Times(0);
    EXPECT_CALL(metadataMock, queryOffsetWatermarks()).Times(0);
    EXPECT_CALL(metadataMock, getPartitionAssignment()).Times(0);
    
    //Reset all partitions
    offsetManager.resetPartitionOffsets(TopicName, IOffsetManager::ResetAction::DoNotFetchOffsets);
    
    //There are no more partitions
    EXPECT_THROW(offsetManager.getCurrentOffset(QueryPartition), std::out_of_range);
}

static int32_t partition0Callback(const ProducerMetadata &metadata,
                               const cppkafka::Buffer &key,
                               int32_t partitionCount)
{
    return 0;
}

class OffsetManagerTester
{
public:
    OffsetManagerTester()
    {
        std::cout << "OffsetManagerTester(): starting" << std::endl;
        using std::placeholders::_1;
        using std::placeholders::_2;
        using std::placeholders::_3;
        using std::placeholders::_4;

        ConfigurationBuilder builder;
        // Connector configuration
        ConnectorConfiguration connConfig(
                { { ConnectorConfiguration::Options::pollIntervalMs, 10 } });
        builder(connConfig);

        Configuration::OptionList topicOptions{ { TopicConfiguration::Options::brokerTimeoutMs,
                                                 5000 } };
        // Consumer configuration
        Configuration::OptionList consumerOptions{
            { "metadata.broker.list", programOptions()._broker },
            { "client.id", "offset-manager-consumer" },
            { "group.id", "offset-manager-group" },
            { "enable.auto.offset.store", false },
            { "enable.partition.eof", false },
            { "enable.auto.commit", true },
            { ConsumerConfiguration::Options::timeoutMs, 100 },
            { ConsumerConfiguration::Options::pauseOnStart, false },
            { ConsumerConfiguration::Options::readSize, 100 },
            { ConsumerConfiguration::Options::pollStrategy, "batch" },
            { ConsumerConfiguration::Options::offsetPersistStrategy, "store" },
            { ConsumerConfiguration::Options::commitExec, "sync" },
            { ConsumerConfiguration::Options::autoOffsetPersist, "true" },
            { ConsumerConfiguration::Options::receiveInvokeThread, "coro" },
            { ConsumerConfiguration::Options::preprocessMessages, "false" },
            { ConsumerConfiguration::Options::receiveCallbackThreadRangeLow, 1 },
            { ConsumerConfiguration::Options::receiveCallbackThreadRangeHigh, 1 },
            { ConsumerConfiguration::Options::preserveMessageOrder, true },
        };
        ConsumerConfiguration consumerConfig{
            topicWithoutHeaders(),
            consumerOptions,
            topicOptions,
            std::bind(&OffsetManagerTester::receiveCallback, this, _1)
        };
        consumerConfig.setOffsetCommitCallback(
                std::bind(&OffsetManagerTester::offsetCommitCallback, this, _1, _2, _3, _4));
        consumerConfig.setLogCallback(std::bind(&OffsetManagerTester::logCallback, this, _1, _2, _3, _4));
        consumerConfig.assignInitialPartitions(PartitionStrategy::Static,
                                               { { "", 0, RD_KAFKA_OFFSET_INVALID } });

        builder(consumerConfig);
        // Producer configuration
        Configuration::OptionList producerOptions{
            { "metadata.broker.list", programOptions()._broker },
            { "client.id", "offset-manager-producer" },
            { "enable.idempotence", true },
        };
        ProducerConfiguration producerConfig{ topicWithoutHeaders(),
                                              producerOptions,
                                              topicOptions };
        producerConfig.setPartitionerCallback(partition0Callback);
        builder(producerConfig);
        std::cout << "OffsetManagerTester(): config completed" << std::endl;
        d_connector = std::make_unique<Connector>(builder, dispatcher());
        std::cout << "OffsetManagerTester(): connector created" << std::endl;
        // Wait for connector to get connected
        using namespace std::chrono_literals;
        std::this_thread::sleep_for(5s);

        // Create OffsetManager
        d_offsetManager = std::make_unique<OffsetManager>(d_connector->consumer(), -1ms);
        std::cout << "OffsetManagerTester(): offsetManager created" << std::endl;
    }

    ~OffsetManagerTester() { d_connector->shutdown(); }

    void produce(const unsigned int numMessages) {
        std::cout << "OffsetManagerTester::produce(): producing " << numMessages << std::endl;
        Key     key{ 0 };
        Message payload;
        payload._message = { "test message" };
        for (unsigned int i = 0; i < numMessages; ++i)
        {
            payload._num = i;
            d_connector->producer().send(topicWithoutHeaders(), nullptr, key, payload);
        }
        std::cout << "OffsetManagerTester::produce(): done" << std::endl;
    }

    void verifyRaceCondition(const unsigned int numOffsets)
    {
        auto offsets = extractOffsets(numOffsets);
        if (offsets.empty())
        {
            // This will fail and cause the test to fail
            EXPECT_EQ(numOffsets, d_offsets.size());
            return;
        }

        {
            quantum::Mutex::Guard guard{ quantum::local::context(), d_offsetsMutex };
            d_lastCommitted = std::nullopt;
        }

        std::vector<Bloomberg::quantum::ThreadContext<int>::Ptr> futures;
        for (const auto& offset : offsets)
        {
            futures.emplace_back(dispatcher().post2(
                    [this](Bloomberg::quantum::CoroContext<int>::Ptr ctx,
                           cppkafka::TopicPartition                  offset) -> int {
                        // Save the offset
                        d_offsetManager->saveOffset(offset);
                        return ctx->set(0);
                    },
                    offset));
        }

        for (auto& future : futures)
        {
            future->get();
        }

        using namespace std::chrono_literals;
        
        while(true)
        {
            {
                quantum::Mutex::Guard guard{ quantum::local::context(), d_offsetsMutex };
                if (d_lastCommitted)
                {
                    EXPECT_EQ(offsets.rbegin()->get_offset(), d_lastCommitted.value().get_offset());
                    return;
                }
            }
            
            std::this_thread::sleep_for(1s);
        }
    }

private:
    // Members
    // Pointers used for deferred initialization
    std::unique_ptr<Connector>     d_connector;
    std::unique_ptr<OffsetManager> d_offsetManager;
    quantum::Mutex                 d_offsetsMutex;
    cppkafka::TopicPartitionList   d_offsets;
    std::optional<cppkafka::TopicPartition> d_lastCommitted;

    // Functions
    void receiveCallback(MessageWithoutHeaders received)
    {
        if (received.isEof())
        {
            return;
        }
        quantum::Mutex::Guard guard{ quantum::local::context(), d_offsetsMutex };
        d_offsets.emplace_back(cppkafka::TopicPartition{
                received.getTopic(), received.getPartition(), received.getOffset() });
    }

    void offsetCommitCallback(const ConsumerMetadata&             metadata,
                              cppkafka::Error                     error,
                              const cppkafka::TopicPartitionList& topicPartitions,
                              const std::vector<void*>&           opaques)
    {
        for (const auto& topicPartition : topicPartitions)
        {
            if (topicPartition.get_offset() == RD_KAFKA_OFFSET_INVALID)
            {
                continue;
            }
            if (!error)
            {
                quantum::Mutex::Guard guard{ quantum::local::context(), d_offsetsMutex };
                d_lastCommitted = topicPartition;
            }
        }
    }

    void logCallback(const Bloomberg::corokafka::Metadata& metadata,
                     cppkafka::LogLevel                    level,
                     const std::string&                    facility,
                     const std::string&                    message)
    {
        static std::set<char> evens      = { '0', '2', '4', '6', '8' };
        auto                  secondToLast = *(message.rbegin() + 1);
        if ((facility == "OffsetManager:Commit") && (evens.count(secondToLast) == 1u))
        {
            auto ctx = Bloomberg::quantum::local::context();
            if (ctx)
            {
                ctx->yield();
            }
        }
    }

    cppkafka::TopicPartitionList extractOffsets(const unsigned int numOffsets)
    {
        quantum::Mutex::Guard guard{ quantum::local::context(), d_offsetsMutex };
        if (d_offsets.size() < numOffsets)
        {
            return {};
        }
        cppkafka::TopicPartitionList ret;
        for (int i = 0; i < numOffsets; ++i)
        {
            ret.emplace_back(d_offsets.front());
            d_offsets.erase(d_offsets.begin());
        }

        return ret;
    }
};    // class OffsetManagerTester

TEST(OffsetManager, SaveOffsetRace)
{
    OffsetManagerTester tester;

    unsigned int numTests = 100;

    tester.produce(numTests * 2);

    for (unsigned int i = 0; i < numTests; ++i)
    {
        tester.verifyRaceCondition(2);
    }
}

}   // namespace Bloomberg::corokafka::tests

