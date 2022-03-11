#include <corokafka/impl/corokafka_offset_manager_impl.h>
#include <corokafka/mock/corokafka_consumer_manager_mock.h>

#include <corokafka_tests_utils.h>

#include <quantum/quantum_mutex.h>

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

class ConsumerManagerOffsetRaceFake : public mocks::ConsumerManagerMock
{
public:
    template<typename TOPIC>
    ConsumerManagerOffsetRaceFake(const TOPIC&              topic,
                                  Configuration::OptionList options,
                                  Configuration::OptionList topicOptions)
        : mocks::ConsumerManagerMock{ topic, options, topicOptions }
    {}

    // Fake commit() - trust that topicPartitions is always size=1 for these tests
    cppkafka::Error commit(const cppkafka::TopicPartition& topicPartition,
                           const void*                     opaque) override
    {
        auto ctx = quantum::local::context();
        // yield if offset is even - to increase the odds of the race condition
        if (topicPartition.get_offset() % 2 == 0)
        {
            if (ctx)
            {
                ctx->yield();
            }
        }

        quantum::Mutex::Guard guard{ ctx, d_mutex };
        std::cout << "Going to record commit() " << topicPartition << std::endl;
        d_committed = topicPartition;

        return {};
    }

    int64_t getLastOffset()
    {
        quantum::Mutex::Guard guard{ quantum::local::context(), d_mutex };
        return d_committed.get_offset();
    }

private:
    quantum::Mutex           d_mutex;
    cppkafka::TopicPartition d_committed;

};    // ConsumerManagerOffsetRaceFake

TEST(OffsetManager, SaveOffsetRace)
{
    NiceMock<ConsumerManagerOffsetRaceFake> consumerManagerMock(mocks::MockTopic{ TopicName },
                                                                Configuration::OptionList{},
                                                                Configuration::OptionList{});
    // Expectations
    auto& metadataMock = consumerManagerMock.consumerMetadataMock();
    const OffsetWatermarkList watermarks{
        { Partition,
          { cppkafka::TopicPartition::Offset::OFFSET_INVALID,
            cppkafka::TopicPartition::Offset::OFFSET_INVALID } }
    };
    EXPECT_CALL(metadataMock, queryCommittedOffsets()).WillOnce(Return(CommittedInvalid));
    EXPECT_CALL(metadataMock, queryOffsetWatermarks()).WillOnce(Return(watermarks));
    EXPECT_CALL(metadataMock, getPartitionAssignment()).WillOnce(ReturnRef(BeginningAssignment));

    OffsetManagerTestAdapter offsetManager(consumerManagerMock);
    offsetManager.resetPartitionOffsets(TopicName,
                                        OffsetManagerImpl::ResetAction::DoNotFetchOffsets);

    unsigned int numTests = 10;

    for (unsigned int i = 0; i < numTests; ++i)
    {
        unsigned int                 low  = i * 2;
        unsigned int                 high = low + 1;
        cppkafka::TopicPartitionList offsets{ { TopicName, 0, low }, { TopicName, 0, high } };
        std::vector<Bloomberg::quantum::ThreadContext<int>::Ptr> futures;
        for (const auto& offset : offsets)
        {
            futures.emplace_back(dispatcher().post2(
                    [&offsetManager](Bloomberg::quantum::CoroContext<int>::Ptr ctx,
                                     cppkafka::TopicPartition                  offset) -> int {
                        // Save the offset
                        offsetManager.saveOffset(offset);
                        return ctx->set(0);
                    },
                    offset));
        }

        for (auto& future : futures)
        {
            future->get();
        }

        std::ostringstream stream;
        stream << "Test #" << i << " Offsets [" << low << ", " << high << "]";
        SCOPED_TRACE(stream.str());
        EXPECT_EQ(high, consumerManagerMock.getLastOffset());
    }
}

}   // namespace Bloomberg::corokafka::tests

