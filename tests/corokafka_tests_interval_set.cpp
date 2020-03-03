#include <corokafka/utils/corokafka_interval_set.h>
#include <gtest/gtest.h>
#include <gmock/gmock.h>

namespace Bloomberg {
namespace corokafka {
namespace tests {

using Irt = IntervalSet<int>::InsertReturnType;

class IntervalFixture : public ::testing::Test
{
public:
    IntervalFixture()
    {
        _ranges.insert(Range<int>(0,5));
        _ranges.insert(Range<int>(10,15));
        _ranges.insert(Range<int>(20,25));
    }
protected:
    IntervalSet<int> _ranges;
};

//=========================================================================
//                          OSTREAM
//=========================================================================
TEST_F(IntervalFixture, OstreamForIntervalSet)
{
    std::ostringstream ostream;
    ostream << _ranges;
    ASSERT_STREQ("{\"intervals\":[{\"begin\":0,\"end\":5},{\"begin\":10,\"end\":15},{\"begin\":20,\"end\":25}]}",
                 ostream.str().c_str());
}

//=========================================================================
//                          POINTS
//=========================================================================
TEST_F(IntervalFixture, InsertPointNoCollision)
{
    int val = 8;
    Irt irt = _ranges.insert(Point<int>(val));
    ASSERT_TRUE((*irt.first == Point<int>(val)));
    ASSERT_TRUE(irt.second);
    ASSERT_EQ(4, _ranges.size());
    ASSERT_EQ(19, _ranges.count());
}

TEST_F(IntervalFixture, InsertPointAndAbsorb)
{
    int val = 4;
    Irt irt = _ranges.insert(Point<int>(val));
    ASSERT_TRUE((*irt.first == Range<int>(0,5)));
    ASSERT_FALSE(irt.second);
    ASSERT_EQ(3, _ranges.size());
    ASSERT_EQ(18, _ranges.count());
}

TEST_F(IntervalFixture, InsertPointOnRightEdge)
{
    int val = 6;
    Irt irt = _ranges.insert(Point<int>(val));
    ASSERT_TRUE((*irt.first == Range<int>(0,val)));
    ASSERT_TRUE(irt.second);
    ASSERT_EQ(3, _ranges.size());
    ASSERT_EQ(19, _ranges.count());
}

TEST_F(IntervalFixture, InsertPointOnLeftEdgeAtBegin)
{
    int val = -1;
    Irt irt = _ranges.insert(Point<int>(val));
    ASSERT_TRUE((*irt.first == Range<int>(val,5)));
    ASSERT_TRUE(irt.second);
    ASSERT_EQ(3, _ranges.size());
    ASSERT_EQ(19, _ranges.count());
}

TEST_F(IntervalFixture, InsertPointOnRightEdgeAtEnd)
{
    int val = 26;
    Irt irt = _ranges.insert(Point<int>(val));
    ASSERT_TRUE((*irt.first == Range<int>(20,val)));
    ASSERT_TRUE(irt.second);
    ASSERT_EQ(3, _ranges.size());
    ASSERT_EQ(19, _ranges.count());
}

TEST_F(IntervalFixture, InsertPointNoCollisionAtEnd)
{
    int val = 27;
    Irt irt = _ranges.insert(Point<int>(val));
    ASSERT_TRUE((*irt.first == Point<int>(val)));
    ASSERT_TRUE(irt.second);
    ASSERT_EQ(4, _ranges.size());
    ASSERT_EQ(19, _ranges.count());
}

//=========================================================================
//                           RANGES
//=========================================================================
TEST_F(IntervalFixture, InsertRangeNoCollisionAtBegin)
{
    Irt irt = _ranges.insert(Range<int>(-5,-2));
    ASSERT_TRUE((*irt.first == Range<int>(-5,-2)));
    ASSERT_TRUE(irt.second);
    ASSERT_EQ(4, _ranges.size());
    ASSERT_EQ(22, _ranges.count());
}

TEST_F(IntervalFixture, InsertRangeNoCollisionAtEnd)
{
    Irt irt = _ranges.insert(Range<int>(28,29));
    ASSERT_TRUE((*irt.first == Range<int>(28,29)));
    ASSERT_TRUE(irt.second);
    ASSERT_EQ(4, _ranges.size());
    ASSERT_EQ(20, _ranges.count());
}

TEST_F(IntervalFixture, InsertRangeNoCollisionInMiddle)
{
    Irt irt = _ranges.insert(Range<int>(7,8));
    ASSERT_TRUE((*irt.first == Range<int>(7,8)));
    ASSERT_TRUE(irt.second);
    ASSERT_EQ(4, _ranges.size());
    ASSERT_EQ(20, _ranges.count());
}

TEST_F(IntervalFixture, InsertRangeOnLeftEdgeAtBegin)
{
    Irt irt = _ranges.insert(Range<int>(-5,-1));
    ASSERT_TRUE((*irt.first == Range<int>(-5,5)));
    ASSERT_TRUE(irt.second);
    ASSERT_EQ(3, _ranges.size());
    ASSERT_EQ(23, _ranges.count());
}

TEST_F(IntervalFixture, InsertRangeOnRightEdgeAtEnd)
{
    Irt irt = _ranges.insert(Range<int>(26,29));
    ASSERT_TRUE((*irt.first == Range<int>(20,29)));
    ASSERT_TRUE(irt.second);
    ASSERT_EQ(3, _ranges.size());
    ASSERT_EQ(22, _ranges.count());
}

TEST_F(IntervalFixture, InsertRangeOnRightEdgeInMiddle)
{
    Irt irt = _ranges.insert(Range<int>(16,18));
    ASSERT_TRUE((*irt.first == Range<int>(10,18)));
    ASSERT_TRUE(irt.second);
    ASSERT_EQ(3, _ranges.size());
    ASSERT_EQ(21, _ranges.count());
}

TEST_F(IntervalFixture, InsertRangeBridgeGap)
{
    Irt irt = _ranges.insert(Range<int>(16,19));
    ASSERT_TRUE((*irt.first == Range<int>(10,25)));
    ASSERT_TRUE(irt.second);
    ASSERT_EQ(2, _ranges.size());
    ASSERT_EQ(22, _ranges.count());
}

TEST_F(IntervalFixture, InsertRangeBridgeGapWithOverlap)
{
    Irt irt = _ranges.insert(Range<int>(14,22));
    ASSERT_TRUE((*irt.first == Range<int>(10,25)));
    ASSERT_TRUE(irt.second);
    ASSERT_EQ(2, _ranges.size());
    ASSERT_EQ(22, _ranges.count());
}

TEST_F(IntervalFixture, InsertRangeLeftEdgeOverlap)
{
    Irt irt = _ranges.insert(Range<int>(-5,2));
    ASSERT_TRUE((*irt.first == Range<int>(-5,5)));
    ASSERT_TRUE(irt.second);
    ASSERT_EQ(3, _ranges.size());
    ASSERT_EQ(23, _ranges.count());
}

TEST_F(IntervalFixture, InsertRangeLeftEdgeAbsorbOne)
{
    Irt irt = _ranges.insert(Range<int>(-5,7));
    ASSERT_TRUE((*irt.first == Range<int>(-5,7)));
    ASSERT_TRUE(irt.second);
    ASSERT_EQ(3, _ranges.size());
    ASSERT_EQ(25, _ranges.count());
}

TEST_F(IntervalFixture, InsertRangeAbsorbTwo)
{
    Irt irt = _ranges.insert(Range<int>(-5,18));
    ASSERT_TRUE((*irt.first == Range<int>(-5,18)));
    ASSERT_TRUE(irt.second);
    ASSERT_EQ(2, _ranges.size());
    ASSERT_EQ(30, _ranges.count());
}

TEST_F(IntervalFixture, InsertRangeAbsorbAll)
{
    Irt irt = _ranges.insert(Range<int>(-5,30));
    ASSERT_TRUE((*irt.first == Range<int>(-5,30)));
    ASSERT_TRUE(irt.second);
    ASSERT_EQ(1, _ranges.size());
    ASSERT_EQ(36, _ranges.count());
}

TEST_F(IntervalFixture, InsertRangeAbsorbMiddle)
{
    Irt irt = _ranges.insert(Range<int>(8,18));
    ASSERT_TRUE((*irt.first == Range<int>(8,18)));
    ASSERT_TRUE(irt.second);
    ASSERT_EQ(3, _ranges.size());
    ASSERT_EQ(23, _ranges.count());
}

}}}

