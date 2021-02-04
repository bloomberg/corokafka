#ifndef BLOOMBERG_COROKAFKA_MESSAGE_MOCK_H
#define BLOOMBERG_COROKAFKA_MESSAGE_MOCK_H

#include <corokafka/interface/corokafka_imessage.h>
#include <gmock/gmock.h>

namespace Bloomberg {
namespace corokafka {
namespace mocks {

struct MessageMock : public virtual IMessage
{
    MessageMock(cppkafka::Buffer buffer = cppkafka::Buffer{},
                cppkafka::Message::HeaderListType list = cppkafka::Message::HeaderListType{}) :
        _buffer(std::move(buffer)),
        _headerList(std::move(list))
    {
        using namespace testing;
        ON_CALL(*this, getKeyBuffer())
            .WillByDefault(ReturnRef(_buffer));
        ON_CALL(*this, getHeaderList())
            .WillByDefault(ReturnRef(_headerList));
        ON_CALL(*this, getPayloadBuffer())
            .WillByDefault(ReturnRef(_buffer));
    }
    MOCK_CONST_METHOD0(getHandle, uint64_t());
    MOCK_CONST_METHOD0(getKeyBuffer, const cppkafka::Buffer&());
    MOCK_CONST_METHOD0(getHeaderList, const cppkafka::Message::HeaderListType&());
    MOCK_CONST_METHOD0(getPayloadBuffer, const cppkafka::Buffer&());
    MOCK_CONST_METHOD0(getError, cppkafka::Error());
    MOCK_CONST_METHOD0(getTopic, std::string());
    MOCK_CONST_METHOD0(getPartition, int());
    MOCK_CONST_METHOD0(getOffset, int64_t());
    MOCK_CONST_METHOD0(getTimestamp, std::chrono::milliseconds());
    MOCK_CONST_METHOD0(operatorBool, bool());
    explicit operator bool() const final {
        return operatorBool();
    }
private:
    cppkafka::Buffer _buffer;
    cppkafka::Message::HeaderListType _headerList;
};

}}}

#endif //BLOOMBERG_COROKAFKA_MESSAGE_MOCK_H
