#ifndef BLOOMBERG_COROKAFKA_RECEIVED_MESSAGE_MOCK_H
#define BLOOMBERG_COROKAFKA_RECEIVED_MESSAGE_MOCK_H

#include <corokafka/interface/corokafka_ireceived_message.h>
#include <gmock/gmock.h>

namespace Bloomberg {
namespace corokafka {
namespace mocks {

/*
 * If the received message contains headers, the mock must inherit
 * from ReceivedMessageMock as well as from one or multiple HeaderAccessorMock<H>
 * classes, where H represents each header type.
 */
template <typename HEADER>
struct HeaderAccessorMock : public IHeaderAccessor<HEADER>
{
    explicit HeaderAccessorMock(HEADER header = HEADER{}) :
        _header(std::move(header))
    {
        using namespace testing;
        ON_CALL(*this, getHeaderAt(_))
                .WillByDefault(ReturnRef(_header));
        ON_CALL(Const(*this), getHeaderAt(_))
                .WillByDefault(ReturnRef(_header));
    }
    MOCK_METHOD1_T(getHeaderAt, HEADER&(size_t index));
    MOCK_CONST_METHOD1_T(getHeaderAt, const HEADER&(size_t index));
    MOCK_CONST_METHOD1_T(isHeaderValidAt, bool(size_t index));
private:
    HEADER _header;
};

template<typename KEY, typename PAYLOAD, typename HEADERS>
struct ReceivedMessageMock : public IReceivedMessage<KEY, PAYLOAD, HEADERS>,
                             public MessageMock
{
    explicit ReceivedMessageMock(KEY key = KEY{},
                                 PAYLOAD payload = PAYLOAD{},
                                 HeaderPack headerPack = HeaderPack{}) :
        _key(std::move(key)),
        _payload(std::move(payload)),
        _headerPack(std::move(headerPack))
    {
        //Set default actions
        using namespace testing;
        ON_CALL(*this, getKey())
                .WillByDefault(ReturnRef(_key));
        ON_CALL(Const(*this), getKey())
                .WillByDefault(ReturnRef(_key));
        ON_CALL(*this, getPayload())
                .WillByDefault(ReturnRef(_payload));
        ON_CALL(Const(*this), getPayload())
                .WillByDefault(ReturnRef(_payload));
        ON_CALL(*this, getHeaders())
                .WillByDefault(ReturnRef(_headerPack));
        ON_CALL(Const(*this), getHeaders())
                .WillByDefault(ReturnRef(_headerPack));
    }
    MOCK_CONST_METHOD0_T(skip, bool());
    MOCK_METHOD1_T(setOpaque, void(const void*));
    MOCK_METHOD1_T(commit, cppkafka::Error(void*));
    MOCK_METHOD2_T(commit, cppkafka::Error(ExecMode, void*));
    MOCK_CONST_METHOD0_T(isEof, bool());
    MOCK_CONST_METHOD0_T(getKey, const KEY&());
    MOCK_METHOD0_T(getKey, KEY&());
    MOCK_CONST_METHOD0_T(getPayload, const PAYLOAD&());
    MOCK_METHOD0_T(getPayload, PAYLOAD&());
    MOCK_CONST_METHOD0_T(getHeaders, const HeaderPack&());
    MOCK_METHOD0_T(getHeaders, HeaderPack&());
    
private:
    KEY         _key;
    PAYLOAD     _payload;
    HeaderPack  _headerPack;
};

}}}

#endif //BLOOMBERG_COROKAFKA_RECEIVED_MESSAGE_MOCK_H
