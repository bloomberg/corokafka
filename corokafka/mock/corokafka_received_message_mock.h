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
    MOCK_CONST_METHOD1_T(getHeaderAt, HEADER&(size_t index));
    MOCK_CONST_METHOD1_T(isHeaderValidAt, bool(size_t index));
};

template<typename KEY, typename PAYLOAD, typename HEADERS>
struct ReceivedMessageMock : public IReceivedMessage<KEY, PAYLOAD, HEADERS>,
                             public MessageMock
{
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
};

}}}

#endif //BLOOMBERG_COROKAFKA_RECEIVED_MESSAGE_MOCK_H
