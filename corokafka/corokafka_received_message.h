/*
** Copyright 2019 Bloomberg Finance L.P.
**
** Licensed under the Apache License, Version 2.0 (the "License");
** you may not use this file except in compliance with the License.
** You may obtain a copy of the License at
**
**     http://www.apache.org/licenses/LICENSE-2.0
**
** Unless required by applicable law or agreed to in writing, software
** distributed under the License is distributed on an "AS IS" BASIS,
** WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
** See the License for the specific language governing permissions and
** limitations under the License.
*/
#ifndef BLOOMBERG_COROKAFKA_RECEIVED_MESSAGE_H
#define BLOOMBERG_COROKAFKA_RECEIVED_MESSAGE_H

#include <corokafka/interface/corokafka_ireceived_message.h>
#include <corokafka/impl/corokafka_received_message_impl.h>
#include <corokafka/interface/corokafka_impl.h>
#include <corokafka/corokafka_header_pack.h>
#include <corokafka/corokafka_topic.h>
#include <boost/any.hpp>

namespace Bloomberg {
namespace corokafka {

template<typename TOPIC>
class ConcreteReceiver;

/**
 * @brief class ReceivedMessage.
 * @tparam KEY Header type
 * @tparam PAYLOAD Payload type
 * @tparam HEADERS Headers type i.e. Headers<...>
 */
template<typename KEY, typename PAYLOAD, typename HEADERS = Headers<>>
class ReceivedMessage : public Impl<IReceivedMessage<KEY, PAYLOAD, HEADERS>>
{
    using Interface = IReceivedMessage<KEY, PAYLOAD, HEADERS>;
    using Concrete = ReceivedMessageImpl<KEY, PAYLOAD, HEADERS>;
public:
    using KeyType = KEY;
    using PayloadType = PAYLOAD;
    using HeadersType = HEADERS;
    using HeaderTypes = typename HEADERS::HeaderTypes;
    
    /**
     * Copy ctor's and assignment operators
     */
    ReceivedMessage(const ReceivedMessage &) = delete;
    ReceivedMessage(ReceivedMessage &&rhs) = default;
    ReceivedMessage &operator=(const ReceivedMessage &) = delete;
    ReceivedMessage &operator=(ReceivedMessage &&rhs) = default;
    /**
     * @brief Destructor. Calls commit() if it has not been called previously. Commit will optionally pass
     *        the opaque application pointer set with setOpaque().
     */
    ~ReceivedMessage() = default;
    
    //==========================================================================
    //                         IMessage interface
    //==========================================================================
    /**
     * @sa IMessage::getKeyBuffer
     */
    const cppkafka::Buffer &getKeyBuffer() const final;
    /**
     * @sa IMessage::getHeaderList
     * @note This functions returns a const reference to the Kafka raw headers.
     *       To get or modify the de-serialized headers, use the getHeaders() API.
     */
    const cppkafka::Message::HeaderListType &getHeaderList() const final;
    /**
     * @sa IMessage::getPayloadBuffer
     */
    const cppkafka::Buffer &getPayloadBuffer() const final;
    /**
     * @sa IMessage::getHandle
     */
    uint64_t getHandle() const final;
    /**
     * @sa IMessage::getError
     */
    cppkafka::Error getError() const final;
    /**
     * @brief Returns the number of the header which contains an error.
     * @return The header number.
     */
    int getHeaderNumWithError() const;
    /**
     * @sa IMessage::getTopic
     */
    std::string getTopic() const final;
    /**
     * @sa IMessage::getPartition
     */
    int getPartition() const final;
    /**
     * @sa IMessage::getOffset
     */
    int64_t getOffset() const final;
    /**
     * @sa IMessage::getTimestamp
     */
    std::chrono::milliseconds getTimestamp() const final;
    /**
     * @sa IMessage::operator bool
     */
    explicit operator bool() const final;
    /**
     * @brief Indicates if this message should be skipped.
     * @note This is a message which was flagged to be dropped by the 'preprocessor' callback.
     * @warning Processing such a message can result in a fatal failure!
     */
    bool skip() const;
    /**
     * @brief Set an application-specific pointer which will be returned inside the offset commit callback.
     *        This is used when commit() is called automatically by the destructor.
     * @param opaque Application-specific pointer.
     * @warning This library does not extend the lifetime of this pointer. The application must ensure the
     *          memory location is still valid until the callback is invoked.
     */
    void setOpaque(const void *opaque);
    /**
     * @brief Commits message with the retry strategy specified in the config for this topic.
     * @param opaque Application-specific pointer which will be returned inside the offset commit callback.
     *               Passing an opaque pointer only works when 'internal.consumer.offset.persist.strategy=commit'.
     * @param execMode If specified, overrides the 'internal.consumer.commit.exec' setting.
     * @return Error object. If the number of retries reaches 0, the error contains RD_KAFKA_RESP_ERR__FAIL.
     * @remark This call is blocking until it succeeds or the last retry failed.
     * @remark If commit() is not called explicitly, it will be called by ~ReceivedMessage() if
     *         'internal.consumer.auto.offset.persist=true'.
     * @remark This will actually commit (or store) the message offset + 1.
     */
    cppkafka::Error commit(const void *opaque = nullptr);
    cppkafka::Error commit(ExecMode execMode,
                           const void *opaque = nullptr);
    /**
     * @brief Helper function to indicate if the message error is an EOF.
     * @return True if EOF was encountered for the partition, False otherwise.
     */
    bool isEof() const;
    
    //==========================================================================
    //                     Deserialized type accessors
    //==========================================================================
    /**
     * @brief Get the key object reference
     * @return The reference
     */
    const KeyType& getKey() const;
    KeyType& getKey();
    
    /**
     * @brief Get the payload object reference
     * @return The reference
     */
    const PayloadType& getPayload() const;
    PayloadType& getPayload();
    
    /**
     * @brief Get the header at the specified position (type-safe)
     * @return The header
     * @note The position specified should match the type in the HEADERS template argument.
     */
    template<size_t I, std::enable_if_t<I < HeadersType::NumHeaders, int> = 0>
    const typename std::tuple_element<I, HeaderTypes>::type& getHeaderAt() const;
    template<size_t I, std::enable_if_t<I < HeadersType::NumHeaders, int> = 0>
    typename std::tuple_element<I, HeaderTypes>::type& getHeaderAt();
    /**
     * @brief Determine if the header at the specified position is valid
     * @tparam I The position of the header
     * @return True if it's valid.
     */
    template<size_t I, std::enable_if_t<I < HeadersType::NumHeaders, int> = 0>
    bool isHeaderValidAt() const;
    /**
     * @brief Get the header pack
     * @return The reference to the pack
     * @warning Accessing headers via the HeaderPack is not type-safe. Casting to incorrect type may
     *          to undefined behavior or 'std::bad_cast' being thrown.
     */
    const HeaderPack& getHeaders() const;
    HeaderPack& getHeaders();
    
    /**
     * @brief For mocking only via dependency injection
     */
    using ImplType = Impl<Interface>;
    using ImplType::Impl;

private:
    friend class ConcreteReceiver<Topic<KEY,PAYLOAD,HEADERS>>;
    
    ReceivedMessage(cppkafka::BackoffCommitter &committer,
                    OffsetMap &offsets,
                    cppkafka::Message &&kafkaMessage,
                    boost::any &&key,
                    boost::any &&payload,
                    HeaderPack &&headers,
                    DeserializerError &&error,
                    const OffsetPersistSettings &offsetSettings);
    
    ReceivedMessageImpl<KEY, PAYLOAD, HEADERS>* _concretePtr{nullptr};
};

//=========================================================================
//                          IMPLEMENTATIONS
//=========================================================================
template<typename KEY, typename PAYLOAD, typename HEADERS>
ReceivedMessage<KEY, PAYLOAD, HEADERS>::ReceivedMessage(
                    cppkafka::BackoffCommitter &committer,
                    OffsetMap &offsets,
                    cppkafka::Message &&kafkaMessage,
                    boost::any &&key,
                    boost::any &&payload,
                    HeaderPack &&headers,
                    DeserializerError &&error,
                    const OffsetPersistSettings &offsetSettings) :
    ImplType(std::make_shared<Concrete>(committer,
                                        offsets,
                                        std::move(kafkaMessage),
                                        std::move(key),
                                        std::move(payload),
                                        std::move(headers),
                                        std::move(error),
                                        offsetSettings)),
    _concretePtr(static_cast<Concrete*>(this->impl().get()))
{
}

template<typename KEY, typename PAYLOAD, typename HEADERS>
uint64_t ReceivedMessage<KEY, PAYLOAD, HEADERS>::getHandle() const {
    return this->impl()->getHandle();
}

template<typename KEY, typename PAYLOAD, typename HEADERS>
const cppkafka::Buffer &ReceivedMessage<KEY, PAYLOAD, HEADERS>::getKeyBuffer() const {
    return this->impl()->getKeyBuffer();
}

template<typename KEY, typename PAYLOAD, typename HEADERS>
const cppkafka::Message::HeaderListType &
ReceivedMessage<KEY, PAYLOAD, HEADERS>::getHeaderList() const {
    return this->impl()->getHeaderList();
}

template<typename KEY, typename PAYLOAD, typename HEADERS>
const cppkafka::Buffer &ReceivedMessage<KEY, PAYLOAD, HEADERS>::getPayloadBuffer() const {
    return this->impl()->getPayloadBuffer();
}

template<typename KEY, typename PAYLOAD, typename HEADERS>
cppkafka::Error ReceivedMessage<KEY, PAYLOAD, HEADERS>::getError() const {
    return this->impl()->getError();
}

template<typename KEY, typename PAYLOAD, typename HEADERS>
int ReceivedMessage<KEY, PAYLOAD, HEADERS>::getHeaderNumWithError() const {
    return this->impl()->getHeaderNumWithError();
}

template<typename KEY, typename PAYLOAD, typename HEADERS>
std::string ReceivedMessage<KEY, PAYLOAD, HEADERS>::getTopic() const {
    return this->impl()->getTopic();
}

template<typename KEY, typename PAYLOAD, typename HEADERS>
int ReceivedMessage<KEY, PAYLOAD, HEADERS>::getPartition() const {
    return this->impl()->getPartition();
}

template<typename KEY, typename PAYLOAD, typename HEADERS>
int64_t ReceivedMessage<KEY, PAYLOAD, HEADERS>::getOffset() const {
    return this->impl()->getOffset();
}

template<typename KEY, typename PAYLOAD, typename HEADERS>
std::chrono::milliseconds ReceivedMessage<KEY, PAYLOAD, HEADERS>::getTimestamp() const {
    return this->impl()->getTimestamp();
}

template<typename KEY, typename PAYLOAD, typename HEADERS>
ReceivedMessage<KEY, PAYLOAD, HEADERS>::operator bool() const {
    return this->impl()->operator bool();
}

template<typename KEY, typename PAYLOAD, typename HEADERS>
bool ReceivedMessage<KEY, PAYLOAD, HEADERS>::skip() const {
    return this->impl()->skip();
}

template<typename KEY, typename PAYLOAD, typename HEADERS>
void ReceivedMessage<KEY, PAYLOAD, HEADERS>::setOpaque(const void *opaque) {
    this->impl()->setOpaque(opaque);
}

template<typename KEY, typename PAYLOAD, typename HEADERS>
cppkafka::Error ReceivedMessage<KEY, PAYLOAD, HEADERS>::commit(const void *opaque) {
    return this->impl()->commit(opaque);
}

template<typename KEY, typename PAYLOAD, typename HEADERS>
cppkafka::Error ReceivedMessage<KEY, PAYLOAD, HEADERS>::commit(ExecMode execMode,
                                                               const void *opaque) {
    return this->impl()->commit(execMode, opaque);
}

template<typename KEY, typename PAYLOAD, typename HEADERS>
bool ReceivedMessage<KEY, PAYLOAD, HEADERS>::isEof() const {
    return this->impl()->isEof();
}

template<typename KEY, typename PAYLOAD, typename HEADERS>
const KEY& ReceivedMessage<KEY, PAYLOAD, HEADERS>::getKey() const {
    return const_cast<const Interface&>(*this->impl()).getKey();
}

template<typename KEY, typename PAYLOAD, typename HEADERS>
KEY& ReceivedMessage<KEY, PAYLOAD, HEADERS>::getKey() {
    return const_cast<Interface&>(*this->impl()).getKey();
}

template<typename KEY, typename PAYLOAD, typename HEADERS>
const PAYLOAD& ReceivedMessage<KEY, PAYLOAD, HEADERS>::getPayload() const {
    return const_cast<const Interface&>(*this->impl()).getPayload();
}

template<typename KEY, typename PAYLOAD, typename HEADERS>
PAYLOAD& ReceivedMessage<KEY, PAYLOAD, HEADERS>::getPayload() {
    return const_cast<Interface&>(*this->impl()).getPayload();
}

template<typename KEY, typename PAYLOAD, typename HEADERS>
template<size_t I, std::enable_if_t<I < HEADERS::NumHeaders, int>>
const typename std::tuple_element<I, typename HEADERS::HeaderTypes>::type &
ReceivedMessage<KEY, PAYLOAD, HEADERS>::getHeaderAt() const {
    if (_concretePtr) {
        return const_cast<const Concrete&>(*_concretePtr).template getHeaderAt<I>();
    }
    //Mock access only
    using Header = typename std::tuple_element<I, HeaderTypes>::type;
    return std::dynamic_pointer_cast<IHeaderAccessor<Header>>(this->impl())->getHeaderAt(I);
}

template<typename KEY, typename PAYLOAD, typename HEADERS>
template<size_t I, std::enable_if_t<I < HEADERS::NumHeaders, int>>
typename std::tuple_element<I, typename HEADERS::HeaderTypes>::type &
ReceivedMessage<KEY, PAYLOAD, HEADERS>::getHeaderAt() {
    if (_concretePtr) {
        return const_cast<Concrete&>(*_concretePtr).template getHeaderAt<I>();
    }
    //Mock access only
    using Header = typename std::tuple_element<I, HeaderTypes>::type;
    return std::dynamic_pointer_cast<IHeaderAccessor<Header>>(this->impl())->getHeaderAt(I);
}

template<typename KEY, typename PAYLOAD, typename HEADERS>
template<size_t I, std::enable_if_t<I < HEADERS::NumHeaders, int>>
bool ReceivedMessage<KEY, PAYLOAD, HEADERS>::isHeaderValidAt() const {
    if (_concretePtr) {
        return _concretePtr->template isHeaderValidAt<I>();
    }
    //Mock access only
    using Header = typename std::tuple_element<I, HeaderTypes>::type;
    return std::dynamic_pointer_cast<IHeaderAccessor<Header>>(this->impl())->isHeaderValidAt(I);
}

template<typename KEY, typename PAYLOAD, typename HEADERS>
const HeaderPack &ReceivedMessage<KEY, PAYLOAD, HEADERS>::getHeaders() const {
    return const_cast<const Interface&>(*this->impl()).getHeaders();
}

template<typename KEY, typename PAYLOAD, typename HEADERS>
HeaderPack &ReceivedMessage<KEY, PAYLOAD, HEADERS>::getHeaders() {
    return const_cast<Interface&>(*this->impl()).getHeaders();
}

}}

#endif //BLOOMBERG_COROKAFKA_RECEIVED_MESSAGE_H
