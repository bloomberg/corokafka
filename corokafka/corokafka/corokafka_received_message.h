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

#include <corokafka/corokafka_message.h>
#include <corokafka/corokafka_header_pack.h>
#include <boost/any.hpp>

namespace Bloomberg {
namespace corokafka {

//==========================================================================
//                     Offset settings
//==========================================================================
struct OffsetPersistSettings {
    bool                    _autoOffsetPersist;
    bool                    _autoOffsetPersistOnException;
    OffsetPersistStrategy   _autoOffsetPersistStrategy;
    ExecMode                _autoCommitExec;
};

template <typename K, typename P>
class ConcreteReceiver;

/**
 * @brief class ReceivedMessage.
 * @tparam K Header type
 * @tparam P Payload type
 */
template <typename K, typename P>
class ReceivedMessage : public IMessage
{
    friend class ConcreteReceiver<K,P>;
public:
    /**
     * Copy ctor's and assignment operators
     */
    ReceivedMessage(const ReceivedMessage&) = delete;
    ReceivedMessage(ReceivedMessage&& rhs) = default;
    ReceivedMessage& operator=(const ReceivedMessage&) = delete;
    ReceivedMessage& operator=(ReceivedMessage&& rhs) = default;
    /**
     * @brief Destructor. Calls commit() if it has not been called previously. Commit will optionally pass
     *        the opaque application pointer set with setOpaque().
     */
    ~ReceivedMessage();
    
    //==========================================================================
    //                         IMessage interface
    //==========================================================================
    /**
     * @sa IMessage::getKeyBuffer
     */
    const cppkafka::Buffer& getKeyBuffer() const final;
    /**
     * @sa IMessage::getHeaderList
     * @note This functions returns a const reference to the Kafka raw headers.
     *       To get or modify the deserialized headers, use the getHeaders() API.
     */
    const HeaderListType& getHeaderList() const final;
    /**
     * @sa IMessage::getPayloadBuffer
     */
    const cppkafka::Buffer& getPayloadBuffer() const final;
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
    void setOpaque(const void* opaque);
    /**
     * @brief Commits message with the retry strategy specified in the config for this topic.
     * @param opaque Application-specific pointer which will be returned inside the offset commit callback.
     *               Passing an opaque pointer only works when 'internal.consumer.offset.persist.strategy=commit'
     * @return Error object. If the number of retries reaches 0, the error contains RD_KAFKA_RESP_ERR__FAIL.
     * @remark This call is blocking until it succeeds or the last retry failed.
     * @remark If commit() is not called explicitly, it will be called by ~ReceivedMessage() if
     *         'internal.consumer.auto.offset.persist=true'.
     */
    cppkafka::Error commit(const void* opaque = nullptr);
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
    const K& getKey() const &;
    K& getKey() &;
    K&& getKey() &&;
    /**
     * @brief Get the payload object reference
     * @return The reference
     */
    const P& getPayload() const &;
    P& getPayload() &;
    P&& getPayload() &&;
    /**
     * @brief Get the header pack
     * @return The reference to the pack
     */
    const HeaderPack& getHeaders() const &;
    HeaderPack& getHeaders() &;
    HeaderPack&& getHeaders() &&;
private:
    ReceivedMessage(cppkafka::BackoffCommitter& committer,
                    OffsetMap& offsets,
                    cppkafka::Message&& kafkaMessage,
                    K&& key,
                    P&& payload,
                    HeaderPack&& headers,
                    DeserializerError&& error,
                    const OffsetPersistSettings& offsetSettings);
    void validateKeyError() const;
    void validatePayloadError() const;
    void validateHeadersError() const;
    cppkafka::Error doCommit();
    
    cppkafka::BackoffCommitter&           _committer;
    OffsetMap&                  _offsets;
    cppkafka::Message           _message;
    K                           _key;
    P                           _payload;
    HeaderPack                  _headers;
    DeserializerError           _error;
    const void*                 _opaque{nullptr};
    bool                        _isPersisted{false};
    OffsetPersistSettings       _offsetSettings;
};

}
}

#include <corokafka/impl/corokafka_received_message_impl.h>

#endif //BLOOMBERG_COROKAFKA_RECEIVED_MESSAGE_H
