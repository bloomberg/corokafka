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
#ifndef BLOOMBERG_COROKAFKA_RECEIVED_MESSAGE_IMPL_H
#define BLOOMBERG_COROKAFKA_RECEIVED_MESSAGE_IMPL_H

#include <corokafka/corokafka_exception.h>
#include <corokafka/interface/corokafka_ireceived_message.h>

namespace Bloomberg {
namespace corokafka {

//Forward declaration
template <typename KEY, typename PAYLOAD, typename HEADERS>
class ReceivedMessage;

//==========================================================================
//                            OFFSET PERSIST SETTINGS
//==========================================================================
struct OffsetPersistSettings {
    OffsetPersistStrategy _autoOffsetPersistStrategy;
    ExecMode _autoCommitExec;
    bool _autoOffsetPersist;
    bool _autoOffsetPersistOnException;
};

//====================================================================================
//                               RECEIVED MESSAGE
//====================================================================================
template <typename KEY, typename PAYLOAD, typename HEADERS>
class ReceivedMessageImpl : public IReceivedMessage<KEY, PAYLOAD, HEADERS>
{
public:
    using KeyType = KEY;
    using PayloadType = PAYLOAD;
    using HeadersType = HEADERS;
    using HeaderTypes = typename HEADERS::HeaderTypes;
    
    ReceivedMessageImpl(cppkafka::BackoffCommitter& committer,
                        OffsetMap& offsets,
                        cppkafka::Message&& kafkaMessage,
                        boost::any&& key,
                        boost::any&& payload,
                        HeaderPack&& headers,
                        DeserializerError&& error,
                        const OffsetPersistSettings& offsetSettings);
    
    ~ReceivedMessageImpl();
    
    //==========================================================================
    //                         IMessage interface
    //==========================================================================
    const cppkafka::Buffer& getKeyBuffer() const final;
    const cppkafka::Message::HeaderListType& getHeaderList() const final;
    const cppkafka::Buffer& getPayloadBuffer() const final;
    uint64_t getHandle() const final;
    cppkafka::Error getError() const final;
    int getHeaderNumWithError() const;
    std::string getTopic() const final;
    int getPartition() const final;
    int64_t getOffset() const final;
    std::chrono::milliseconds getTimestamp() const final;
    explicit operator bool() const final;
    
    //==========================================================================
    //                         IReceivedMessage interface
    //==========================================================================
    bool skip() const final;
    void setOpaque(const void* opaque) final;
    cppkafka::Error commit(const void* opaque = nullptr) final;
    cppkafka::Error commit(ExecMode execMode,
                           const void* opaque = nullptr) final;
    bool isEof() const final;
    
    //==========================================================================
    //                     Deserialized type accessors
    //==========================================================================
    const KeyType& getKey() const final;
    KeyType& getKey() final;
    const PayloadType& getPayload() const final;
    PayloadType& getPayload() final;
    template <size_t I, std::enable_if_t<I<HeadersType::NumHeaders, int> = 0>
    const typename std::tuple_element<I,HeaderTypes>::type& getHeaderAt() const;
    template <size_t I, std::enable_if_t<I<HeadersType::NumHeaders, int> = 0>
    typename std::tuple_element<I,HeaderTypes>::type& getHeaderAt();
    template <size_t I, std::enable_if_t<I<HeadersType::NumHeaders, int> = 0>
    bool isHeaderValidAt() const;
    const HeaderPack& getHeaders() const final;
    HeaderPack& getHeaders() final;
    
private:
    void validateMessageError() const;
    void validateKeyError() const;
    void validatePayloadError() const;
    void validateHeadersError() const;
    cppkafka::Error doCommit(ExecMode execMode);
    
    cppkafka::BackoffCommitter& _committer;
    OffsetMap&                  _offsets;
    cppkafka::Message           _message;
    boost::any                  _key;
    boost::any                  _payload;
    HeaderPack                  _headers;
    DeserializerError           _error;
    const void*                 _opaque{nullptr};
    bool                        _isPersisted{false};
    OffsetPersistSettings       _offsetSettings;
};

//=========================================================================
//                          IMPLEMENTATIONS
//=========================================================================
template <typename KEY, typename PAYLOAD, typename HEADERS>
ReceivedMessageImpl<KEY,PAYLOAD,HEADERS>::ReceivedMessageImpl(
                                cppkafka::BackoffCommitter& committer,
                                OffsetMap& offsets,
                                cppkafka::Message&& kafkaMessage,
                                boost::any&& key,
                                boost::any&& payload,
                                HeaderPack&& headers,
                                DeserializerError&& error,
                                const OffsetPersistSettings& offsetSettings) :
    _committer(committer),
    _offsets(offsets),
    _message(std::move(kafkaMessage)),
    _key(std::move(key)),
    _payload(std::move(payload)),
    _headers(std::move(headers)),
    _error(std::move(error)),
    _offsetSettings(offsetSettings)
{
    if (_message.get_error() && !_error._error) {
        _error._error = _message.get_error();
        _error.setError(DeserializerError::Source::Kafka);
    }
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
ReceivedMessageImpl<KEY,PAYLOAD,HEADERS>::~ReceivedMessageImpl()
{
    if (!_offsetSettings._autoOffsetPersist || _isPersisted) {
        // auto-persistence is turned off or the offset has been persisted manually
        return;
    }
    if (std::uncaught_exception() && !_offsetSettings._autoOffsetPersistOnException) {
        return; // don't commit if we are being destroyed as the result of an exception
    }
    doCommit(_offsetSettings._autoCommitExec);
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
uint64_t ReceivedMessageImpl<KEY,PAYLOAD,HEADERS>::getHandle() const
{
    return reinterpret_cast<uint64_t>(_message.get_handle());
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
const cppkafka::Buffer& ReceivedMessageImpl<KEY,PAYLOAD,HEADERS>::getKeyBuffer() const
{
    return _message.get_key();
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
const cppkafka::Message::HeaderListType&
ReceivedMessageImpl<KEY,PAYLOAD,HEADERS>::getHeaderList() const
{
    return _message.get_header_list();
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
const cppkafka::Buffer& ReceivedMessageImpl<KEY,PAYLOAD,HEADERS>::getPayloadBuffer() const
{
    return _message.get_payload();
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
cppkafka::Error ReceivedMessageImpl<KEY,PAYLOAD,HEADERS>::getError() const
{
    return _error._error;
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
int ReceivedMessageImpl<KEY,PAYLOAD,HEADERS>::getHeaderNumWithError() const
{
    return _error._headerNum;
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
std::string ReceivedMessageImpl<KEY,PAYLOAD,HEADERS>::getTopic() const
{
    return _message.get_topic();
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
int ReceivedMessageImpl<KEY,PAYLOAD,HEADERS>::getPartition() const
{
    return _message.get_partition();
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
int64_t ReceivedMessageImpl<KEY,PAYLOAD,HEADERS>::getOffset() const
{
    return _message.get_offset();
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
std::chrono::milliseconds ReceivedMessageImpl<KEY,PAYLOAD,HEADERS>::getTimestamp() const
{
    boost::optional<cppkafka::MessageTimestamp> timestamp = _message.get_timestamp();
    if (!timestamp) {
        throw MessageException("Timestamp not set");
    }
    return timestamp.get().get_timestamp();
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
ReceivedMessageImpl<KEY,PAYLOAD,HEADERS>::operator bool() const
{
    return (bool)_message;
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
bool ReceivedMessageImpl<KEY,PAYLOAD,HEADERS>::skip() const
{
    return _error.hasError(DeserializerError::Source::Preprocessor);
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
void ReceivedMessageImpl<KEY,PAYLOAD,HEADERS>::setOpaque(const void* opaque)
{
    _opaque = opaque;
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
cppkafka::Error ReceivedMessageImpl<KEY,PAYLOAD,HEADERS>::commit(const void* opaque)
{
    return commit(_offsetSettings._autoCommitExec, opaque);
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
cppkafka::Error ReceivedMessageImpl<KEY,PAYLOAD,HEADERS>::commit(ExecMode execMode,
                                                                 const void* opaque)
{
    if (!_message) {
        return RD_KAFKA_RESP_ERR__BAD_MSG;
    }
    if (_message.get_error()) {
        //nothing to commit
        return _message.get_error();
    }
    _opaque = opaque;
    if ((_opaque != nullptr) &&
        (_offsetSettings._autoOffsetPersistStrategy == OffsetPersistStrategy::Commit) &&
        _committer.get_consumer().get_configuration().get_offset_commit_callback()) {
        _offsets.insert(cppkafka::TopicPartition(getTopic(), getPartition(), getOffset()), opaque);
    }
    return doCommit(execMode);
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
bool ReceivedMessageImpl<KEY,PAYLOAD,HEADERS>::isEof() const
{
    return _message.is_eof();
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
const KEY& ReceivedMessageImpl<KEY,PAYLOAD,HEADERS>::getKey() const
{
    validateKeyError();
    return boost::any_cast<const KEY&>(_key);
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
KEY& ReceivedMessageImpl<KEY,PAYLOAD,HEADERS>::getKey()
{
    validateKeyError();
    return boost::any_cast<KEY&>(_key);
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
const PAYLOAD& ReceivedMessageImpl<KEY,PAYLOAD,HEADERS>::getPayload() const
{
    validatePayloadError();
    return boost::any_cast<const PAYLOAD&>(_payload);
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
PAYLOAD& ReceivedMessageImpl<KEY,PAYLOAD,HEADERS>::getPayload()
{
    validatePayloadError();
    return boost::any_cast<PAYLOAD&>(_payload);
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
template <size_t I, std::enable_if_t<I<HEADERS::NumHeaders, int>>
const typename std::tuple_element<I,typename HEADERS::HeaderTypes>::type&
ReceivedMessageImpl<KEY,PAYLOAD,HEADERS>::getHeaderAt() const
{
    using Header = typename std::tuple_element<I,typename HEADERS::HeaderTypes>::type;
    validateHeadersError();
    if (!_headers.isValidAt(I)) {
        throw InvalidArgumentException(0, "Header is invalid (empty)");
    }
    return _headers.getAt<Header>(I).value();
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
template <size_t I, std::enable_if_t<I<HEADERS::NumHeaders, int>>
typename std::tuple_element<I,typename HEADERS::HeaderTypes>::type&
ReceivedMessageImpl<KEY,PAYLOAD,HEADERS>::getHeaderAt()
{
    using Header = typename std::tuple_element<I,typename HEADERS::HeaderTypes>::type;
    validateHeadersError();
    if (!_headers.isValidAt(I)) {
        throw InvalidArgumentException(0, "Header is invalid (empty)");
    }
    return _headers.getAt<Header>(I).value();
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
template <size_t I, std::enable_if_t<I<HEADERS::NumHeaders, int>>
bool ReceivedMessageImpl<KEY,PAYLOAD,HEADERS>::isHeaderValidAt() const
{
    return _headers.isValidAt(I);
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
const HeaderPack& ReceivedMessageImpl<KEY,PAYLOAD,HEADERS>::getHeaders() const
{
    validateHeadersError();
    return _headers;
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
HeaderPack& ReceivedMessageImpl<KEY,PAYLOAD,HEADERS>::getHeaders()
{
    validateHeadersError();
    return _headers;
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
void ReceivedMessageImpl<KEY,PAYLOAD,HEADERS>::validateMessageError() const
{
    if (_error.hasError(DeserializerError::Source::Preprocessor)) {
        throw MessageException("Dropped message");
    }
    if (_error.hasError(DeserializerError::Source::Kafka)) {
        throw MessageException("Invalid message");
    }
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
void ReceivedMessageImpl<KEY,PAYLOAD,HEADERS>::validateKeyError() const
{
    validateMessageError();
    if (_error.hasError(DeserializerError::Source::Key)) {
        throw MessageException("Key deserialization error");
    }
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
void ReceivedMessageImpl<KEY,PAYLOAD,HEADERS>::validatePayloadError() const
{
    validateMessageError();
    if (_error.hasError(DeserializerError::Source::Payload)) {
        throw MessageException("Payload deserialization error");
    }
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
void ReceivedMessageImpl<KEY,PAYLOAD,HEADERS>::validateHeadersError() const
{
    validateMessageError();
    if (_error.hasError(DeserializerError::Source::Header)) {
        throw MessageException("Header deserialization error");
    }
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
cppkafka::Error ReceivedMessageImpl<KEY,PAYLOAD,HEADERS>::doCommit(ExecMode execMode)
{
    try {
        if (!_message) {
            return RD_KAFKA_RESP_ERR__BAD_MSG;
        }
        if (_message.get_error()) {
            //nothing to commit
            return _message.get_error();
        }
        if (_offsetSettings._autoOffsetPersistStrategy == OffsetPersistStrategy::Commit) {
            if (execMode == ExecMode::Sync) {
                auto ctx = quantum::local::context();
                if (ctx) {
                    //Start an IO task and wait on it to complete
                    ctx->postAsyncIo([this]()->int{
                        _committer.commit(_message);
                        return 0;
                    })->get(ctx);
                }
                else {
                    _committer.commit(_message);
                }
            }
            else { // async
                _committer.get_consumer().async_commit(_message);
            }
        }
        else { //OffsetPersistStrategy::Store
            _committer.get_consumer().store_offset(_message);
        }
        _isPersisted = true;
    }
    catch (const cppkafka::HandleException& ex) {
        return ex.get_error();
    }
    catch (const cppkafka::ActionTerminatedException& ex) {
        return RD_KAFKA_RESP_ERR__FAIL; //no more retries left
    }
    catch (...) {
        return RD_KAFKA_RESP_ERR_UNKNOWN;
    }
    return {};
}

}
}

#endif //BLOOMBERG_COROKAFKA_RECEIVED_MESSAGE_IMPL_H
