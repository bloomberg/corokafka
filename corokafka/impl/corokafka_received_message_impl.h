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
#include <corokafka/corokafka_exception.h>

namespace Bloomberg {
namespace corokafka {

//====================================================================================
//                               RECEIVED MESSAGE
//====================================================================================
template <typename KEY, typename PAYLOAD, typename HEADERS>
ReceivedMessage<KEY,PAYLOAD,HEADERS>::ReceivedMessage(
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
ReceivedMessage<KEY,PAYLOAD,HEADERS>::~ReceivedMessage()
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
uint64_t ReceivedMessage<KEY,PAYLOAD,HEADERS>::getHandle() const
{
    return reinterpret_cast<uint64_t>(_message.get_handle());
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
const cppkafka::Buffer& ReceivedMessage<KEY,PAYLOAD,HEADERS>::getKeyBuffer() const
{
    return _message.get_key();
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
const IMessage::HeaderListType&
ReceivedMessage<KEY,PAYLOAD,HEADERS>::getHeaderList() const
{
    return _message.get_header_list();
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
const cppkafka::Buffer& ReceivedMessage<KEY,PAYLOAD,HEADERS>::getPayloadBuffer() const
{
    return _message.get_payload();
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
cppkafka::Error ReceivedMessage<KEY,PAYLOAD,HEADERS>::getError() const
{
    return _error._error;
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
int ReceivedMessage<KEY,PAYLOAD,HEADERS>::getHeaderNumWithError() const
{
    return _error._headerNum;
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
std::string ReceivedMessage<KEY,PAYLOAD,HEADERS>::getTopic() const
{
    return _message.get_topic();
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
int ReceivedMessage<KEY,PAYLOAD,HEADERS>::getPartition() const
{
    return _message.get_partition();
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
int64_t ReceivedMessage<KEY,PAYLOAD,HEADERS>::getOffset() const
{
    return _message.get_offset();
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
std::chrono::milliseconds ReceivedMessage<KEY,PAYLOAD,HEADERS>::getTimestamp() const
{
    boost::optional<cppkafka::MessageTimestamp> timestamp = _message.get_timestamp();
    if (!timestamp) {
        throw MessageException("Timestamp not set");
    }
    return timestamp.get().get_timestamp();
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
ReceivedMessage<KEY,PAYLOAD,HEADERS>::operator bool() const
{
    return (bool)_message;
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
bool ReceivedMessage<KEY,PAYLOAD,HEADERS>::skip() const
{
    return _error.hasError(DeserializerError::Source::Preprocessor);
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
void ReceivedMessage<KEY,PAYLOAD,HEADERS>::setOpaque(const void* opaque)
{
    _opaque = opaque;
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
cppkafka::Error ReceivedMessage<KEY,PAYLOAD,HEADERS>::commit(const void* opaque)
{
    return commit(_offsetSettings._autoCommitExec, opaque);
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
cppkafka::Error ReceivedMessage<KEY,PAYLOAD,HEADERS>::commit(ExecMode execMode,
                                                             const void* opaque)
{
    if (!_message) {
        return RD_KAFKA_RESP_ERR__BAD_MSG;
    }
    if (_message.is_eof()) {
        //nothing to commit
        return RD_KAFKA_RESP_ERR__PARTITION_EOF;
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
bool ReceivedMessage<KEY,PAYLOAD,HEADERS>::isEof() const
{
    return _message.is_eof();
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
const KEY& ReceivedMessage<KEY,PAYLOAD,HEADERS>::getKey() const &
{
    validateKeyError();
    return boost::any_cast<const KEY&>(_key);
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
KEY& ReceivedMessage<KEY,PAYLOAD,HEADERS>::getKey() &
{
    validateKeyError();
    return boost::any_cast<KEY&>(_key);
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
KEY&& ReceivedMessage<KEY,PAYLOAD,HEADERS>::getKey() &&
{
    validateKeyError();
    return boost::any_cast<KEY&&>(_key);
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
const PAYLOAD& ReceivedMessage<KEY,PAYLOAD,HEADERS>::getPayload() const &
{
    validatePayloadError();
    return boost::any_cast<const PAYLOAD&>(_payload);
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
PAYLOAD& ReceivedMessage<KEY,PAYLOAD,HEADERS>::getPayload() &
{
    validatePayloadError();
    return boost::any_cast<PAYLOAD&>(_payload);
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
PAYLOAD&& ReceivedMessage<KEY,PAYLOAD,HEADERS>::getPayload() &&
{
    validatePayloadError();
    return boost::any_cast<PAYLOAD&&>(_payload);
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
template <size_t I, std::enable_if_t<I<HEADERS::NumHeaders, int>>
const typename std::tuple_element<I,typename HEADERS::HeaderTypes>::type&
ReceivedMessage<KEY,PAYLOAD,HEADERS>::getHeaderAt() const &
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
ReceivedMessage<KEY,PAYLOAD,HEADERS>::getHeaderAt() &
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
typename std::tuple_element<I,typename HEADERS::HeaderTypes>::type&&
ReceivedMessage<KEY,PAYLOAD,HEADERS>::getHeaderAt() &&
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
bool ReceivedMessage<KEY,PAYLOAD,HEADERS>::isHeaderValidAt() const
{
    return _headers.isValidAt(I);
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
const HeaderPack& ReceivedMessage<KEY,PAYLOAD,HEADERS>::getHeaders() const &
{
    validateHeadersError();
    return _headers;
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
HeaderPack& ReceivedMessage<KEY,PAYLOAD,HEADERS>::getHeaders() &
{
    validateHeadersError();
    return _headers;
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
HeaderPack&& ReceivedMessage<KEY,PAYLOAD,HEADERS>::getHeaders() &&
{
    validateHeadersError();
    return std::move(_headers);
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
void ReceivedMessage<KEY,PAYLOAD,HEADERS>::validateMessageError() const
{
    if (_error.hasError(DeserializerError::Source::Preprocessor)) {
        throw MessageException("Dropped message");
    }
    if (_error.hasError(DeserializerError::Source::Kafka)) {
        throw MessageException("Invalid message");
    }
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
void ReceivedMessage<KEY,PAYLOAD,HEADERS>::validateKeyError() const
{
    validateMessageError();
    if (_error.hasError(DeserializerError::Source::Key)) {
        throw MessageException("Key deserialization error");
    }
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
void ReceivedMessage<KEY,PAYLOAD,HEADERS>::validatePayloadError() const
{
    validateMessageError();
    if (_error.hasError(DeserializerError::Source::Payload)) {
        throw MessageException("Payload deserialization error");
    }
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
void ReceivedMessage<KEY,PAYLOAD,HEADERS>::validateHeadersError() const
{
    validateMessageError();
    if (_error.hasError(DeserializerError::Source::Header)) {
        throw MessageException("Header deserialization error");
    }
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
cppkafka::Error ReceivedMessage<KEY,PAYLOAD,HEADERS>::doCommit(ExecMode execMode)
{
    try {
        if (!_message) {
            return RD_KAFKA_RESP_ERR__BAD_MSG;
        }
        if (_message.is_eof()) {
            //nothing to commit
            return RD_KAFKA_RESP_ERR__PARTITION_EOF;
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
