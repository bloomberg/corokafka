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
        _error._source |= (uint8_t)DeserializerError::Source::Kafka;
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
    doCommit();
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
uint64_t ReceivedMessage<KEY,PAYLOAD,HEADERS>::getHandle() const
{
    return (uint64_t)_message.get_handle();
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
        throw std::runtime_error("Timestamp not set");
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
    return _error.isPreprocessorError();
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
void ReceivedMessage<KEY,PAYLOAD,HEADERS>::setOpaque(const void* opaque)
{
    _opaque = opaque;
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
cppkafka::Error ReceivedMessage<KEY,PAYLOAD,HEADERS>::commit(const void* opaque)
{
    _opaque = opaque;
    if ((_opaque != nullptr) &&
        (_offsetSettings._autoOffsetPersistStrategy == OffsetPersistStrategy::Commit) &&
        _committer.get_consumer().get_configuration().get_offset_commit_callback()) {
        _offsets.insert(cppkafka::TopicPartition(getTopic(), getPartition(), getOffset()), opaque);
    }
    return doCommit();
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
template <size_t I>
const typename std::tuple_element<I,typename HEADERS::HeaderTypes>::type&
ReceivedMessage<KEY,PAYLOAD,HEADERS>::getHeader() const &
{
    using Header = typename std::tuple_element<I,typename HEADERS::HeaderTypes>::type;
    validateHeadersError();
    if (!_headers.isValidAt(I)) {
        throw std::runtime_error("Header is invalid (empty)");
    }
    return _headers.getAt<Header>(I).value();
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
template <size_t I>
typename std::tuple_element<I,typename HEADERS::HeaderTypes>::type&
ReceivedMessage<KEY,PAYLOAD,HEADERS>::getHeader() &
{
    using Header = typename std::tuple_element<I,typename HEADERS::HeaderTypes>::type;
    validateHeadersError();
    if (!_headers.isValidAt(I)) {
        throw std::runtime_error("Header is invalid (empty)");
    }
    return _headers.getAt<Header>(I).value();
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
template <size_t I>
typename std::tuple_element<I,typename HEADERS::HeaderTypes>::type&&
ReceivedMessage<KEY,PAYLOAD,HEADERS>::getHeader() &&
{
    using Header = typename std::tuple_element<I,typename HEADERS::HeaderTypes>::type;
    validateHeadersError();
    if (!_headers.isValidAt(I)) {
        throw std::runtime_error("Header is invalid (empty)");
    }
    return _headers.getAt<Header>(I).value();
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
    if (_error.isPreprocessorError()) {
        throw std::runtime_error("Dropped message");
    }
    if (_error.isKafkaError()) {
        throw std::runtime_error("Invalid message");
    }
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
void ReceivedMessage<KEY,PAYLOAD,HEADERS>::validateKeyError() const
{
    validateMessageError();
    if (_error.isKeyError()) {
        throw std::runtime_error("Key deserialization error");
    }
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
void ReceivedMessage<KEY,PAYLOAD,HEADERS>::validatePayloadError() const
{
    validateMessageError();
    if (_error.isPayloadError()) {
        throw std::runtime_error("Payload deserialization error");
    }
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
void ReceivedMessage<KEY,PAYLOAD,HEADERS>::validateHeadersError() const
{
    validateMessageError();
    if (_error.isHeaderError()) {
        throw std::runtime_error("Header deserialization error");
    }
}

template <typename KEY, typename PAYLOAD, typename HEADERS>
cppkafka::Error ReceivedMessage<KEY,PAYLOAD,HEADERS>::doCommit()
{
    try {
        if (!_message) {
            return RD_KAFKA_RESP_ERR__BAD_MSG;
        }
        if (_message.is_eof()) {
            //nothing to commit
            return {};
        }
        if (_offsetSettings._autoOffsetPersistStrategy == OffsetPersistStrategy::Commit) {
            if (_offsetSettings._autoCommitExec== ExecMode::Sync) {
                _committer.commit(_message);
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
