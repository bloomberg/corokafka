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
template <typename K, typename P>
ReceivedMessage<K,P>::ReceivedMessage(
                                BackoffCommitter& committer,
                                OffsetMap& offsets,
                                Message&& kafkaMessage,
                                K&& key,
                                P&& payload,
                                HeaderPack&& headers,
                                DeserializerError&& error,
                                const OffsetPersistSettings& offsetSettings) :
    _committer(committer),
    _offsets(offsets),
    _message(std::move(kafkaMessage)),
    _key(std::forward<K>(key)),
    _payload(std::forward<P>(payload)),
    _headers(std::move(headers)),
    _error(std::move(error)),
    _offsetSettings(offsetSettings)
{
    if (_message.get_error() && !_error._error) {
        _error._error = _message.get_error();
        _error._source |= (uint8_t)DeserializerError::Source::Kafka;
    }
}

template <typename K, typename P>
ReceivedMessage<K,P>::~ReceivedMessage()
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

template <typename K, typename P>
uint64_t ReceivedMessage<K,P>::getHandle() const
{
    return (uint64_t)_message.get_handle();
}

template <typename K, typename P>
const Buffer& ReceivedMessage<K,P>::getKeyBuffer() const
{
    return _message.get_key();
}

template <typename K, typename P>
const IMessage::HeaderListType&
ReceivedMessage<K,P>::getHeaderList() const
{
    return _message.get_header_list();
}

template <typename K, typename P>
const Buffer& ReceivedMessage<K,P>::getPayloadBuffer() const
{
    return _message.get_payload();
}

template <typename K, typename P>
Error ReceivedMessage<K,P>::getError() const
{
    return _error._error;
}

template <typename K, typename P>
int ReceivedMessage<K,P>::getHeaderNumWithError() const
{
    return _error._headerNum;
}

template <typename K, typename P>
std::string ReceivedMessage<K,P>::getTopic() const
{
    return _message.get_topic();
}

template <typename K, typename P>
int ReceivedMessage<K,P>::getPartition() const
{
    return _message.get_partition();
}

template <typename K, typename P>
int64_t ReceivedMessage<K,P>::getOffset() const
{
    return _message.get_offset();
}

template <typename K, typename P>
std::chrono::milliseconds ReceivedMessage<K,P>::getTimestamp() const
{
    boost::optional<MessageTimestamp> timestamp = _message.get_timestamp();
    if (!timestamp) {
        throw std::runtime_error("Timestamp not set");
    }
    return timestamp.get().get_timestamp();
}

template <typename K, typename P>
ReceivedMessage<K,P>::operator bool() const
{
    return (bool)_message;
}

template <typename K, typename P>
bool ReceivedMessage<K,P>::skip() const
{
    return _error.isPreprocessorError();
}

template <typename K, typename P>
void ReceivedMessage<K,P>::setOpaque(const void* opaque)
{
    _opaque = opaque;
}

template <typename K, typename P>
Error ReceivedMessage<K,P>::commit(const void* opaque)
{
    _opaque = opaque;
    if ((_opaque != nullptr) &&
        (_offsetSettings._autoOffsetPersistStrategy == OffsetPersistStrategy::Commit) &&
        _committer.get_consumer().get_configuration().get_offset_commit_callback()) {
        _offsets.insert(TopicPartition(getTopic(), getPartition(), getOffset()), opaque);
    }
    return doCommit();
}

template <typename K, typename P>
bool ReceivedMessage<K,P>::isEof() const
{
    return _message.is_eof();
}

template <typename K, typename P>
const K& ReceivedMessage<K,P>::getKey() const &
{
    validateKeyError();
    return _key;
}

template <typename K, typename P>
K& ReceivedMessage<K,P>::getKey() &
{
    validateKeyError();
    return _key;
}

template <typename K, typename P>
K&& ReceivedMessage<K,P>::getKey() &&
{
    validateKeyError();
    return _key;
}

template <typename K, typename P>
const P& ReceivedMessage<K,P>::getPayload() const &
{
    validatePayloadError();
    return _payload;
}

template <typename K, typename P>
P& ReceivedMessage<K,P>::getPayload() &
{
    validatePayloadError();
    return _payload;
}

template <typename K, typename P>
P&& ReceivedMessage<K,P>::getPayload() &&
{
    validatePayloadError();
    return std::move(_payload);
}

template <typename K, typename P>
const HeaderPack& ReceivedMessage<K,P>::getHeaders() const &
{
    validateHeadersError();
    return _headers;
}

template <typename K, typename P>
HeaderPack& ReceivedMessage<K,P>::getHeaders() &
{
    validateHeadersError();
    return _headers;
}

template <typename K, typename P>
HeaderPack&& ReceivedMessage<K,P>::getHeaders() &&
{
    validateHeadersError();
    return std::move(_headers);
}

template <typename K, typename P>
void ReceivedMessage<K,P>::validateKeyError() const
{
    if (_error.isPreprocessorError()) {
        throw std::runtime_error("Dropped message");
    }
    if (_error.isKafkaError()) {
        throw std::runtime_error("Invalid message");
    }
    if (_error.isKeyError()) {
        throw std::runtime_error("Key deserialization error");
    }
}

template <typename K, typename P>
void ReceivedMessage<K,P>::validatePayloadError() const
{
    if (_error.isPreprocessorError()) {
        throw std::runtime_error("Dropped message");
    }
    if (_error.isKafkaError()) {
        throw std::runtime_error("Invalid message");
    }
    if (_error.isPayloadError()) {
        throw std::runtime_error("Payload deserialization error");
    }
}

template <typename K, typename P>
void ReceivedMessage<K,P>::validateHeadersError() const
{
    if (_error.isPreprocessorError()) {
        throw std::runtime_error("Dropped message");
    }
    if (_error.isKafkaError()) {
        throw std::runtime_error("Invalid message");
    }
    if (_error.isHeaderError()) {
        throw std::runtime_error("Header deserialization error");
    }
}

template <typename K, typename P>
Error ReceivedMessage<K,P>::doCommit()
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
    catch (const HandleException& ex) {
        return ex.get_error();
    }
    catch (const ActionTerminatedException& ex) {
        return RD_KAFKA_RESP_ERR__FAIL; //no more retries left
    }
    catch (...) {
        return RD_KAFKA_RESP_ERR_UNKNOWN;
    }
    return {};
}

}
}
