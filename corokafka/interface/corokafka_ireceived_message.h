#ifndef BLOOMBERG_COROKAFKA_IRECEIVED_MESSAGE_H
#define BLOOMBERG_COROKAFKA_IRECEIVED_MESSAGE_H

#include <corokafka/interface/corokafka_imessage.h>
#include <corokafka/corokafka_header_pack.h>

namespace Bloomberg {
namespace corokafka {

//Used for mocking only
template <typename HEADER>
struct IHeaderAccessor
{
    virtual ~IHeaderAccessor() = default;
    virtual const HEADER& getHeaderAt(size_t index) const = 0;
    virtual HEADER& getHeaderAt(size_t index) = 0;
    virtual bool isHeaderValidAt(size_t index) = 0;
};

template<typename KEY, typename PAYLOAD, typename HEADERS>
struct IReceivedMessage : public virtual IMessage
{
    using KeyType = KEY;
    using PayloadType = PAYLOAD;
    using HeadersType = HEADERS;
    using HeaderTypes = typename HEADERS::HeaderTypes;
    
    /**
     * @brief Indicates if this message should be skipped.
     * @note This is a message which was flagged to be dropped by the 'preprocessor' callback.
     * @warning Processing such a message can result in a fatal failure!
     */
    virtual bool skip() const = 0;
    
    /**
     * @brief Set an application-specific pointer which will be returned inside the offset commit callback.
     *        This is used when commit() is called automatically by the destructor.
     * @param opaque Application-specific pointer.
     * @warning This library does not extend the lifetime of this pointer. The application must ensure the
     *          memory location is still valid until the callback is invoked.
     */
    virtual void setOpaque(const void *opaque) = 0;
    
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
    virtual cppkafka::Error commit(const void *opaque) = 0;
    virtual cppkafka::Error commit(ExecMode execMode,
                                   const void *opaque) = 0;
    
    /**
     * @brief Helper function to indicate if the message error is an EOF.
     * @return True if EOF was encountered for the partition, False otherwise.
     */
    virtual bool isEof() const = 0;
    
    //==========================================================================
    //                     Deserialized type accessors
    //==========================================================================
    /**
     * @brief Get the key object reference
     * @return The reference
     */
    virtual const KeyType &getKey() const = 0;
    virtual KeyType &getKey() = 0;
    
    /**
     * @brief Get the payload object reference
     * @return The reference
     */
    virtual const PayloadType &getPayload() const = 0;
    virtual PayloadType &getPayload() = 0;
    
    /**
     * @brief Get the header pack
     * @return The reference to the pack
     * @warning Accessing headers via the HeaderPack is not type-safe. Casting to incorrect type may
     *          to undefined behavior or 'std::bad_cast' being thrown.
     */
    virtual const HeaderPack &getHeaders() const = 0;
    virtual HeaderPack &getHeaders() = 0;
};

}}

#endif //BLOOMBERG_COROKAFKA_IRECEIVED_MESSAGE_H
