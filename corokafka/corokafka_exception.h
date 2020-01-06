#ifndef BLOOMBERG_COROKAFKA_EXCEPTION_H
#define BLOOMBERG_COROKAFKA_EXCEPTION_H

#include <corokafka/utils/corokafka_json_builder.h>
#include <stdexcept>
#include <sstream>

namespace Bloomberg {
namespace corokafka {

//=========================================================================================
//                                 Exception
//=========================================================================================
struct Exception : public std::runtime_error
{
    using std::runtime_error::runtime_error;
    virtual ~Exception() = default;
    const char* what() const noexcept override
    {
        std::ostringstream oss;
        JsonBuilder json(oss);
        json.startMember(name()).
            tag("reason", std::runtime_error::what()).
            endMember().end();
        return oss.str().c_str();
    }
    virtual const char* name() const noexcept
    {
        static const char* name = "Exception";
        return name;
    }
};

//=========================================================================================
//                                 MessageException
//=========================================================================================
struct MessageException : public Exception
{
    using Exception::Exception;
    const char* name() const noexcept override
    {
        static const char* name = "MessageException";
        return name;
    }
};

//=========================================================================================
//                                 HandleException
//=========================================================================================
struct HandleException : public Exception
{
    using Exception::Exception;
    const char* name() const noexcept override
    {
        static const char* name = "HandleException";
        return name;
    }
};

//=========================================================================================
//                                 TopicException
//=========================================================================================
struct TopicException : public Exception
{
    TopicException(std::string topic, const std::string& reason) :
        Exception(reason),
        _topic(std::move(topic))
    {}
    TopicException(std::string topic, const char* reason) :
        Exception(reason),
        _topic(std::move(topic))
    {}
    using Exception::Exception;
    virtual ~TopicException() = default;
    const char* what() const noexcept override
    {
        std::ostringstream oss;
        JsonBuilder json(oss);
        json.startMember(name()).
            tag("topic", topic()).
            tag("reason", std::runtime_error::what()).
            endMember().end();
        return oss.str().c_str();
    }
    const char* topic() const noexcept
    {
        return _topic.c_str();
    }
    const char* name() const noexcept override
    {
        static const char* name = "TopicException";
        return name;
    }
private:
    std::string _topic{"unknown"};
};

//=========================================================================================
//                                 ConfigurationException
//=========================================================================================
struct ConfigurationException : public TopicException
{
    using TopicException::TopicException;
    const char* name() const noexcept override
    {
        static const char* name = "ConfigurationException";
        return name;
    }
};

//=========================================================================================
//                                 InvalidArgumentException
//=========================================================================================
struct InvalidArgumentException : public Exception
{
    InvalidArgumentException(size_t argument, const std::string& reason) :
        Exception(reason),
        _argument(argument)
    {
    }
    InvalidArgumentException(size_t argument, const char* reason) :
        Exception(reason),
        _argument(argument)
    {
    }
    const char* what() const noexcept override
    {
        std::ostringstream oss;
        JsonBuilder json(oss);
        json.startMember(name()).
            tag("argument", argument()).
            tag("reason", std::runtime_error::what()).
            endMember().end();
        return oss.str().c_str();
    }
    const char* name() const noexcept override
    {
        static const char* name = "InvalidArgumentException";
        return name;
    }
    size_t argument() const { return _argument; }
private:
    size_t  _argument;
};

//=========================================================================================
//                                 InvalidOptionException
//=========================================================================================
struct InvalidOptionException : public ConfigurationException
{
    InvalidOptionException(std::string topic, std::string option, const std::string& reason) :
        ConfigurationException(std::move(topic), reason),
        _option(std::move(option))
    {}
    InvalidOptionException(std::string topic, std::string option, const char* reason) :
        ConfigurationException(std::move(topic), reason),
        _option(std::move(option))
    {}
    InvalidOptionException(std::string option, const std::string& reason) :
        ConfigurationException(std::move(reason)),
        _option(std::move(option))
    {}
    InvalidOptionException(std::string option, const char* reason) :
        ConfigurationException(std::move(reason)),
        _option(std::move(option))
    {}
    const char* what() const noexcept override
    {
        std::ostringstream oss;
        JsonBuilder json(oss);
        json.startMember(name()).
            tag("topic", topic()).
            tag("option", option()).
            tag("reason", std::runtime_error::what()).
            endMember().end();
        return oss.str().c_str();
    }
    const char* option() const noexcept
    {
        return _option.c_str();
    }
    const char* name() const noexcept override
    {
        static const char* name = "InvalidOptionException";
        return name;
    }
private:
    std::string _option;
};

//=========================================================================================
//                                 ConsumerException
//=========================================================================================
struct ConsumerException : public TopicException
{
    using TopicException::TopicException;
    const char* name() const noexcept override
    {
        static const char* name = "ConsumerException";
        return name;
    }
};

//=========================================================================================
//                                 ProducerException
//=========================================================================================
struct ProducerException : public TopicException
{
    using TopicException::TopicException;
    const char* name() const noexcept override
    {
        static const char* name = "ProducerException";
        return name;
    }
};

//=========================================================================================
//                                 FeatureNotSupportedException
//=========================================================================================
struct FeatureNotSupportedException : public Exception
{
    using Exception::Exception;
    const char* name() const noexcept override
    {
        static const char* name = "FeatureNotSupportedException";
        return name;
    }
};

}
}

#endif //BLOOMBERG_COROKAFKA_EXCEPTION_H
