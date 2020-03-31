#ifndef BLOOMBERG_COROKAFKA_EXCEPTION_H
#define BLOOMBERG_COROKAFKA_EXCEPTION_H

#include <corokafka/utils/corokafka_json_builder.h>
#include <stdexcept>
#include <sstream>
#include <atomic>

namespace Bloomberg {
namespace corokafka {

//=========================================================================================
//                                 Exception
//=========================================================================================
struct Exception : public std::runtime_error
{
    using std::runtime_error::runtime_error;

    const char* what() const noexcept override
    {
        if (_what.empty()) {
            std::ostringstream oss;
            JsonBuilder json(oss);
            json.startMember(name()).
                tag("reason", std::runtime_error::what()).
                endMember().end();
            _what = oss.str();
        }
        return _what.c_str();
    }
    virtual const char* name() const noexcept
    {
        return "Exception";
    }
protected:
    mutable std::string _what;
};

//=========================================================================================
//                                 MessageException
//=========================================================================================
struct MessageException : public Exception
{
    using Exception::Exception;
    const char* name() const noexcept override
    {
        return "MessageException";
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
        return "HandleException";
    }
};

//=========================================================================================
//                                 TopicException
//=========================================================================================
struct TopicException : public Exception
{
    TopicException(const std::string& topic,
                   const std::string& reason) :
        Exception(reason),
        _topic(topic)
    {}
    TopicException(const std::string& topic,
                   const char* reason) :
        Exception(reason),
        _topic(topic)
    {}
    using Exception::Exception;
    const char* what() const noexcept override
    {
        if (_what.empty()) {
            std::ostringstream oss;
            JsonBuilder json(oss);
            json.startMember(name()).
                tag("topic", topic()).
                    tag("reason", std::runtime_error::what()).
                    endMember().end();
            _what = oss.str();
        }
        return _what.c_str();
    }
    const char* topic() const noexcept
    {
        return _topic.c_str();
    }
    const char* name() const noexcept override
    {
        return "TopicException";
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
        return "ConfigurationException";
    }
};

//=========================================================================================
//                                 InvalidArgumentException
//=========================================================================================
struct InvalidArgumentException : public Exception
{
    InvalidArgumentException(size_t argument,
                             const std::string& reason) :
        Exception(reason),
        _argument(argument)
    {
    }
    InvalidArgumentException(size_t argument,
                             const char* reason) :
        Exception(reason),
        _argument(argument)
    {
    }
    const char* what() const noexcept override
    {
        if (_what.empty()) {
            std::ostringstream oss;
            JsonBuilder json(oss);
            json.startMember(name()).
                tag("argument", argument()).
                    tag("reason", std::runtime_error::what()).
                    endMember().end();
            _what = oss.str();
        }
        return _what.c_str();
    }
    const char* name() const noexcept override
    {
        return "InvalidArgumentException";
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
    InvalidOptionException(const std::string& topic,
                           const std::string& option,
                           const std::string& reason) :
        ConfigurationException(topic, reason),
        _option(option)
    {}
    InvalidOptionException(const std::string& topic,
                           const std::string& option,
                           const char* reason) :
        ConfigurationException(topic, reason),
        _option(option)
    {}
    InvalidOptionException(const std::string& option,
                           const std::string& reason) :
        ConfigurationException(reason),
        _option(option)
    {}
    InvalidOptionException(const std::string& option,
                           const char* reason) :
        ConfigurationException(reason),
        _option(option)
    {}
    const char* what() const noexcept override
    {
        if (_what.empty()) {
            std::ostringstream oss;
            JsonBuilder json(oss);
            json.startMember(name()).
                tag("topic", topic()).
                    tag("option", option()).
                    tag("reason", std::runtime_error::what()).
                    endMember().end();
            _what = oss.str();
        }
        return _what.c_str();
    }
    const char* option() const noexcept
    {
        return _option.c_str();
    }
    const char* name() const noexcept override
    {
        return "InvalidOptionException";
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
        return "ConsumerException";
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
        return "ProducerException";
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
        return "FeatureNotSupportedException";
    }
};

}
}

#endif //BLOOMBERG_COROKAFKA_EXCEPTION_H
