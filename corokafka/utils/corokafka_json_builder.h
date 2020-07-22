#ifndef BLOOMBERG_COROKAFKA_JSON_BUILDER_H
#define BLOOMBERG_COROKAFKA_JSON_BUILDER_H

#include <sstream>
#include <ios>
#include <type_traits>
#include <vector>

namespace Bloomberg {
namespace corokafka {

class JsonBuilder
{
public:
    enum class Array { False, True };
    explicit JsonBuilder(std::ostream& oss) : _ostream(oss)
    {
        _ostream << std::boolalpha << "{";
    }
    ~JsonBuilder() {
        _ostream << "}";
    }
    JsonBuilder& startMember(const char* name,
                             Array isArray = Array::False)
    {
        comma();
        //add a new level
        _levels.push_back({});
        //mark it as an array
        _levels.back()._isArray = (isArray  == Array::True);
        _ostream << "\"" << name << "\":";
        _ostream << (_levels.back()._isArray ? "[" : "{");
        return *this;
    }
    JsonBuilder& startMember(Array isArray = Array::False)
    {
        comma();
        //add a new level
        _levels.push_back({});
        //mark it as an array
        _levels.back()._isArray = (isArray  == Array::True);
        _ostream << (_levels.back()._isArray ? "[" : "{");
        return *this;
    }
    JsonBuilder& tag(const char* name,
                     const char* value)
    {
        comma();
        _ostream << "\"" << name << "\":\"" << value << "\"";
        return *this;
    }
    template<typename T, std::enable_if_t<std::is_integral<T>::value, int> = 0>
    JsonBuilder& tag(const char* name,
                     const T& value,
                     std::ios::fmtflags flag = std::ios_base::dec)
    {
        comma();
        _ostream.setf(flag, std::ios_base::basefield);
        _ostream << "\"" << name << "\":" << value;
        return *this;
    }
    template<typename T, std::enable_if_t<!std::is_integral<T>::value && !std::is_pointer<T>::value, int> = 0>
    JsonBuilder& tag(const char* name,
                     const T& value)
    {
        comma();
        _ostream << "\"" << name << "\":\"" << value << "\"";
        return *this;
    }
    template<typename T, std::enable_if_t<!std::is_integral<T>::value && std::is_pointer<T>::value, int> = 0>
    JsonBuilder& tag(const char* name,
                     const T& value)
    {
        comma();
        _ostream.setf(std::ios_base::hex, std::ios_base::basefield);
        _ostream << "\"" << name << "\":" << value;
        return *this;
    }
    //Raw tags assume the value is already JSON-formatted
    template<typename T>
    JsonBuilder& rawTag(const char* name,
                        const T& value)
    {
        comma();
        _ostream << "\"" << name << "\":" << value;
        return *this;
    }
    //Raw tags assume the value is already JSON-formatted
    template<typename T>
    JsonBuilder& rawTag(const T& tagAndValue)
    {
        comma();
        _ostream << tagAndValue;
        return *this;
    }
    JsonBuilder& endMember() {
        _ostream << (_levels.back()._isArray ? "]" : "}");
        _levels.pop_back();
        return *this;
    }
private:
    struct Level {
        bool            _firstField{true};
        bool            _isArray{false};
    };
    void comma() {
        if (_levels.empty()) {
            return;
        }
        Level& level = _levels.back();
        if (level._firstField) {
            level._firstField = false;
        }
        else {
            _ostream << ",";
        }
    }

    std::ostream&       _ostream;
    std::vector<Level>  _levels;
    
};

}
}

#endif //BLOOMBERG_COROKAFKA_JSON_BUILDER_H
