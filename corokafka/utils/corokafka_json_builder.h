#ifndef BLOOMBERG_COROKAFKA_JSON_BUILDER_H
#define BLOOMBERG_COROKAFKA_JSON_BUILDER_H

#include <sstream>
#include <ios>
#include <type_traits>

namespace Bloomberg {
namespace corokafka {

class JsonBuilder
{
public:
    JsonBuilder(std::ostream& oss) : _ostream(oss)
    {
        _ostream << std::boolalpha << "{ ";
    }
    JsonBuilder& startMember(const char* name) {
        if (!_firstMember) {
            _ostream << ", ";
        }
        _ostream << "\"" << name << "\": { ";
        return *this;
    }
    JsonBuilder& tag(const char* name, const char* value)
    {
        comma();
        _ostream << "\"" << name << "\": \"" << value << "\"";
        return *this;
    }
    template<typename T, std::enable_if_t<std::is_integral<T>::value, int> = 0>
    JsonBuilder& tag(const char* name, const T& value, std::ios::fmtflags flag = std::ios_base::dec)
    {
        comma();
        _ostream.setf(flag, std::ios_base::basefield);
        _ostream << "\"" << name << "\": " << value;
        return *this;
    }
    template<typename T, std::enable_if_t<!std::is_integral<T>::value && !std::is_pointer<T>::value, int> = 0>
    JsonBuilder& tag(const char* name, const T& value)
    {
        comma();
        _ostream << "\"" << name << "\": \"" << value << "\"";
        return *this;
    }
    template<typename T, std::enable_if_t<!std::is_integral<T>::value && std::is_pointer<T>::value, int> = 0>
    JsonBuilder& tag(const char* name, const T& value)
    {
        comma();
        _ostream.setf(std::ios_base::hex, std::ios_base::basefield);
        _ostream << "\"" << name << "\": " << value;
        return *this;
    }
    JsonBuilder& endMember() {
        _firstField = true; //reset flag
        _firstMember = false;
        end();
        return *this;
    }
    void end() {
        _ostream << " }";
    }
private:
    void comma() {
        if (_firstField) {
            _firstField = false;
        }
        else {
            _ostream << ", ";
        }
    }
    std::ostream&   _ostream;
    bool            _firstField{true};
    bool            _firstMember{true};
};

}
}

#endif //BLOOMBERG_COROKAFKA_JSON_BUILDER_H
