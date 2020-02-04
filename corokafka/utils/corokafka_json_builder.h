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
    enum class Brace { None, Start, End, Both};
    JsonBuilder(std::ostream& oss) : _ostream(oss)
    {
        _ostream << std::boolalpha << "{";
    }
    JsonBuilder& startMember(const char* name, bool isArray = false) {
        _isArray = isArray;
        if (!_firstMember) {
            _ostream << ",";
        }
        _ostream << "\"" << name << "\":";
        _ostream << (_isArray ? "[" : "{");
        return *this;
    }
    JsonBuilder& tag(const char* name,
                     const char* value,
                     Brace type = Brace::None)
    {
        comma();
        if (type == Brace::Start || type == Brace::Both) brace(Brace::Start);
        _ostream << "\"" << name << "\":\"" << value << "\"";
        if (type == Brace::End || type == Brace::Both) brace(Brace::End);
        return *this;
    }
    template<typename T, std::enable_if_t<std::is_integral<T>::value, int> = 0>
    JsonBuilder& tag(const char* name,
                     const T& value,
                     Brace type = Brace::None,
                     std::ios::fmtflags flag = std::ios_base::dec)
    {
        comma();
        if (type == Brace::Start || type == Brace::Both) brace(Brace::Start);
        _ostream.setf(flag, std::ios_base::basefield);
        _ostream << "\"" << name << "\":" << value;
        if (type == Brace::End || type == Brace::Both) brace(Brace::End);
        return *this;
    }
    template<typename T, std::enable_if_t<!std::is_integral<T>::value && !std::is_pointer<T>::value, int> = 0>
    JsonBuilder& tag(const char* name,
                     const T& value,
                     Brace type = Brace::None)
    {
        comma();
        if (type == Brace::Start || type == Brace::Both) brace(Brace::Start);
        _ostream << "\"" << name << "\":\"" << value << "\"";
        if (type == Brace::End || type == Brace::Both) brace(Brace::End);
        return *this;
    }
    template<typename T, std::enable_if_t<!std::is_integral<T>::value && std::is_pointer<T>::value, int> = 0>
    JsonBuilder& tag(const char* name,
                     const T& value,
                     Brace type = Brace::None)
    {
        comma();
        if (type == Brace::Start || type == Brace::Both) brace(Brace::Start);
        _ostream.setf(std::ios_base::hex, std::ios_base::basefield);
        _ostream << "\"" << name << "\":" << value;
        if (type == Brace::End || type == Brace::Both) brace(Brace::End);
        return *this;
    }
    JsonBuilder& endMember() {
        end();
        _firstField = true; //reset flag
        _firstMember = false;
        _isArray = false;
        return *this;
    }
    void end() {
        _ostream << (_isArray ? "]" : "}");
    }
private:
    void comma() {
        if (_firstField) {
            _firstField = false;
        }
        else {
            _ostream << ",";
        }
    }
    void brace(Brace type) {
        if (type == Brace::Start) {
            if (!_braceContext) {
                _ostream << "{";
                _braceContext = true;
            }
        }
        else if (type == Brace::End) {
            if (_braceContext) {
                _ostream << "}";
                _braceContext = false;
            }
        }
    }
    std::ostream&   _ostream;
    bool            _firstField{true};
    bool            _firstMember{true};
    bool            _isArray{false};
    bool            _braceContext{false};
};

}
}

#endif //BLOOMBERG_COROKAFKA_JSON_BUILDER_H
