#ifndef BLOOMBERG_COROKAFKA_IMPL_H
#define BLOOMBERG_COROKAFKA_IMPL_H

#include <memory>
#include <utility>

namespace Bloomberg {
namespace corokafka {

template <typename T>
struct ImplBase
{
    ImplBase() = default;
    
    template <typename ...ARGS>
    ImplBase(std::piecewise_construct_t, ARGS&&...args) :
        _impl(std::make_shared<T>(std::forward<ARGS>(args)...))
    {}
    template <typename U, typename ...ARGS>
    ImplBase(U*, ARGS&&...args) :
        _impl(std::make_shared<U>(std::forward<ARGS>(args)...))
    {}
    template <typename U>
    explicit ImplBase(std::shared_ptr<U> impl) :
        _impl(std::move(impl))
    {}
    virtual ~ImplBase() = default;
    
    const std::shared_ptr<T>& impl() const { return _impl; }
    std::shared_ptr<T>& impl() { return _impl; }
    
private:
    std::shared_ptr<T> _impl;
};

template <typename T>
struct Impl : public ImplBase<T>, public T
{
    using ImplBase<T>::ImplBase;
};

template <typename T>
struct VirtualImpl : public ImplBase<T>, public virtual T
{
    using ImplBase<T>::ImplBase;
};

}
}

#endif //BLOOMBERG_COROKAFKA_IMPL_H
