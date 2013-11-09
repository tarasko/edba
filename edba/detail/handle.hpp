#ifndef EDBA_HANDLE_HPP
#define EDBA_HANDLE_HPP

#include <boost/move/move.hpp>

namespace edba { namespace detail {

template <typename Handle, typename IntType, IntType HandleType, typename Deallocator> 
class handle 
{
    struct proxy;

    BOOST_MOVABLE_BUT_NOT_COPYABLE(handle)

public:
    handle() : handle_(0) {}
    explicit handle(Handle handle) : handle_(handle) {}

    // move constructor
    handle(BOOST_RV_REF(handle) x) : handle_(x.handle_)
    {
        x.handle_ = 0;
    }

    handle& operator=(BOOST_RV_REF(handle) x) 
    {
        handle_ = x.handle_;
        x.handle_ = 0;
        return *this;
    }

    ~handle() 
    {
        if(handle_)
            Deallocator::free(handle_, HandleType);
    }

    void reset(Handle handle = 0)
    {
        if(handle_)
            Deallocator::free(handle_, HandleType);

        handle_ = handle;
    }

    Handle get() const 
    {
        return handle_;
    }

    proxy ptr() 
    {
        return proxy(this);
    }

private:
    struct proxy 
    {
        proxy(handle* h) : h_(h), val_(h->get()) {}

        ~proxy() 
        {
            if (h_->get() != val_) 
                h_->reset(val_);
        }

        operator Handle*() 
        {
            return &val_;
        }

        void** as_void()
        {
            return (void**)&val_;
        }

    private:
        handle* h_;
        Handle val_;
    };

    Handle handle_;
};

}}

#endif