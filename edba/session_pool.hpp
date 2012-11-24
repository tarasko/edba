#ifndef EDBA_SESSION_POOL_HPP
#define EDBA_SESSION_POOL_HPP

#include <edba/session.hpp>
#include <edba/conn_info.hpp>

#include <boost/function.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition_variable.hpp>

namespace edba {

///
/// 

class EDBA_API session_pool
{
public:
    typedef boost::function<boost::intrusive_ptr<backend::connection_iface>(const conn_info& ci, session_monitor* sm)> conn_create_callback;
    typedef boost::function<void(session&)> conn_init_callback;

    ///
    /// Construct pool of session with max limit.
    /// All sessions will be created using specified \a driver and \a conn_string.
    /// Constructor doesn`t create connection itself, instead they will be created lazily by \a open and \a try_open calls.
    ///
    template<typename Driver>
    session_pool(Driver driver, const char* conn_string, int max_pool_size, session_monitor* sm = 0);

    ///
    /// Invoke provided function object once on connection creation. This allow to setup all
    /// sessions in pool in uniform manner. configure call doesn`t affect already created sessions it will be applied only
    /// to the new one.
    ///
    void invoke_on_connect(const conn_init_callback& callback);

    ///
    /// Get session from pool or create new one if there is no free sessions and max_pool_size limit is not exceeded.
    /// If there is no free sessions and max_pool_size limit exceeded then wait until someone will release session.
    ///
    session open();
    ///
    /// Get session from pool or create new one if there is no free sessions and max_pool_size limit is not exceeded.
    /// If there is no free sessions and max_pool_size limit exceeded then return false and leave sess untouched
    ///
    bool try_open(session& sess);

private:
    // NONCOPYABLE
    session_pool(const session_pool&);
    session_pool& operator=(const session_pool&);

    ///
    /// Insert connection back to the pool.
    ///
    void insert_to_pool(const boost::intrusive_ptr<backend::connection_iface>& conn);

private:
    class connection_proxy;

    conn_create_callback conn_create_callback_;
    conn_info conn_info_;
    int conn_left_unopened_;
    session_monitor* sm_;

    conn_init_callback conn_init_callback_;

    std::vector< boost::intrusive_ptr<backend::connection_iface> > pool_;
    boost::mutex pool_guard_;
    boost::condition_variable pool_max_cv_;
};

template<typename Driver>
session_pool::session_pool(Driver driver, const char* conn_string, int max_pool_size, session_monitor* sm)
    : conn_create_callback_(driver)
    , conn_info_(conn_string)
    , conn_left_unopened_(max_pool_size)
    , sm_(sm)
{
    pool_.reserve(max_pool_size);
}


}                                                                               // namespace edba

#endif                                                                          // EDBA_SESSION_POOL_HPP

