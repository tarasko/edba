#include <edba/session_pool.hpp>

namespace edba {

class connection_proxy;

void session_pool::invoke_on_connect(const conn_init_callback& callback)
{
    boost::mutex::scoped_lock g(pool_guard_);
    conn_init_callback_ = callback;
}

session session_pool::open()
{
    boost::mutex::scoped_lock g(pool_guard_);

    if (!pool_.empty())  // take connection from pool
    {
        session sess(pool_.back());
        pool_.pop_back();
        return sess;
    }
    else if (pool_.empty() && conn_left_unopened_) // we can create new connection
    {
        boost::intrusive_ptr<backend::connection> conn = conn_create_callback_(conn_info_, sm_);
        session sess(conn);
        if (conn_init_callback_)
            conn_init_callback_(sess);

        --conn_left_unopened_;

        return sess;
    }
    else // we must wait until someone will free connection for us
    {
        pool_max_cv_.wait(g); 
        assert(!pool_.empty() && "pool_ is not empty");

        session sess(pool_.back());
        pool_.pop_back();
        return sess;
    }
}

bool session_pool::try_open(session& sess)
{
    boost::mutex::scoped_lock g(pool_guard_);

    if (!pool_.empty())  // take connection from pool
    {
        session tmp(pool_.back());
        pool_.pop_back();
        sess = tmp;
    }
    else if (pool_.empty() && conn_left_unopened_) // we can create new connection
    {
        boost::intrusive_ptr<backend::connection> conn = conn_create_callback_(conn_info_, sm_);
        session tmp(conn);
        if (conn_init_callback_)
            conn_init_callback_(tmp);

        --conn_left_unopened_;

        sess = tmp;
    }
    else // we must wait until someone will free connection for us
        return false;

    return true;
}

void session_pool::insert_to_pool(const boost::intrusive_ptr<backend::connection>& conn)
{
    boost::mutex::scoped_lock g(pool_guard_);
    pool_.push_back(conn);
}

}
