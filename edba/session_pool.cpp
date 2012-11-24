#include <edba/session_pool.hpp>

namespace edba {

struct session_pool::connection_proxy : backend::connection_iface
{
    connection_proxy(session_pool& pool, const backend::connection_ptr& conn) 
        : pool_(pool), conn_(conn) 
    {
    }
    
    ~connection_proxy()
    {
        boost::mutex::scoped_lock g(pool_.pool_guard_);
        pool_.pool_.push_back(conn_);
        pool_.pool_max_cv_.notify_one();
    }

    virtual backend::statement_ptr prepare_statement(const string_ref& q)
    {
        return conn_->prepare_statement(q);
    }

    virtual backend::statement_ptr create_statement(const string_ref& q)
    {
        return conn_->create_statement(q);
    }
    
    virtual void exec_batch(const string_ref& q)
    {
        return conn_->exec_batch(q);
    }

    virtual void set_specific(const boost::any& data)
    {
        return conn_->set_specific(data);
    }

    virtual boost::any& get_specific()
    {
        return conn_->get_specific();
    }

    virtual void begin()
    {
        return conn_->begin();
    }

    virtual void commit()
    {
        return conn_->commit();
    }

    virtual void rollback()
    {
        return conn_->rollback();
    }

    virtual std::string escape(const string_ref& str)
    {
        return conn_->escape(str);
    }

    virtual const std::string& backend()
    {
        return conn_->backend();
    }

    virtual const std::string& engine()
    {
        return conn_->engine();
    }

    virtual void version(int& major, int& minor)
    {
        return conn_->version(major, minor);
    }

    virtual const std::string& description()
    {
        return conn_->description();
    }

private:
    session_pool& pool_;
    backend::connection_ptr conn_;
};

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
        session sess(create_proxy(pool_.back()));
        pool_.pop_back();
        return sess;
    }
    else if (pool_.empty() && conn_left_unopened_) // we can create new connection
    {
        backend::connection_ptr conn = conn_create_callback_(conn_info_, sm_);
        session sess(create_proxy(conn));
        if (conn_init_callback_)
            conn_init_callback_(sess);

        --conn_left_unopened_;

        return sess;
    }
    else // we must wait until someone will free connection for us
    {
        pool_max_cv_.wait(g, !boost::bind(&pool_type::empty, &pool_)); 
        assert(!pool_.empty() && "pool_ is not empty");

        session sess(create_proxy(pool_.back()));
        pool_.pop_back();
        return sess;
    }
}

bool session_pool::try_open(session& sess)
{
    boost::mutex::scoped_lock g(pool_guard_);

    if (!pool_.empty())  // take connection from pool
    {
        sess = session(create_proxy(pool_.back()));
        pool_.pop_back();
    }
    else if (pool_.empty() && conn_left_unopened_) // we can create new connection
    {
        backend::connection_ptr conn = conn_create_callback_(conn_info_, sm_);
        session tmp(create_proxy(conn));
        if (conn_init_callback_)
            conn_init_callback_(tmp);

        --conn_left_unopened_;

        sess = tmp;
    }
    else // we must wait until someone will free connection for us
        return false;

    return true;
}

backend::connection_ptr session_pool::create_proxy(const backend::connection_ptr& conn)
{
    return backend::connection_ptr(new connection_proxy(*this, conn));
}

}
