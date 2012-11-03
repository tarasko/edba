#include <edba/session.hpp>
#include <edba/backend/backend.hpp>

namespace edba {

session::session()
{
}
session::session(const boost::intrusive_ptr<backend::connection>& conn) : conn_(conn) 
{
}

void session::close()
{
    conn_.reset();
}

bool session::is_open()
{
    return conn_;
}

statement session::prepare(const string_ref& query)
{
    boost::intrusive_ptr<backend::statement> stat_ptr(conn_->prepare(query));
    statement stat(stat_ptr,conn_);
    return stat;
}

statement session::create_statement(const string_ref& query)
{
    boost::intrusive_ptr<backend::statement> stat_ptr(conn_->get_statement(query));
    statement stat(stat_ptr,conn_);
    return stat;
}

statement session::create_prepared_statement(const string_ref& query)
{
    boost::intrusive_ptr<backend::statement> stat_ptr(conn_->get_prepared_statement(query));
    statement stat(stat_ptr,conn_);
    return stat;
}

void session::exec_batch(const string_ref& q)
{
    conn_->exec_batch(q);
}

void session::begin()
{
    conn_->begin();
}
void session::commit()
{
    conn_->commit();
}
void session::rollback()
{
    conn_->rollback();
}
std::string session::escape(char const *b,char const *e)
{
    return conn_->escape(b,e);
}
std::string session::escape(char const *s)
{
    return conn_->escape(s);
}
std::string session::escape(std::string const &s)
{
    return conn_->escape(s);
}
const std::string& session::backend()
{
    return conn_->backend();
}
const std::string& session::engine()
{
    return conn_->engine();
}
void session::version(int& major, int& minor)
{
    conn_->version(major, minor);
}
const std::string& session::description()
{
    return conn_->description();
}

}
