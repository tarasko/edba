#include <edba/session.hpp>
#include <edba/backend/backend.hpp>

namespace edba {

session::session()
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

statement session::prepare_statement(const string_ref& query)
{
    boost::intrusive_ptr<backend::statement> stat_ptr(conn_->prepare_statement(query));
    statement stat(stat_ptr);
    return stat;
}

statement session::create_statement(const string_ref& query)
{
    boost::intrusive_ptr<backend::statement> stat_ptr(conn_->create_statement(query));
    statement stat(stat_ptr);
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
