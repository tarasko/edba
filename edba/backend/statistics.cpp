#include <edba/backend/statistics.hpp>
#include <edba/backend/interfaces.hpp>

namespace edba { namespace backend {

namespace {

struct dump_to_ostream : boost::static_visitor<>
{
    dump_to_ostream(std::ostream& os) : os_(os) {}

    template<typename T>
    void operator()(const T& v)
    {
        os_ << '\'' << v << '\'';
    }

    void operator()(null_type)
    {
        os_ << "(NULL)";
    }

    void operator()(const std::tm& v)
    {
        os_ << '\'' << format_time(v) << '\'';
    }

    void operator()(std::istream*)
    {
        os_ << "(BLOB)";
    }

private:
    std::ostream& os_;
};

}

void statement_stat::bind(const string_ref& name, const bind_types_variant& val)
{
    if (session_stat_->user_monitor())
    {
        bindings_ << "['" << name << "', ";
        dump_to_ostream vis(bindings_);
        val.apply_visitor(vis);
        bindings_ << ']';
    }
}

void statement_stat::bind(int col, const bind_types_variant& val)
{
    if (session_stat_->user_monitor())
    {
        bindings_ << '[' << col << ", ";
        dump_to_ostream vis(bindings_);
        val.apply_visitor(vis);
        bindings_ << ']';
    }
}

void statement_stat::reset_bindings()
{
    if (session_stat_->user_monitor())
        bindings_.str("");
}

statement_stat::measure_query::measure_query(
    statement_stat* stat, const std::string* query, result_ptr* r
  )
  : stat_(stat)
  , query_(query)
  , r_(r)
{
    stat_->timer_.restart();
}

statement_stat::measure_query::~measure_query()
{
    double execution_time = stat_->timer_.elapsed();
    stat_->session_stat_->add_query_time(execution_time);

    if (stat_->session_stat_->user_monitor())
    {
        bool succeded = !std::uncaught_exception();
        uint64_t rows = succeded ? (*r_)->rows() : uint64_t(-1);

        try 
        {
            stat_->session_stat_->user_monitor()->query_executed(
                query_->c_str(), stat_->bindings_.str(), succeded, execution_time, rows);
        }
        catch(...)
        {
            // Rethrow only if there is no active exception already
            // Althouth exception is thrown from destructor it should break everything 
            // while measure_query object is kept on stack
            if(succeded)
                throw;
        }
    }
}

statement_stat::measure_statement::measure_statement(
    statement_stat* stat, const std::string* query, statement_iface* st
  )
  : stat_(stat)
  , query_(query)
  , st_(st)
{
    stat_->timer_.restart();
}

statement_stat::measure_statement::~measure_statement()
{
    double execution_time = stat_->timer_.elapsed();
    stat_->session_stat_->add_query_time(execution_time);

    if (stat_->session_stat_->user_monitor())
    {
        bool succeded = !std::uncaught_exception();
        uint64_t affected = succeded ? st_->affected() : 0;

        try 
        {
            stat_->session_stat_->user_monitor()->statement_executed(
                query_->c_str(), stat_->bindings_.str(), succeded, execution_time, affected);
        }
        catch(...)
        {
            // Rethrow only if there is now active exception already
            // Althouth exception is thrown from destructor it should break everything while measure_query object is kept on stack
            if(succeded)
                throw;
        }
    }
}

}}
