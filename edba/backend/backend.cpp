#include <edba/backend/backend.hpp>
#include <edba/utils.hpp>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/timer.hpp>

#include <map>
#include <list>

namespace edba { namespace backend {

EDBA_ADD_INTRUSIVE_PTR_SUPPORT_FOR_TYPE_IMPL(result)
EDBA_ADD_INTRUSIVE_PTR_SUPPORT_FOR_TYPE_IMPL(bindings)
EDBA_ADD_INTRUSIVE_PTR_SUPPORT_FOR_TYPE_IMPL(statement)
EDBA_ADD_INTRUSIVE_PTR_SUPPORT_FOR_TYPE_IMPL(connection)

//////////////
//bindings
//////////////

namespace {

struct dump_to_ostream : boost::static_visitor<>
{
    dump_to_ostream(std::ostream& os) : os_(os) {}

    template<typename T>
    void operator()(const T& v)
    {
        os_ << '\'' << v << "' ";
    }

    void operator()(null_type)
    {
        os_ << "(NULL)";
    }

    void operator()(const std::tm& v)
    {
        os_ << '\'' << format_time(v) << "' ";
    }

    void operator()(std::istream*)
    {
        os_ << "(BLOB)";
    }

private:
    std::ostream& os_;
};

}

void bindings::bind(int col, const bind_types_variant& val)
{
    bind_impl(col, val);
    dump_to_ostream vis(bindings_);
    val.apply_visitor(vis);
}

void bindings::bind(const string_ref& name, const bind_types_variant& val)
{
    bind_impl(name, val);
    dump_to_ostream vis(bindings_);
    val.apply_visitor(vis);
}

void bindings::reset()
{
    reset_impl();
    bindings_.str("");
}

std::string bindings::to_string() const
{
    return bindings_.str();
}

//////////////
//statement
//////////////

// Begin of API
statement::statement(session_monitor* sm, const string_ref& orig_sql) : 
    sm_(sm)
  , orig_sql_(orig_sql.begin(), orig_sql.end())
{
}

boost::intrusive_ptr<result> statement::query()
{
    if (sm_)
    {
        boost::timer t;
        boost::intrusive_ptr<result> r;
        try 
        {
            r = query_impl();
        }
        catch(...)
        {
            sm_->query_executed(orig_sql_, bindings_->to_string(), false, t.elapsed(), 0);
            throw;
        }

        // TODO: Add way to evaluate number of rows
        sm_->query_executed(orig_sql_, bindings_->to_string(), true, t.elapsed(), r->rows());
        return r;
    }
    else
        return query_impl();
}

void statement::exec()
{
    if (sm_)
    {
        boost::timer t;
        try 
        {
            exec_impl();
        }
        catch(...)
        {
            sm_->statement_executed(orig_sql_, bindings_->to_string(), false, t.elapsed(), 0);
            throw;
        }

        sm_->statement_executed(orig_sql_, bindings_->to_string(), true, t.elapsed(), affected());
    }
    else
        exec_impl();
}

//////////////
//connection
//////////////

boost::intrusive_ptr<statement> connection::prepare(const string_ref& q) 
{
    if(default_is_prepared_)
        return get_prepared_statement(q);
    else
        return get_statement(q);
}

boost::intrusive_ptr<statement> connection::get_statement(const string_ref& _q)
{
    string_ref q;

    if(expand_conditionals_)
    {
        int major;
        int minor;
        version(major, minor);
        q = select_statement(q, engine(), major, minor);
    }
    else
        q = _q;

    return q.empty() ? boost::intrusive_ptr<statement>() : create_statement(q);
}

boost::intrusive_ptr<statement> connection::get_prepared_statement(const string_ref& _q)
{
    string_ref q;

    if(expand_conditionals_)
    {
        int major;
        int minor;
        version(major, minor);
        q = select_statement(_q, engine(), major, minor);
    }
    else
        q = _q;

    return q.empty() ? boost::intrusive_ptr<statement>() : prepare_statement(q);
}

void connection::exec_batch(const string_ref& _q)
{
    if(expand_conditionals_)
    {
        int major;
        int minor;
        version(major, minor);
        exec_batch_impl(select_statements_in_batch(_q, engine(), major, minor));
    }
    else 
        exec_batch_impl(_q);
}

void connection::begin()
{
    begin_impl();

    if(sm_) try 
    { 
        sm_->transaction_started(); 
    } 
    catch(...)
    {
        rollback_impl();
        throw;
    }
}
void connection::commit()
{
    commit_impl();

    if(sm_) 
        sm_->transaction_committed();
}
void connection::rollback()
{
    rollback_impl();

    if(sm_) try
    {
        sm_->transaction_reverted();
    }
    catch(...)
    {
    }
}

connection::connection(conn_info const &info, session_monitor* sm) : sm_(sm)
{
    string_ref def_is_prep = info.get("@use_prepared","on");
    if(boost::algorithm::iequals(def_is_prep, "on"))
        default_is_prepared_ = 1;
    else if(boost::algorithm::iequals(def_is_prep, "off"))
        default_is_prepared_ = 0;
    else
        throw edba_error("edba::backend::connection: @use_prepared should be either 'on' or 'off'");

    string_ref exp_cond = info.get("@expand_conditionals", "on");
    if(boost::algorithm::iequals(exp_cond, "on"))
        expand_conditionals_ = 1;
    else if(boost::algorithm::iequals(exp_cond, "off"))
        expand_conditionals_ = 0;
    else
        throw edba_error("edba::backend::connection: @expand_conditionals should be either 'on' or 'off'");

}

}} // edba, backend


