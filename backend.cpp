#define EDBA_SOURCE

#include <edba/backend.hpp>
#include <edba/utils.hpp>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/timer.hpp>

#include <map>
#include <list>

namespace edba { namespace backend {

EDBA_ADD_INTRUSIVE_PTR_SUPPORT_FOR_TYPE_IMPL(result)
EDBA_ADD_INTRUSIVE_PTR_SUPPORT_FOR_TYPE_IMPL(statement)
EDBA_ADD_INTRUSIVE_PTR_SUPPORT_FOR_TYPE_IMPL(connection)

//////////////
//statement
//////////////

// Begin of API
statement::statement(session_monitor* sm, const chptr_range& orig_sql) : 
    sm_(sm)
  , orig_sql_(orig_sql.begin(), orig_sql.end())
{
}

///
/// Reset the prepared statement to initial state as before the operation. It is
/// called by front-end each time before new query() or exec() are called.
///
void statement::reset()
{
    reset_impl();
    if (sm_)
        bindings_.str("");
}

void statement::bind(int col, const std::tm& val)
{
    bind_impl(col, val);
    if (sm_)
        bindings_ << '\'' << format_time(val) << "' ";
}

void statement::bind(int col,std::istream & s)
{
    bind_impl(col, s);
    if (sm_)
        bindings_ << "'BLOB' ";
}

void statement::bind_null(int col)
{
    bind_null_impl(col);
    if (sm_)
        bindings_ << "'NULL' ";
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
            sm_->query_executed(orig_sql_, bindings_.str(), false, t.elapsed(), 0);
            throw;
        }

        // TODO: Add way to evaluate number of rows
        sm_->query_executed(orig_sql_, bindings_.str(), true, t.elapsed(), r->rows());
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
            sm_->statement_executed(orig_sql_, bindings_.str(), false, t.elapsed(), 0);
            throw;
        }

        sm_->statement_executed(orig_sql_, bindings_.str(), true, t.elapsed(), affected());
    }
    else
        exec_impl();
}

//////////////
//connection
//////////////

boost::intrusive_ptr<statement> connection::prepare(const chptr_range& q) 
{
    if(default_is_prepared_)
        return get_prepared_statement(q);
    else
        return get_statement(q);
}

boost::intrusive_ptr<statement> connection::get_statement(const chptr_range& _q)
{
    chptr_range q;

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

boost::intrusive_ptr<statement> connection::get_prepared_statement(const chptr_range& _q)
{
    chptr_range q;

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

void connection::exec_batch(const chptr_range& _q)
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
    chptr_range def_is_prep = info.get("@use_prepared","on");
    if(boost::algorithm::iequals(def_is_prep, "on"))
        default_is_prepared_ = 1;
    else if(boost::algorithm::iequals(def_is_prep, "off"))
        default_is_prepared_ = 0;
    else
        throw edba_error("edba::backend::connection: @use_prepared should be either 'on' or 'off'");

    chptr_range exp_cond = info.get("@expand_conditionals", "on");
    if(boost::algorithm::iequals(exp_cond, "on"))
        expand_conditionals_ = 1;
    else if(boost::algorithm::iequals(exp_cond, "off"))
        expand_conditionals_ = 0;
    else
        throw edba_error("edba::backend::connection: @expand_conditionals should be either 'on' or 'off'");

}

}} // edba, backend


