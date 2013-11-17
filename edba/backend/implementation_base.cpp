#include <edba/backend/implementation_base.hpp>
#include <edba/session_monitor.hpp>
#include <edba/conn_info.hpp>

#include <edba/detail/utils.hpp>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/range/algorithm/equal_range.hpp>
#include <boost/range/algorithm/sort.hpp>
#include <boost/typeof/typeof.hpp>
#include <boost/timer.hpp>

#include <map>
#include <list>


#if defined(_WIN32)
#  include <windows.h>
#  define RTLD_LAZY 0

namespace {

void *dlopen(char const *name,int /*unused*/)
{
    return LoadLibrary(name);
}
void dlclose(void *h)
{
    HMODULE m=(HMODULE)(h);
    FreeLibrary(m);
}
void *dlsym(void *h,char const *sym)
{
    HMODULE m=(HMODULE)(h);
    return (void *)GetProcAddress(m,sym);
}

}

#else
#	include <dlfcn.h>
#endif

namespace edba { namespace backend {

EDBA_ADD_INTRUSIVE_PTR_SUPPORT_FOR_TYPE_IMPL(result_iface)
EDBA_ADD_INTRUSIVE_PTR_SUPPORT_FOR_TYPE_IMPL(statement_iface)
EDBA_ADD_INTRUSIVE_PTR_SUPPORT_FOR_TYPE_IMPL(connection_iface)

connect_function_type get_connect_function(const char* path, const char* entry_func_name)
{
    assert("Path not null" && path);

    void* module = dlopen(path, RTLD_LAZY);

    if (!module)
        throw edba_error("edba::loadable_driver::failed to load " + std::string(path));

    connect_function_type f = reinterpret_cast<connect_function_type>(
        dlsym(module, entry_func_name)
      );

    if (!f)
    {
        dlclose(module);
        throw edba_error("edba::loadable_driver::failed to get " + std::string(entry_func_name) + " address in " + std::string(path));
    }

    return f;
}

//////////////
//statement
//////////////

statement::statement(session_stat* sess_stat)
  : stat_(sess_stat)
{
}

void statement::bind(int col, const bind_types_variant& val)
{
    bind_impl(col, val);
    stat_.bind(col, val);
}

void statement::bind(const string_ref& name, const bind_types_variant& val)
{
    bind_impl(name, val);
    stat_.bind(name, val);
}

void statement::reset_bindings()
{
    reset_bindings_impl();
    stat_.reset_bindings();
}

result_ptr statement::run_query()
{
    result_ptr r;
    {
        statement_stat::measure_query m(&stat_, &patched_query(), &r);
        r = query_impl();
    }
    return r;
}

void statement::run_exec()
{
    statement_stat::measure_statement m(&stat_, &patched_query(), this);
    exec_impl();
}

//////////////
//connection
//////////////

string_ref connection::select_statement(const string_ref& _q)
{
    string_ref q;

    if(expand_conditionals_)
    {
        int major;
        int minor;
        version(major, minor);
        q = ::edba::select_statement(_q, engine(), major, minor);
    }
    else
        q = _q;

    return q;
}

statement_ptr connection::prepare_statement(const string_ref& _q)
{
    string_ref q = select_statement(_q);

    if (q.empty())
        return statement_ptr();

    BOOST_AUTO(found, (boost::equal_range(cache_, q, string_ref_less())));
    if (boost::empty(found))
    {
        statement_ptr st = prepare_statement_impl(q);
        cache_.resize(cache_.size() + 1);
        cache_.back().first.assign(q.begin(), q.end());
        cache_.back().second = st;
        boost::sort(cache_, string_ref_less());
        return st;
    }
    else
    {
        found.first->second->reset_bindings();
        return found.first->second;
    }
}

void connection::before_destroy()
{
    cache_.clear();
}

statement_ptr connection::create_statement(const string_ref& _q)
{
    string_ref q = select_statement(_q);
    return q.empty() ? statement_ptr() : create_statement_impl(q);
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

void connection::set_specific(const boost::any& data)
{
    specific_data_ = data;
}

boost::any& connection::get_specific()
{
    return specific_data_;
}

void connection::begin()
{
    begin_impl();
    
    try 
    { 
        stat_.transaction_started();
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
    stat_.transaction_commited();
}

void connection::rollback()
{
    rollback_impl();
    stat_.transaction_reverted();
}

double connection::total_execution_time() const
{
    return stat_.total_execution_time();
}

connection::connection(conn_info const &info, session_monitor* sm) : stat_(sm)
{
    const std::locale& loc = std::locale::classic();
    string_ref exp_cond = info.get("@expand_conditionals", "on");
    if(boost::algorithm::iequals(exp_cond, "on", loc))
        expand_conditionals_ = 1;
    else if(boost::algorithm::iequals(exp_cond, "off", loc))
        expand_conditionals_ = 0;
    else
        throw edba_error("edba::backend::connection: @expand_conditionals should be either 'on' or 'off'");

}

}}
