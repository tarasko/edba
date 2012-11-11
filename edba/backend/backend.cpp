#include <edba/backend/backend.hpp>
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

EDBA_ADD_INTRUSIVE_PTR_SUPPORT_FOR_TYPE_IMPL(result)
EDBA_ADD_INTRUSIVE_PTR_SUPPORT_FOR_TYPE_IMPL(statement)
EDBA_ADD_INTRUSIVE_PTR_SUPPORT_FOR_TYPE_IMPL(connection)

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
//bindings
//////////////

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

bindings::bindings() : enable_recording_(false)
{
}

void bindings::bind(int col, const bind_types_variant& val)
{
    bind_impl(col, val);
    if (enable_recording_)
    {
        bindings_ << '[' << col << ", ";
        dump_to_ostream vis(bindings_);
        val.apply_visitor(vis);
        bindings_ << ']';
    }
}

void bindings::bind(const string_ref& name, const bind_types_variant& val)
{
    bind_impl(name, val);
    if (enable_recording_)
    {
        bindings_ << "['" << name << "', ";
        dump_to_ostream vis(bindings_);
        val.apply_visitor(vis);
        bindings_ << ']';
    }
}

void bindings::reset()
{
    reset_impl();
    if (enable_recording_)
        bindings_.str("");
}

std::string bindings::to_string() const
{
    return bindings_.str();
}

void bindings::enable_recording(bool enable)
{
    enable_recording_ = enable;
}

bool bindings::enable_recording() const
{
    return enable_recording_;
}

//////////////
//statement
//////////////

// Begin of API
statement::statement(session_monitor* sm, const string_ref& orig_sql) : 
    sm_(sm)
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
            sm_->query_executed(orig_sql(), bindings().to_string(), false, t.elapsed(), 0);
            throw;
        }

        // TODO: Add way to evaluate number of rows
        sm_->query_executed(orig_sql(), bindings().to_string(), true, t.elapsed(), r->rows());
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
            sm_->statement_executed(orig_sql(), bindings().to_string(), false, t.elapsed(), 0);
            throw;
        }

        sm_->statement_executed(orig_sql(), bindings().to_string(), true, t.elapsed(), affected());
    }
    else
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

boost::intrusive_ptr<statement> connection::prepare_statement(const string_ref& _q)
{
    string_ref q = select_statement(_q);
    BOOST_AUTO(found, (boost::equal_range(cache_, q, string_ref_iless())));
    if (boost::empty(found))
    {
        boost::intrusive_ptr<statement> st = prepare_statement_impl(q);
        cache_.resize(cache_.size() + 1);
        cache_.back().first.assign(q.begin(), q.end());
        cache_.back().second = st;
        boost::sort(cache_, string_ref_iless());
        return st;
    }
    else
    {
        found.first->second->bindings().reset();
        return found.first->second;
    }
}

boost::intrusive_ptr<statement> connection::create_statement(const string_ref& _q)
{
    string_ref q = select_statement(_q);
    return create_statement_impl(q);
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
    string_ref exp_cond = info.get("@expand_conditionals", "on");
    if(boost::algorithm::iequals(exp_cond, "on"))
        expand_conditionals_ = 1;
    else if(boost::algorithm::iequals(exp_cond, "off"))
        expand_conditionals_ = 0;
    else
        throw edba_error("edba::backend::connection: @expand_conditionals should be either 'on' or 'off'");

}

}} // edba, backend


