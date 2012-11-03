#include <edba/frontend.hpp>
#include <edba/backend/backend.hpp>

#include <boost/algorithm/string/find_iterator.hpp>
#include <boost/algorithm/string/finder.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/range/as_literal.hpp>
#include <boost/typeof/typeof.hpp>
#include <boost/foreach.hpp>

namespace edba {


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

loadable_driver::loadable_driver(const char* path, const char* driver)
{
    assert("Path not null" && path);

    module_ = dlopen(path, RTLD_LAZY);

    if (!module_)
        throw edba_error("edba::loadable_driver::failed to load " + std::string(path));

    std::string entry_func_name("edba_");
    entry_func_name += driver;
    entry_func_name += "_get_connection";

    connect_ = reinterpret_cast<connect_function_type>(
        dlsym(module_, entry_func_name.c_str())
        );

    if (!connect_)
    {
        dlclose(module_);
        throw edba_error("edba::loadable_driver::failed to get " + entry_func_name + " address in " + std::string(path));
    }
}

loadable_driver::~loadable_driver()
{
    if (module_)
        dlclose(module_);
}

session loadable_driver::open(const conn_info& ci, session_monitor* sm)
{
    return session(boost::intrusive_ptr<edba::backend::connection>(connect_(ci, sm)));
}

session static_driver::open(const conn_info& ci, session_monitor* sm)
{
    return session(boost::intrusive_ptr<edba::backend::connection>(connect_(ci, sm)));
}

}  // edba
