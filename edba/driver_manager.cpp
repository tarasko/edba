#include <edba/driver_manager.hpp>

#include <boost/preprocessor/stringize.hpp>
#include <boost/preprocessor/cat.hpp>
#include <boost/preprocessor/if.hpp>

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
    HMODULE m = (HMODULE)(h);
    FreeLibrary(m);
}
void *dlsym(void *h,char const *sym)
{
    HMODULE m = (HMODULE)(h);
    return (void *)GetProcAddress(m,sym);
}

}

#else
#	include <dlfcn.h>
#endif

namespace edba { 
namespace {

extern "C" 
{
    edba::backend::connection_iface* edba_sqlite3_get_connection(const edba::conn_info& cs, edba::session_monitor* sm);
    edba::backend::connection_iface* edba_oracle_get_connection(const edba::conn_info& cs, edba::session_monitor* sm);
    edba::backend::connection_iface* edba_mysql_get_connection(const edba::conn_info& cs, edba::session_monitor* sm);
    edba::backend::connection_iface* edba_odbc_get_connection(const edba::conn_info& cs, edba::session_monitor* sm);
    edba::backend::connection_iface* edba_postgresql_get_connection(const edba::conn_info& cs, edba::session_monitor* sm);
}

connect_function_type get_connect_function(const char* path, const char* entry_func_name)
{
    assert("Path not null" && path);

#ifdef _WIN32
    void* module = dlopen(path, RTLD_LAZY);
#else
    void* module = dlopen(path, RTLD_LAZY | RTLD_GLOBAL);
#endif

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

} // namespace {

backend::connection_ptr driver_manager::create_conn(const conn_info& ci, session_monitor* sm)
{
#define EDBA_MAKE_BACKEND_LIB_NAME(Name) EDBA_BACKEND_LIB_PREFIX "edba_" BOOST_PP_STRINGIZE(Name) EDBA_BACKEND_LIB_SUFFIX
#define EDBA_MAKE_BACKEND_FUNC_NAME(Name) BOOST_PP_CAT(BOOST_PP_CAT(edba_, Name), _get_connection)

#if defined(EDBA_BACKEND_SHARED)
#define EDBA_CREATE_CONN(Name) \
    connect_function_type f = get_connect_function(EDBA_MAKE_BACKEND_LIB_NAME(Name), BOOST_PP_STRINGIZE(EDBA_MAKE_BACKEND_FUNC_NAME(Name))); \
    return backend::connection_ptr(f(ci, sm));
#else
#define EDBA_CREATE_CONN(Name) return backend::connection_ptr(EDBA_MAKE_BACKEND_FUNC_NAME(Name)(ci, sm));
#endif

#define EDBA_PROCESS_BACKEND(Name) \
    if (ci.driver_name() == boost::as_literal(BOOST_PP_STRINGIZE(Name))) \
    { \
        EDBA_CREATE_CONN(Name) \
    }

    EDBA_PROCESS_BACKEND(sqlite3);
    EDBA_PROCESS_BACKEND(odbc);
    EDBA_PROCESS_BACKEND(oracle);
    EDBA_PROCESS_BACKEND(postgresql);
    EDBA_PROCESS_BACKEND(mysql);

    throw invalid_connection_string(to_string(ci.driver_name()) + " - unknown backend");
}

}
