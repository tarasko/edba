#ifndef EDBA_DRIVER_SQLITE3_HPP
#define EDBA_DRIVER_SQLITE3_HPP

#include <edba/backend/interfaces.hpp>

extern "C" edba::backend::connection_iface* edba_sqlite3_get_connection(const edba::conn_info& cs, edba::session_monitor* sm);

namespace edba { namespace driver {

struct sqlite3
{
    backend::connection_ptr operator()(const conn_info& ci, session_monitor* sm) const
    {
        connect_function_type f = backend::get_connect_function(EDBA_MAKE_BACKEND_LIB_NAME(edba_sqlite3), "edba_sqlite3_get_connection");
        return backend::connection_ptr(f(ci, sm));
    }
};

struct sqlite3_s
{
    backend::connection_ptr operator()(const conn_info& ci, session_monitor* sm) const
    {
        return backend::connection_ptr(edba_sqlite3_get_connection(ci, sm));
    }
};

}}

#endif // EDBA_DRIVER_SQLITE3_HPP
