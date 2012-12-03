#ifndef EDBA_DRIVER_ORACLE_HPP
#define EDBA_DRIVER_ORACLE_HPP

#include <edba/types.hpp>

extern "C" edba::backend::connection_iface* edba_oracle_get_connection(const edba::conn_info& cs, edba::session_monitor* sm);

namespace edba { namespace driver {

struct oracle
{
    backend::connection_ptr operator()(const conn_info& ci, session_monitor* sm) const
    {
        connect_function_type f = backend::get_connect_function("edba_oracle", "edba_oracle_get_connection");
        return backend::connection_ptr(f(ci, sm));
    }
};

struct oracle_s
{
    backend::connection_ptr operator()(const conn_info& ci, session_monitor* sm) const
    {
        return backend::connection_ptr(edba_oracle_get_connection(ci, sm));
    }
};

}}

#endif // EDBA_DRIVER_ORACLE_HPP
