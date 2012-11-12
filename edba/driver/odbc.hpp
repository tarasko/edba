#ifndef EDBA_DRIVER_ODBC_HPP
#define EDBA_DRIVER_ODBC_HPP

#include <edba/types.hpp>

extern "C" edba::backend::connection* edba_odbc_get_connection(const edba::conn_info& cs, edba::session_monitor* sm);

namespace edba { namespace driver {

struct odbc
{
    boost::intrusive_ptr<backend::connection> open_connection(const conn_info& ci, session_monitor* sm) const
    {
        connect_function_type f = backend::get_connect_function("edba_odbc", "edba_odbc_get_connection");
        return boost::intrusive_ptr<backend::connection>(f(ci, sm));
    }
};

struct odbc_s 
{
    boost::intrusive_ptr<backend::connection> open_connection(const conn_info& ci, session_monitor* sm) const
    {
        return boost::intrusive_ptr<backend::connection>(edba_odbc_get_connection(ci, sm));
    }
};

}}

#endif // EDBA_DRIVER_ODBC_HPP
