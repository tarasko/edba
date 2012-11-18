#ifndef EDBA_DRIVER_SQLITE3_HPP
#define EDBA_DRIVER_SQLITE3_HPP

#include <edba/types.hpp>

extern "C" edba::backend::connection* edba_sqlite3_get_connection(const edba::conn_info& cs, edba::session_monitor* sm);

namespace edba { namespace driver {

struct sqlite3
{
    boost::intrusive_ptr<backend::connection> operator()(const conn_info& ci, session_monitor* sm) const
    {
        connect_function_type f = backend::get_connect_function("edba_sqlite3", "edba_sqlite3_get_connection");
        return boost::intrusive_ptr<backend::connection>(f(ci, sm));
    }
};

struct sqlite3_s
{
    boost::intrusive_ptr<backend::connection> operator()(const conn_info& ci, session_monitor* sm) const
    {
        return boost::intrusive_ptr<backend::connection>(edba_sqlite3_get_connection(ci, sm));
    }
};

}}

#endif // EDBA_DRIVER_SQLITE3_HPP
