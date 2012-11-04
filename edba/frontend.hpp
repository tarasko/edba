#ifndef EDBA_FRONTEND_H
#define EDBA_FRONTEND_H

#include <edba/conn_info.hpp>
#include <edba/result.hpp>
#include <edba/statement.hpp>
#include <edba/session.hpp>
#include <edba/transaction.hpp>
#include <edba/errors.hpp>
#include <edba/utils.hpp>
#include <edba/backend/backend.hpp>

#include <boost/intrusive_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/noncopyable.hpp>
#include <boost/call_traits.hpp>

#include <iosfwd>
#include <ctime>
#include <string>
#include <map>
#include <memory>

///
/// The namespace of all data related to the edba api
///

namespace edba {

extern "C" {
    ///
    /// This function type is the function that is generally resolved from the shared objects when loaded
    ///
    typedef edba::backend::connection *edba_backend_connect_function(const conn_info& cs, session_monitor* sm);
}

///
/// \brief Interface for loadable and static drivers
///
struct EDBA_API driver : boost::noncopyable 
{
    virtual ~driver() {}

    ///
    /// Create a session object from connection string
    ///
    session open(const char* cs, session_monitor* sm = 0)
    {
        return open(conn_info(cs), sm);
    }

    ///
    /// Create a session object from parsed connection string - should be implemented by driver
    ///
    virtual session open(const conn_info& ci, session_monitor* sm) = 0;


protected:
    ///
    /// Typedef of the function pointer that is used for creation of connection objects.
    ///
    typedef edba_backend_connect_function *connect_function_type;
};

///
/// \brief This class represents a driver that can be loaded as external so or dll
///
class EDBA_API loadable_driver : public driver 
{
public:
    ///
    /// \brief Load shared library
    ///
    /// \param path - Path to dynamic library
    /// \param engine - Explicit name of backend (used to form correct name to entry function)
    /// (for example 'sqlite3', 'odbc', 'postgres', 'mssql')
    ///
    loadable_driver(const char* path, const char* engine);

    ///
    /// \brief Unload driver.
    ///
    /// Be carefull, after this all sessions objects become invalid and
    /// their methods calls will lead to access violations and crashes
    ///
    ~loadable_driver();

    ///
    /// Create a session object from parsed connection string - should be implemented by driver
    ///
    virtual session open(const conn_info& ci, session_monitor* sm = 0);

private:
    void* module_;
    connect_function_type connect_;
};

///
/// \brief Create a static driver using connection function (usable for statically linking drivers).
///
class EDBA_API static_driver : public driver 
{
public:
    ///
    /// Create a new driver that creates connection using function \a c
    ///
    static_driver(connect_function_type c) : connect_(c) {}

    ///
    /// Create new connection - basically calls the function to create the object
    ///
    virtual session open(const conn_info& ci, session_monitor* sm = 0);

private:
    connect_function_type connect_;
};

} // edba

#endif
