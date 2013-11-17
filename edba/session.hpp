#ifndef EDBA_SESSION_HPP
#define EDBA_SESSION_HPP

#include <edba/backend/interfaces.hpp>
#include <edba/statement.hpp>
#include <edba/conn_info.hpp>

namespace edba {

class session_pool;

/// \brief SQL session object that represents a single connection and is the gateway to SQL database
///
/// It is the main class that is used for access to the DB, it uses various singleton classes to
/// load drivers open connections and cache them. 
class session 
{
    struct once_type
    {
        once_type(session* sess) : sess_(sess) {}

        statement operator<<(const string_ref& q)
        {
            return sess_->create_statement(q);
        }

    private:
        session* sess_;
    };

public:   
    /// Create an empty session object, it should not be used until it is opened with calling open() function.
    session()
    {
    }

    /// Create a session using driver, connection string and optional session monitor
    template<typename Driver>
    session(Driver driver, const string_ref& conn_string, session_monitor* sm = 0)
      : conn_(driver(conn_string, sm))
    {
    }

    /// Open a session using driver, connection string and optional session monitor
    template<typename Driver>
    void open(Driver driver, const string_ref& conn_string, session_monitor* sm = 0)
    {
        conn_ = driver(conn_string, sm);
    }

    /// Close current connection.
    void close()
    {
        conn_.reset();
    }

    /// Check if the session was opened.
    bool is_open()
    {
        return conn_;
    }

    /// Try fetch statement from cache. If it doesn`t exist then create prepared statement, put it in cache and return.
    /// This is the most convenient function to create statements with.
    statement prepare_statement(const string_ref& q)
    {
        if (!conn_)
            throw empty_session("prepare_statement");

        backend::statement_ptr stmt(conn_->prepare_statement(q));
        return statement(conn_, stmt);
    }

    /// Create unprepared statement which is never cached. It should
    /// be used when such statement is executed rarely or very customized.
    statement create_statement(const string_ref& q)
    {
        if (!conn_)
            throw empty_session("create_statement");

        backend::statement_ptr stmt(conn_->create_statement(q));
        return statement(conn_, stmt);
    }

    /// Syntactic sugar for unprepared statements and their subsequent execution. Following two lines are equvalent
    /// \code
    /// sess.once() << query << use(somevar);
    /// sess.create_statement(query) << use(somevar) << exec;
    /// \endcode
    once_type once()
    {
        return once_type(this);
    }

    /// Execute list of sql commands as single request to database
    void exec_batch(const string_ref& q)
    {
        if (!conn_)
            throw empty_session("exec_batch");

        conn_->exec_batch(q);
    }

    /// Set session specific data
    template<typename T>
    void set_specific(const T& data)
    {
        if (!conn_)
            throw empty_session("set_specific");

        conn_->set_specific(data);
    }

    /// Get session specific data, will throw bad_value_cast if type is differenet from original one
    /// passed to set_specific
    template<typename T>
    T& get_specific()
    {
        if (!conn_)
            throw empty_session("get_specific");

        return boost::any_cast<T&>(conn_->get_specific());
    }
    
    /// Begin a transaction. Don't use it directly for RAII reasons. Use transaction class instead.
    void begin()
    {
        if (!conn_)
            throw empty_session("begin");

        conn_->begin();
    }

    /// Commit a transaction. Don't use it directly for RAII reasons. Use transaction class instead.
    void commit()
    {
        if (!conn_)
            throw empty_session("commit");

        conn_->commit();
    }

    /// Rollback a transaction. Don't use it directly for RAII reasons. Use transaction class instead.
    void rollback()
    {
        if (!conn_)
            throw empty_session("rollback");

        conn_->rollback();
    }

    /// Escape a string \a s for inclusion in SQL statement. It does not add quotation marks at beginning and end.
    /// It is designed to be used with text, don't use it with generic binary data.
    ///    
    /// Some backends (odbc) may not support this.
    std::string escape(const string_ref& str)
    {
        if (!conn_)
            throw empty_session("escape");

        return conn_->escape(str);
    }

    /// Get the backend name, as postgresql, odbc, sqlite3.
    /// Known backends are:
    /// - odbc
    /// - sqlite3
    /// - PgSQL
    /// - Oracle
    /// - MySQL
    /// 
    const std::string& backend()
    {
        if (!conn_)
            throw empty_session("backend");

        return conn_->backend();
    }

    /// Get an SQL database name, it may be not the same as driver name for multiple engine drivers like odbc.
    /// This names can be used inside of SQL scripts to write statements specific only for some engine
    /// Currently known engines are:
    /// - sqlite3
    /// - PgSQL
    /// - Oracle
    /// - MySQL
    /// - Microsoft SQL Server
    const std::string& engine()
    {
        if (!conn_)
            throw empty_session("engine");

        return conn_->engine();
    }

    /// Get the SQL Server version 
    void version(int& major, int& minor)
    {
        if (!conn_)
            throw empty_session("version");

        conn_->version(major, minor);
    }

    /// Get human readable string describing SQL Server, usefull for logging
    const std::string& description()
    {
        if (!conn_)
            throw empty_session("description");

        return conn_->description();
    }

    /// Return total time in seconds spent on executing statements and queries
    double total_execution_time() const
    {
        if (!conn_)
            throw empty_session("total_execution_time");

        return conn_->total_execution_time();
    }
    
    /// Equality operator
    friend bool operator==(const session& s1, const session& s2)
    {
        return s1.conn_ == s2.conn_;
    }
    
    /// Syntactic sugar, same as prepare(q)
    statement operator<<(string_ref query)
    {
        return prepare_statement(query);
    }    

private:
    friend class session_pool;

    session(const backend::connection_ptr& conn)
      : conn_(conn)
    {
    }

    backend::connection_ptr conn_;
};

}

#endif // EDBA_SESSION_HPP
