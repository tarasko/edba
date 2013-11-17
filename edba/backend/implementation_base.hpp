#ifndef EDBA_BACKEND_IMPLEMENTATION_BASE_HPP
#define EDBA_BACKEND_IMPLEMENTATION_BASE_HPP

#include <edba/backend/interfaces.hpp>
#include <edba/backend/statistics.hpp>
#include <edba/conn_info.hpp>

#include <vector>
#include <utility>
#include <string>

namespace edba { namespace backend {

class result : public result_iface 
{
};

class EDBA_API statement : public statement_iface
{
protected:
    statement(session_stat* stat);

    ///
    /// Bind variant value to column \a col (starting from 1).
    ///
    /// Should throw invalid_column() if the value of col is out of range. May
    /// ignore if it is impossible to know whether the placeholder exists without special
    /// support from back-end.
    ///
    /// May throw bad_value_cast() if the value out of supported range by the DB. 
    ///
    virtual void bind_impl(int col, bind_types_variant const& v) = 0;
    virtual void bind_impl(const string_ref& name, bind_types_variant const& v) = 0; 

    ///
    /// Reset all bindings
    ///
    virtual void reset_bindings_impl() = 0;

    ///
    /// Return SQL Query result, MAY throw edba_error if the statement is not a query
    ///
    virtual result_ptr query_impl() = 0;

    ///
    /// Execute a statement, MAY throw edba_error if the statement returns results.
    ///
    virtual void exec_impl() = 0;

public:
    ///
    /// Bind value to column \a col (starting from 1).
    /// 
    /// Dispatch call to suitable implementation
    ///
    void bind(int col, const bind_types_variant& val);

    ///
    /// Bind value to column by name.
    /// 
    /// Dispatch call to suitable implementation
    ///
    void bind(const string_ref& name, const bind_types_variant& val);

    ///
    /// Reset all bindings to initial state
    ///
    void reset_bindings();

    ///
    /// Return SQL Query result, MAY throw edba_error if the statement is not a query
    ///
    boost::intrusive_ptr<result_iface> run_query();

    ///
    /// Execute a statement, MAY throw edba_error if the statement returns results.
    ///
    void run_exec();

protected:    
    statement_stat stat_; 
};

class EDBA_API connection : public connection_iface 
{
protected:
    ///
    /// Create a prepared statement \a q. May throw if preparation had failed.
    /// Should never return null value.
    ///
    virtual statement_ptr prepare_statement_impl(const string_ref& q) = 0;

    ///
    /// Create a (unprepared) statement \a q. May throw if had failed.
    /// Should never return null value.
    ///
    virtual statement_ptr create_statement_impl(const string_ref& q) = 0;

    ///
    /// Executes commands batch in one shot
    ///
    virtual void exec_batch_impl(const string_ref& q) = 0;

    ///
    /// Start new isolated transaction. Would not be called
    /// withing other transaction on current connection.
    ///
    virtual void begin_impl() = 0;

    ///
    /// Commit the transaction, you may assume that is called after begin()
    /// was called.
    ///
    virtual void commit_impl() = 0;

    ///
    /// Rollback the transaction. MUST never throw!!!
    ///
    virtual void rollback_impl() = 0;

public:
    connection(conn_info const &info, session_monitor* sm);

    ///
    /// Try get already compiled statement from the cache. If failed then use prepare_statement_impl 
    /// to create prepared statement. \a q. May throw if preparation had failed.
    /// Should never return null value.
    /// 
    statement_ptr prepare_statement(const string_ref& q);

    ///
    /// Create a (unprepared) statement \a q. May throw if had failed.
    /// Should never return null value.
    ///    
    statement_ptr create_statement(const string_ref& q);
    
    ///
    /// Executes commands batch in one shot
    ///
    void exec_batch(const string_ref& q);

    ///
    /// Set connection specific data
    ///
    void set_specific(const boost::any& data);

    /// 
    /// Get connection specific data
    ///
    boost::any& get_specific();

    // API 

    ///
    /// Start new isolated transaction. Would not be called
    /// withing other transaction on current connection.
    ///
    void begin();
    ///
    /// Commit the transaction, you may assume that is called after begin()
    /// was called.
    ///
    void commit();
    ///
    /// Rollback the transaction. MUST never throw!!!
    ///
    void rollback();

    double total_execution_time() const;

protected:
    typedef std::vector< std::pair<std::string, statement_ptr > > stmt_map;

    void before_destroy();
    string_ref select_statement(const string_ref& _q);

    session_stat stat_;
    stmt_map cache_;                              // Statement cache
    boost::any specific_data_;                    // Connection specific data
    unsigned expand_conditionals_ : 1;            // If true then process query as list of backend specific queries
    unsigned reserved_ : 30;
};

}} // namespace edba, backend

#endif // EDBA_BACKEND_IMPLEMENTATION_BASE_HPP

