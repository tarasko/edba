#ifndef EDBA_BACKEND_INTERFACES_HPP
#define EDBA_BACKEND_INTERFACES_HPP

#include <edba/types.hpp>
#include <edba/string_ref.hpp>

#include <boost/any.hpp>

#include <string>

namespace edba { namespace backend {

///
/// Tries to load dynamic driver and return connect function
///
EDBA_API connect_function_type get_connect_function(const char* path, const char* entry_func_name);

struct result_iface : ref_cnt
{
public:
    ///
    /// The flag that defines the information about availability of the next row in result
    ///
    typedef enum {
        last_row_reached, ///< No more rows exits, next() would return false
        next_row_exists,  ///< There are more rows, next() would return true
        next_row_unknown  ///< It is unknown, next() may return either true or false
    } next_row;

    virtual ~result_iface() {}

    ///
    /// Check if the next row in the result exists. If the DB engine can't perform
    /// this check without loosing data for current row, it should return next_row_unknown.
    ///
    virtual next_row has_next() = 0;
    ///
    /// Move to next row. Should be called before first access to any of members. If no rows remain
    /// return false, otherwise return true
    ///
    virtual bool next() = 0;

    ///
    /// Fetch value with type currently stored in variant for column \a starting from 0.
    /// Returns true if ok, returns false if the column value is NULL and the referenced object should remain unchanged
    ///
    /// Should throw invalid_column() \a col value is invalid, should throw bad_value_cast() if the underlying data
    /// can't be converted to type currently stored in variant.
    ///
    virtual bool fetch(int col, const fetch_types_variant& v) = 0;

    ///
    /// Return true if value is null at specified column
    ///
    virtual bool is_null(int col) = 0;

    ///
    /// Return the number of columns in the result. Should be valid even without calling next() first time.
    ///
    virtual int cols() = 0;
    ///
    /// Return the number of rows in the result. Return boost::uint64_t(-1) if backend doesn`t support
    /// this feature
    ///
    virtual boost::uint64_t rows() = 0;
    ///
    /// Return the number of columns by its name. Return -1 if the name is invalid
    /// Should be able to work even without calling next() first time.
    ///
    virtual int name_to_column(const string_ref&) = 0;
    ///
    /// Return the column name for column index starting from 0.
    /// Should throw invalid_column() if the index out of range
    /// Should be able to work even without calling next() first time.
    ///
    virtual std::string column_to_name(int) = 0;
};

struct statement_iface : ref_cnt
{
    virtual ~statement_iface() {}

    ///
    /// Bind value to column \a col (starting from 1).
    ///
    /// Dispatch call to suitable implementation
    ///
    virtual void bind(int col, const bind_types_variant& val) = 0;

    ///
    /// Bind value to column by name.
    ///
    /// Dispatch call to suitable implementation
    ///
    virtual void bind(const string_ref& name, const bind_types_variant& val) = 0;

    ///
    /// Reset all bindings to initial state
    ///
    virtual void reset_bindings() = 0;

    ///
    /// Return query that is scheduled for execution by backend after all possible transformations
    ///
    virtual const std::string& patched_query() const = 0;

    ///
    /// Return SQL Query result, MAY throw edba_error if the statement is not a query
    ///
    virtual boost::intrusive_ptr<result_iface> run_query() = 0;

    ///
    /// Execute a statement, MAY throw edba_error if the statement returns results.
    ///
    virtual void run_exec() = 0;

    ///
    /// Fetch the last sequence generated for last inserted row. May use sequence as parameter
    /// if the database uses sequences, should ignore the parameter \a sequence if the last
    /// id is fetched without parameter.
    ///
    /// Should be called after exec() for insert statement, otherwise the behavior is undefined.
    ///
    /// MUST throw not_supported_by_backend() if such option is not supported by the DB engine.
    ///
    virtual long long sequence_last(std::string const &sequence) = 0;

    ///
    /// Return the number of affected rows by last statement.
    ///
    /// Should be called after exec(), otherwise behavior is undefined.
    ///
    virtual unsigned long long affected() = 0;
};

struct connection_iface : public ref_cnt
{
    virtual ~connection_iface() {}

    ///
    /// Try get already compiled statement from the cache. If failed then use prepare_statement_impl
    /// to create prepared statement. \a q. May throw if preparation had failed.
    /// Should never return null value.
    ///
    virtual statement_ptr prepare_statement(const string_ref& q) = 0;

    ///
    /// Create a (unprepared) statement \a q. May throw if had failed.
    /// Should never return null value.
    ///
    virtual statement_ptr create_statement(const string_ref& q) = 0;

    ///
    /// Executes commands batch in one shot
    ///
    virtual void exec_batch(const string_ref& q) = 0;

    ///
    /// Set connection specific data
    ///
    virtual void set_specific(const boost::any& data) = 0;

    ///
    /// Get connection specific data
    ///
    virtual boost::any& get_specific() = 0;

    // API

    ///
    /// Start new isolated transaction. Would not be called
    /// withing other transaction on current connection.
    ///
    virtual void begin() = 0;
    ///
    /// Commit the transaction, you may assume that is called after begin()
    /// was called.
    ///
    virtual void commit() = 0;
    ///
    /// Rollback the transaction. MUST never throw!!!
    ///
    virtual void rollback() = 0;

    ///
    /// Escape a string for inclusion in SQL query. May throw not_supported_by_backend() if not supported by backend.
    ///
    virtual std::string escape(const string_ref& str) = 0;

    ///
    /// Get the name of the edba backend, for example sqlite3, odbc
    ///
    virtual const std::string& backend() = 0;
    ///
    /// Get the name of the SQL Server
    ///
    virtual const std::string& engine() = 0;
    ///
    /// Get the SQL Server version
    ///
    virtual void version(int& major, int& minor) = 0;
    ///
    /// Get human readable string describing SQL Server, usefull for logging
    ///
    virtual const std::string& description() = 0;
    ///
    /// Return total time spent on executing queries and statements
    ///
    virtual double total_execution_time() const = 0;
    ///
    /// Return conn_info object provided for connection during construction
    ///
    virtual const conn_info& connection_info() const = 0;
};

}} // namespace edba, backend

#endif // EDBA_BACKEND_INTERFACES_HPP
