#ifndef EDBA_BACKEND_H
#define EDBA_BACKEND_H

#include <edba/backend/backend_fwd.hpp>
#include <edba/detail/exports.hpp>

#include <edba/conn_info.hpp>
#include <edba/errors.hpp>
#include <edba/session_monitor.hpp>
#include <edba/types.hpp>

#include <boost/noncopyable.hpp>

#include <iosfwd>
#include <ctime>
#include <string>
#include <memory>
#include <map>

namespace edba {

///
/// \brief This namepace includes all classes required to implement a edba SQL backend.
///
namespace backend {	

///
/// \brief This class represents query result.
///
/// This object is created by statement::query call, backend developer may assume that this object
/// will stay alive as long as statement that created it exits, i.e. statement would be destroyed after
/// result.
///
class EDBA_API result : public ref_cnt
{
public:
    virtual ~result() {}

    ///
    /// The flag that defines the information about availability of the next row in result
    ///
    typedef enum {
        last_row_reached, ///< No more rows exits, next() would return false
        next_row_exists,  ///< There are more rows, next() would return true
        next_row_unknown  ///< It is unknown, next() may return either true or false
    } next_row;

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
    /// can't be converted to type currently stored in variant. If variant currently has null_type than change 
    /// variant to hold appropriate type.
    ///
    virtual bool fetch(int col, fetch_types_variant& v) = 0;

    /// 
    /// Return true if value is null at specified column
    ///
    virtual bool is_null(int col) = 0;

    ///
    /// Return the number of columns in the result. Should be valid even without calling next() first time.
    ///
    virtual int cols() = 0;
    ///
    /// Return the number of rows in the result. Return unsigned long long(-1) if backend doesn`t support
    /// this feature
    ///
    virtual unsigned long long rows() = 0;
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

///
/// Represent statement bindings
///
class EDBA_API bindings 
{
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
    void reset();
    /// 
    /// Return serialized bindings string
    ///
    std::string to_string() const;

protected:
    ///
    /// Bind variant value to column \a col (starting from 1).
    ///
    /// Should throw invalid_placeholder() if the value of col is out of range. May
    /// ignore if it is impossible to know whether the placeholder exists without special
    /// support from back-end.
    ///
    /// May throw bad_value_cast() if the value out of supported range by the DB. 
    ///
    virtual void bind_impl(int col, bind_types_variant const& v) = 0;

   
    virtual void bind_impl(string_ref name, bind_types_variant const& v) = 0; 
    ///
    /// Reset all bindings
    ///
    virtual void reset_impl() = 0;

private:
    /// Accumulator for string representation of bounded parameters
    /// Used in session_monitor calls
    std::ostringstream bindings_;
};

///
/// \brief This class represents a statement that can be either executed or queried for result
/// 
/// It also stores session monitor, and serialize string 
///
class EDBA_API statement : public ref_cnt 
{
public:
    // Begin of API
    statement(session_monitor* sm, const string_ref& orig_sql);
    virtual ~statement() = 0 {} 

    ///
    /// Return query that was passed on construction
    /// 
    const std::string& orig_sql() const
    {
        return orig_sql_;
    }

    ///
    /// Return SQL Query result, MAY throw edba_error if the statement is not a query
    ///
    boost::intrusive_ptr<result> query();

    ///
    /// Execute a statement, MAY throw edba_error if the statement returns results.
    ///
    void exec();

    /// 
    /// Return statement bindings
    ///
    virtual bindings& bindings() = 0;

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

protected:
    ///
    /// Return SQL Query result, MAY throw edba_error if the statement is not a query
    ///
    virtual boost::intrusive_ptr<result> query_impl() = 0;

    ///
    /// Execute a statement, MAY throw edba_error if the statement returns results.
    ///
    virtual void exec_impl() = 0;


private:
    session_monitor* sm_;                       //!< Callback for library user to track certain library events. Can be 0.
    std::string orig_sql_;                      //!< Original sql that was passed to constructor.
};

///
/// \brief this class represents connection to database
///
class EDBA_API connection : public ref_cnt
{
public:
    connection(conn_info const &info, session_monitor* sm);
    virtual ~connection() {}

    boost::intrusive_ptr<statement> prepare(const string_ref& q);
    boost::intrusive_ptr<statement> get_prepared_statement(const string_ref& q);
    boost::intrusive_ptr<statement> get_statement(const string_ref& q);
    void exec_batch(const string_ref& q);

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

    ///
    /// Create a prepared statement \a q. May throw if preparation had failed.
    /// Should never return null value.
    ///
    virtual boost::intrusive_ptr<statement> prepare_statement(const string_ref& q) = 0;
    ///
    /// Create a (unprepared) statement \a q. May throw if preparation had failed.
    /// Should never return null value.
    ///
    virtual boost::intrusive_ptr<statement> create_statement(const string_ref& q) = 0;

    ///
    /// Escape a string for inclusion in SQL query. May throw not_supported_by_backend() if not supported by backend.
    ///
    virtual std::string escape(std::string const &) = 0;
    ///
    /// Escape a string for inclusion in SQL query. May throw not_supported_by_backend() if not supported by backend.
    ///
    virtual std::string escape(char const *s) = 0;
    ///
    /// Escape a string for inclusion in SQL query. May throw not_supported_by_backend() if not supported by backend.
    ///
    virtual std::string escape(char const *b,char const *e) = 0;
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

protected:
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

    ///
    /// Executes commands batch in one shot
    ///
    virtual void exec_batch_impl(const string_ref& q) = 0;

    session_monitor* sm_;
    unsigned default_is_prepared_ : 1;
    unsigned expand_conditionals_ : 1;
    unsigned reserved_ : 30;
};

}} // edba, backend

#endif
