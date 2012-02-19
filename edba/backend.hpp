#ifndef EDBA_BACKEND_H
#define EDBA_BACKEND_H

#include <edba/defs.hpp>
#include <edba/errors.hpp>
#include <edba/frontend.hpp>
#include <edba/utils.hpp>

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
    /// Fetch an integer value for column \a col starting from 0.
    ///
    /// Should throw invalid_column() \a col value is invalid, should throw bad_value_cast() if the underlying data
    /// can't be converted to integer or its range is not supported by the integer type.
    ///
    virtual bool fetch(int col,short &v) = 0;
    ///
    /// Fetch an integer value for column \a col starting from 0.
    /// Returns true if ok, returns false if the column value is NULL and the referenced object should remain unchanged
    ///
    /// Should throw invalid_column() \a col value is invalid, should throw bad_value_cast() if the underlying data
    /// can't be converted to integer or its range is not supported by the integer type.
    ///
    virtual bool fetch(int col,unsigned short &v) = 0;
    ///
    /// Fetch an integer value for column \a col starting from 0.
    /// Returns true if ok, returns false if the column value is NULL and the referenced object should remain unchanged
    ///
    /// Should throw invalid_column() \a col value is invalid, should throw bad_value_cast() if the underlying data
    /// can't be converted to integer or its range is not supported by the integer type.
    ///
    virtual bool fetch(int col,int &v) = 0;
    ///
    /// Fetch an integer value for column \a col starting from 0.
    /// Returns true if ok, returns false if the column value is NULL and the referenced object should remain unchanged
    ///
    /// Should throw invalid_column() \a col value is invalid, should throw bad_value_cast() if the underlying data
    /// can't be converted to integer or its range is not supported by the integer type.
    ///
    virtual bool fetch(int col,unsigned &v) = 0;
    ///
    /// Fetch an integer value for column \a col starting from 0.
    /// Returns true if ok, returns false if the column value is NULL and the referenced object should remain unchanged
    ///
    /// Should throw invalid_column() \a col value is invalid, should throw bad_value_cast() if the underlying data
    /// can't be converted to integer or its range is not supported by the integer type.
    ///
    virtual bool fetch(int col,long &v) = 0;
    ///
    /// Fetch an integer value for column \a col starting from 0.
    /// Returns true if ok, returns false if the column value is NULL and the referenced object should remain unchanged
    ///
    /// Should throw invalid_column() \a col value is invalid, should throw bad_value_cast() if the underlying data
    /// can't be converted to integer or its range is not supported by the integer type.
    ///
    virtual bool fetch(int col,unsigned long &v) = 0;
    ///
    /// Fetch an integer value for column \a col starting from 0.
    /// Returns true if ok, returns false if the column value is NULL and the referenced object should remain unchanged
    ///
    /// Should throw invalid_column() \a col value is invalid, should throw bad_value_cast() if the underlying data
    /// can't be converted to integer or its range is not supported by the integer type.
    ///
    virtual bool fetch(int col,long long &v) = 0;
    ///
    /// Fetch an integer value for column \a col starting from 0.
    /// Returns true if ok, returns false if the column value is NULL and the referenced object should remain unchanged
    ///
    /// Should throw invalid_column() \a col value is invalid, should throw bad_value_cast() if the underlying data
    /// can't be converted to integer or its range is not supported by the integer type.
    ///
    virtual bool fetch(int col,unsigned long long &v) = 0;
    ///
    /// Fetch a floating point value for column \a col starting from 0.
    /// Returns true if ok, returns false if the column value is NULL and the referenced object should remain unchanged
    ///
    /// Should throw invalid_column() \a col value is invalid, should throw bad_value_cast() if the underlying data
    /// can't be converted to floating point value.
    ///
    virtual bool fetch(int col,float &v) = 0;
    ///
    /// Fetch a floating point value for column \a col starting from 0.
    /// Returns true if ok, returns false if the column value is NULL and the referenced object should remain unchanged
    ///
    /// Should throw invalid_column() \a col value is invalid, should throw bad_value_cast() if the underlying data
    /// can't be converted to floating point value.
    ///
    virtual bool fetch(int col,double &v) = 0;
    ///
    /// Fetch a floating point value for column \a col starting from 0.
    /// Returns true if ok, returns false if the column value is NULL and the referenced object should remain unchanged
    ///
    /// Should throw invalid_column() \a col value is invalid, should throw bad_value_cast() if the underlying data
    /// can't be converted to floating point value.
    ///
    virtual bool fetch(int col,long double &v) = 0;
    ///
    /// Fetch a string value for column \a col starting from 0.
    /// Returns true if ok, returns false if the column value is NULL and the referenced object should remain unchanged
    ///
    /// Should throw invalid_column() \a col value is invalid, any data should be convertible to
    /// text value (as formatting integer, floating point value or date-time as string).
    ///
    virtual bool fetch(int col,std::string &v) = 0;
    ///
    /// Fetch a BLOB value for column \a col starting from 0.
    /// Returns true if ok, returns false if the column value is NULL and the referenced object should remain unchanged
    ///
    /// Should throw invalid_column() \a col value is invalid, any data should be convertible to
    /// BLOB value as text (as formatting integer, floating point value or date-time as string).
    ///
    virtual bool fetch(int col,std::ostream &v) = 0;
    ///
    /// Fetch a date-time value for column \a col starting from 0.
    /// Returns true if ok, returns false if the column value is NULL and the referenced object should remain unchanged
    ///
    /// Should throw invalid_column() \a col value is invalid. If the data can't be converted
    /// to date-time it should throw bad_value_cast()
    ///
    virtual bool fetch(int col,std::tm &v) = 0;
    ///
    /// Check if the column \a col is NULL starting from 0, should throw invalid_column() if the index out of range
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
    virtual int name_to_column(const chptr_range&) = 0;
    ///
    /// Return the column name for column index starting from 0.
    /// Should throw invalid_column() if the index out of range
    /// Should be able to work even without calling next() first time.
    ///
    virtual std::string column_to_name(int) = 0;
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
    statement(session_monitor* sm, const chptr_range& orig_sql);
    virtual ~statement() {}

    ///
    /// Reset the prepared statement to initial state as before the operation. It is
    /// called by front-end each time before new query() or exec() are called.
    ///
    void reset();

    ///
    /// Return query that was passed on construction
    /// 
    const std::string& orig_sql() const
    {
        return orig_sql_;
    }

    ///
    /// Bind value to column \a col (starting from 1).
    /// 
    /// Dispatch call to suitable implementation
    ///
    template<typename T>
    void bind(int col, const T& val)
    {
        bind_impl(col, val);
        if (sm_)
            bindings_ << '\'' << val <<"' ";
    }

    ///
    /// Bind time value to column \a col (starting from 1).
    /// 
    /// Dispatch call to suitable implementation
    ///
    void bind(int col, const std::tm& val);

    ///
    /// Bind a BLOB value to column \a col (starting from 1).
    /// 
    /// Dispatch call to suitable implementation
    ///
    void bind(int col,std::istream &);

    ///
    /// Bind a NULL value to column \a col (starting from 1).
    ///
    /// Dispatch call to suitable implementation
    ///
    void bind_null(int col);

    ///
    /// Return SQL Query result, MAY throw edba_error if the statement is not a query
    ///
    boost::intrusive_ptr<result> query();

    ///
    /// Execute a statement, MAY throw edba_error if the statement returns results.
    ///
    void exec();

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
    /// Reset the prepared statement to initial state as before the operation. It is
    /// called by front-end each time before new query() or exec() are called.
    ///
    virtual void reset_impl() = 0;

    ///
    /// Bind a text value to column \a col (starting from 1). You may assume
    /// that the reference remains valid until real call of query() or exec()
    ///
    /// Should throw invalid_placeholder() if the value of col is out of range. May
    /// ignore if it is impossible to know whether the placeholder exists without special
    /// support from back-end.
    ///
    virtual void bind_impl(int col,chptr_range const &) = 0;
    ///
    /// Bind a date-time value to column \a col (starting from 1).
    ///
    /// Should throw invalid_placeholder() if the value of col is out of range. May
    /// ignore if it is impossible to know whether the placeholder exists without special
    /// support from back-end.
    ///
    virtual void bind_impl(int col,std::tm const &) = 0;
    ///
    /// Bind a BLOB value to column \a col (starting from 1).
    ///
    /// Should throw invalid_placeholder() if the value of col is out of range. May
    /// ignore if it is impossible to know whether the placeholder exists without special
    /// support from back-end.
    ///
    virtual void bind_impl(int col,std::istream &) = 0;
    ///
    /// Bind an integer value to column \a col (starting from 1).
    ///
    /// Should throw invalid_placeholder() if the value of col is out of range. May
    /// ignore if it is impossible to know whether the placeholder exists without special
    /// support from back-end.
    ///
    virtual void bind_impl(int col,int v) = 0;
    ///
    /// Bind an integer value to column \a col (starting from 1).
    ///
    /// Should throw invalid_placeholder() if the value of col is out of range. May
    /// ignore if it is impossible to know whether the placeholder exists without special
    /// support from back-end.
    ///
    /// May throw bad_value_cast() if the value out of supported range by the DB. 
    ///
    virtual void bind_impl(int col,unsigned v) = 0;
    ///
    /// Bind an integer value to column \a col (starting from 1).
    ///
    /// Should throw invalid_placeholder() if the value of col is out of range. May
    /// ignore if it is impossible to know whether the placeholder exists without special
    /// support from back-end.
    ///
    /// May throw bad_value_cast() if the value out of supported range by the DB. 
    ///
    virtual void bind_impl(int col,long v) = 0;
    ///
    /// Bind an integer value to column \a col (starting from 1).
    ///
    /// Should throw invalid_placeholder() if the value of col is out of range. May
    /// ignore if it is impossible to know whether the placeholder exists without special
    /// support from back-end.
    ///
    /// May throw bad_value_cast() if the value out of supported range by the DB. 
    ///
    virtual void bind_impl(int col,unsigned long v) = 0;
    ///
    /// Bind an integer value to column \a col (starting from 1).
    ///
    /// Should throw invalid_placeholder() if the value of col is out of range. May
    /// ignore if it is impossible to know whether the placeholder exists without special
    /// support from back-end.
    ///
    /// May throw bad_value_cast() if the value out of supported range by the DB. 
    ///
    virtual void bind_impl(int col,long long v) = 0;
    ///
    /// Bind an integer value to column \a col (starting from 1).
    ///
    /// Should throw invalid_placeholder() if the value of col is out of range. May
    /// ignore if it is impossible to know whether the placeholder exists without special
    /// support from back-end.
    ///
    /// May throw bad_value_cast() if the value out of supported range by the DB. 
    ///
    virtual void bind_impl(int col,unsigned long long v) = 0;
    ///
    /// Bind a floating point value to column \a col (starting from 1).
    ///
    /// Should throw invalid_placeholder() if the value of col is out of range. May
    /// ignore if it is impossible to know whether the placeholder exists without special
    /// support from back-end.
    ///
    virtual void bind_impl(int col,double v) = 0;
    ///
    /// Bind a floating point value to column \a col (starting from 1).
    ///
    /// Should throw invalid_placeholder() if the value of col is out of range. May
    /// ignore if it is impossible to know whether the placeholder exists without special
    /// support from back-end.
    ///
    virtual void bind_impl(int col,long double v) = 0;
    ///
    /// Bind a NULL value to column \a col (starting from 1).
    ///
    /// Should throw invalid_placeholder() if the value of col is out of range. May
    /// ignore if it is impossible to know whether the placeholder exists without special
    /// support from back-end.
    ///
    virtual void bind_null_impl(int col) = 0;
    ///
    /// Return SQL Query result, MAY throw edba_error if the statement is not a query
    ///
    virtual boost::intrusive_ptr<result> query_impl() = 0;
    ///
    /// Execute a statement, MAY throw edba_error if the statement returns results.
    ///
    virtual void exec_impl() = 0;


private:
    /// Callback for library user to track certain library events. Can be 0
    session_monitor* sm_;

    /// Accumulator for string representation of bounded parameters
    /// Used in session_monitor calls
    std::ostringstream bindings_;

    /// Original sql that was passed to constructor
    std::string orig_sql_; 
};

///
/// \brief this class represents connection to database
///
class EDBA_API connection : public ref_cnt
{
public:
    connection(conn_info const &info, session_monitor* sm);
    virtual ~connection() {}

    boost::intrusive_ptr<statement> prepare(const chptr_range& q);
    boost::intrusive_ptr<statement> get_prepared_statement(const chptr_range& q);
    boost::intrusive_ptr<statement> get_statement(const chptr_range& q);
    void exec_batch(const chptr_range& q);

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
    virtual boost::intrusive_ptr<statement> prepare_statement(const chptr_range& q) = 0;
    ///
    /// Create a (unprepared) statement \a q. May throw if preparation had failed.
    /// Should never return null value.
    ///
    virtual boost::intrusive_ptr<statement> create_statement(const chptr_range& q) = 0;

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
    virtual void exec_batch_impl(const chptr_range& q) = 0;

    session_monitor* sm_;
    unsigned default_is_prepared_ : 1;
    unsigned expand_conditionals_ : 1;
    unsigned reserved_ : 30;
};


    } // backend
} // edba

#endif
