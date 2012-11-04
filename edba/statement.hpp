#ifndef EDBA_STATEMENT_HPP
#define EDBA_STATEMENT_HPP

#include <edba/result.hpp>

namespace edba {

///
/// \brief This class represents a prepared (or ordinary) statement that can be executed.
///
/// This object is usually created via session::prepare() function.
///
class EDBA_API statement 
{
public:
    ///
    /// Default constructor, provided for convenience, access to any member function
    /// of empty statement will cause an exception being thrown. 
    ///
    statement();

    ///
    /// Reset the statement - remove all bindings and return it into initial state so query() or exec()
    /// functions can be called once again.
    ///
    /// You must use it if you use the same statement multiple times.
    ///
    void reset();

    ///
    /// Bind a value \a v to the placeholder by index (starting from the 1).
    ///
    /// Placeholders are marked as ':placeholdername' in the query.
    /// If placeholder index is higher then the number placeholders is the statement it
    /// may throw invalid_placeholder exception.
    ///
    /// If placeholder was not binded the behavior is undefined and may vary between different backends.
    ///
    template<typename T>
    statement& bind(int col, const T& v)
    {
        bind_conversion<T>::template bind(*this, col, v);
        return *this;
    }

    ///
    /// Bind a value \a v to the placeholder by name.
    ///
    /// Placeholders are marked as ':placeholdername' in the query.
    /// If placeholder name is invalid then it may throw invalid_placeholder exception
    ///
    /// If placeholder was not binded the behavior is undefined and may vary between different backends.
    ///
    template<typename T>
    statement& bind(const string_ref& name, const T& v)
    {
        bind_conversion<T>::template bind(*this, name, v);
        return *this;
    }

    ///
    /// Bind a value \a v to the placeholder.
    ///
    /// Placeholders are marked as ':placeholdername' in the query.
    /// If number of calls is higher then the number placeholders is the statement it
    /// may throw invalid_placeholder exception.
    ///
    /// If placeholder was not binded the behavior is undefined and may vary between different backends.
    ///
    template<typename T>
    statement& bind(const T& v)
    {
        return bind(placeholder_++, v);
    }

    ///
    /// Get last insert id from the last executed statement, note, it is the same as sequence_last("").
    /// 
    /// Some backends requires explicit sequence name so you should use sequence_last("sequence_name") in such
    /// case.
    ///
    /// If the statement is actually query, the behavior is undefined and may vary between backends.
    ///
    long long last_insert_id();
    ///
    /// Get last created sequence value from the last executed statement.
    ///
    /// If the backend does not support named sequences but rather supports "auto increment" columns (like MySQL, Sqlite3),
    /// the \a seq parameter is ignored.
    /// 
    ///
    /// If the statement is actually query, the behavior is undefined and may vary between backends.
    ///
    long long sequence_last(std::string const &seq);
    ///
    /// Get the number of affected rows by the last statement, 
    ///
    ///
    /// If the statement is actually query, the behavior is undefined and may vary between backends.
    ///
    unsigned long long affected();

    ///
    /// Fetch a single row from the query. Unlike query(), you should not call result::next()
    /// function as it is already called. You may check if the data was fetched using result::empty()
    /// function.
    ///
    /// If the result set consists of more then one row it throws multiple_rows_query exception, however some backends
    /// may ignore this.
    ///
    /// If the statement is not query statement (like SELECT) it would likely
    /// throw an exception, however the behavior may vary between backends that may ignore this error.
    ///
    result row();
    ///
    /// Fetch a result of the query, if the statement is not query statement (like SELECT) it would likely
    /// throw an exception, however the behavior may vary between backends that may ignore this error.
    ///
    result query();
    ///
    /// Same as query() - syntactic sugar
    ///
    operator result();

    ///
    /// Execute a statement, of the statement is actually SELECT like operator, it throws edba_error exception,
    /// however the behavior may vary between backends that may ignore this error.
    ///
    void exec();

private:
    statement(
        const boost::intrusive_ptr<backend::statement>& stat
      , const boost::intrusive_ptr<backend::connection>& conn
      );

    friend class session;

    int placeholder_;
    boost::intrusive_ptr<backend::statement> stat_;
    boost::intrusive_ptr<backend::connection> conn_;
};

///
/// \brief Bind statement parameter by name
///
template<typename T>
detail::tag<string_ref, const T&> use(const string_ref& name, const T& v)
{
    return detail::tag<string_ref, const T&>(name, v);
}

///
/// \brief Bind statement parameter by index (starting from 1)
///
template<typename T>
detail::tag<int, const T&> use(int index, const T& v)
{
    return detail::tag<int, const T&>(index, v);
}

///
/// \brief Manipulator that causes statement execution. Used as:
///
/// \code
///  sql << "delete from test" << edba::exec;
/// \endcode
///
inline void exec(statement &st)
{
    st.exec();
}

///
/// \brief Manipulator that reset statement bindings. Used as:
///
/// \code
///  sql << "<some sql with bindings>" << 1 << 2 << 3 << edba::exec << edba::reset;
/// \endcode
///
inline void reset(statement &st)
{
    st.reset();
}

///
/// \brief Manipulator that fetches a single row. Used as:
///
/// \code
///  edba::result r = sql << "SELECT name where uid=?" << id << edba::row;
/// if(!r.empty()) {
///  ...
/// }
/// \endcode
///
/// Or:
///
/// \code
///  sql << "SELECT name where uid=?" << id << edba::row >> name;
/// \endcode
///
/// Which would throw empty_row_access exception on attempt to fetch name if the result is empty.
///
inline result row(statement &st)
{
    return st.row();
}

///
/// Apply manipulator on the statement, same as manipulator(*this).
///
inline statement& operator<<(statement& st, void (*manipulator)(statement &st))
{
    manipulator(st);
    return st;
}

///
/// Apply manipulator on the statement, same as manipulator(*this).
///
inline result operator<<(statement& st, result (*manipulator)(statement &st))
{
    return manipulator(st);
}

///
/// Used with types produced by use function.
///
/// The call st << use("paramname", val) is same as
///
/// \code
///  st.bind("paramname", val);
/// \endcode
///
template<typename T1, typename T2>
statement &operator<<(statement& st, const detail::tag<T1, T2>& val)
{
    return st.bind(val.first_, val.second_);
}

/// 
/// Syntactic sugar for 
/// \code
/// st.bind(val);
/// \endcode
///
template<typename T>
statement& operator<<(statement& st, const T& v)
{
    return st.bind(v);
}

template<>
EDBA_API statement& statement::bind(int col, const bind_types_variant& v);

template<>
EDBA_API statement& statement::bind(const string_ref& name, const bind_types_variant& v);

}

#endif // EDBA_STATEMENT_HPP
