#ifndef EDBA_STATEMENT_HPP
#define EDBA_STATEMENT_HPP

#include <edba/backend/interfaces.hpp>
#include <edba/rowset.hpp>

#include <boost/type_traits/is_convertible.hpp>
#include <boost/mpl/not.hpp>
#include <boost/mpl/and.hpp>

namespace edba {

///
/// \brief This class represents a prepared (or ordinary) statement that can be executed.
///
/// This object is usually created via session::prepare() function.
///
class statement 
{
public:
    ///
    /// Default constructor, provided for convenience, access to some member function
    /// of empty statement will cause an exception empty_statement being thrown. 
    ///
    statement();

    ///
    /// Reset the statement - remove all bindings and return it into initial state so query() or exec()
    /// functions can be called once again.
    ///
    /// You must use it if you use the same statement multiple times.
    ///
    /// Immediatelly exits for empty statements
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
    /// Immediatelly exits for empty statements
    ///
    template<typename T>
    statement& bind(int col, const T& v);

    ///
    /// Bind a value \a v to the placeholder by index (starting from the 1).
    ///
    /// Placeholders are marked as ':placeholdername' in the query.
    /// If placeholder index is higher then the number placeholders is the statement it
    /// may throw invalid_placeholder exception.
    ///
    /// If placeholder was not binded the behavior is undefined and may vary between different backends.
    ///
    /// Immediatelly exits for empty statements
    ///
    statement& bind(int col, const bind_types_variant& v);

    ///
    /// Bind a value \a v to the placeholder by name.
    ///
    /// Placeholders are marked as ':placeholdername' in the query.
    /// If placeholder name is invalid then it may throw invalid_placeholder exception
    ///
    /// If placeholder was not binded the behavior is undefined and may vary between different backends.
    ///
    /// Immediatelly exits for empty statements
    ///
    template<typename T>
    statement& bind(const string_ref& name, const T& v);

    ///
    /// Bind a value \a v to the placeholder by name.
    ///
    /// Placeholders are marked as ':placeholdername' in the query.
    /// If placeholder name is invalid then it may throw invalid_placeholder exception
    ///
    /// If placeholder was not binded the behavior is undefined and may vary between different backends.
    ///
    /// Immediatelly exits for empty statements
    ///
    statement& bind(const string_ref& name, const bind_types_variant& v);

    ///
    /// Bind a value \a v to the placeholder.
    ///
    /// Placeholders are marked as ':placeholdername' in the query.
    /// If number of calls is higher then the number placeholders is the statement it
    /// may throw invalid_placeholder exception.
    ///
    /// If placeholder was not binded the behavior is undefined and may vary between different backends.
    ///
    /// Immediatelly exits for empty statements
    ///
    template<typename T>
    statement& bind(const T& v);

    ///
    /// Get last insert id from the last executed statement, note, it is the same as sequence_last("").
    /// 
    /// Some backends requires explicit sequence name so you should use sequence_last("sequence_name") in such
    /// case.
    ///
    /// If the statement is actually query, the behavior is undefined and may vary between backends.
    ///
    /// Throw empty_statement exception for empty statements
    ///
    long long last_insert_id();
    ///
    /// Get last created sequence value from the last executed statement.
    ///
    /// If the backend does not support named sequences but rather supports "auto increment" columns (like MySQL, Sqlite3),
    /// the \a seq parameter is ignored.
    /// 
    /// If the statement is actually query, the behavior is undefined and may vary between backends.
    ///
    /// Throw empty_statement exception for empty statements
    ///
    long long sequence_last(std::string const &seq);
    ///
    /// Get the number of affected rows by the last statement, 
    ///
    ///
    /// If the statement is actually query, the behavior is undefined and may vary between backends.
    ///
    /// Return 0 for empty statements
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
    /// Would throw empty_row_access exception if the result is empty.
    ///
    /// If the statement is not query statement (like SELECT) it would likely
    /// throw an exception, however the behavior may vary between backends that may ignore this error.
    ///
    /// Throw empty_statement exception for empty statements
    ///
    row first_row();
    ///
    /// Fetch a result of the query, if the statement is not query statement (like SELECT) it would likely
    /// throw an exception, however the behavior may vary between backends that may ignore this error.
    ///
    /// Throw empty_statement exception for empty statements
    ///
    rowset<> query();
    ///
    /// Same as query() - syntactic sugar
    ///
    template<typename T>
    operator rowset<T>();

    ///
    /// Execute a statement, of the statement is actually SELECT like operator, it throws edba_error exception,
    /// however the behavior may vary between backends that may ignore this error.
    ///
    /// Do nothing for empty statements
    ///
    void exec();

    // NOTE: Following overloaded operators are members because in case of free functions they need to accept
    // statement by value or const reference. Otherwise the next statement will be illformed because rvalue ref   
    // can`t be casted to non-const lvalue ref.
    // rowset<> rs = sess << "select * from world where id = :id" << 10;
    
    /// 
    /// Syntactic sugar for 
    /// \code
    /// st.bind(val);
    /// \endcode
    ///
    template<typename T>
    statement& operator<<(const T& v)
    {
        return bind(v);
    }    
    
    ///
    /// Apply manipulator on the statement, same as manipulator(*this).
    ///
    statement& operator<<(void (*manipulator)(statement &st))
    {
        manipulator(*this);
        return *this;
    }

    ///
    /// Apply manipulator on the statement, same as manipulator(*this).
    ///
    row operator<<(row (*manipulator)(statement &st))
    {
        return manipulator(*this);
    }

    ///
    /// Apply manipulator on the statement, same as manipulator(*this).
    ///
    rowset<> operator<<(rowset<> (*manipulator)(statement &st))
    {
        return manipulator(*this);
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
    statement &operator<<(const detail::tag<T1, T2>& val)
    {
        return bind(val.first_, val.second_);
    }    
    
    ///
    /// Equality operator
    ///
    friend bool operator==(const statement& s1, const statement& s2)
    {
        return s1.stmt_ == s2.stmt_;
    }

private:
    statement(const backend::connection_ptr& conn, const backend::statement_ptr& stmt);

    friend class session;

    int placeholder_;
    backend::connection_ptr conn_;
    backend::statement_ptr stmt_;
};

// ------ statement implementation ------

inline statement::statement() : placeholder_(1) 
{
}
inline statement::statement(
    const backend::connection_ptr& conn
  , const backend::statement_ptr& stmt
  ) 
    : placeholder_(1)
    , conn_(conn)
    , stmt_(stmt)
{
}
inline void statement::reset()
{
    if (stmt_) 
    {
        placeholder_ = 1;
        stmt_->bindings_reset();
    }
}
template<typename T>
statement& statement::bind(int col, const T& v)
{
    if (stmt_) 
        bind_conversion<T>::template bind(*this, col, v);

    return *this;
}
inline statement& statement::bind(int col, const bind_types_variant& v)
{
    if (stmt_) 
        stmt_->bind(col, v);

    return *this;
}
template<typename T>
statement& statement::bind(const string_ref& name, const T& v)
{
    if (stmt_) 
        bind_conversion<T>::template bind(*this, name, v);

    return *this;
}
inline statement& statement::bind(const string_ref& name, const bind_types_variant& v)
{
    if (stmt_) 
        stmt_->bind(name, v);

    return *this;
}
template<typename T>
statement& statement::bind(const T& v)
{
    if (stmt_) 
    {
        // bind_conversion specialization can recursively call bind and thus increment placeholder many times.
        // In case when bind has internally changed placeholder_ we should not increment placeholder.
        // Placeholder must be incremented only if bind was plain and was not called recursively

        int old_placeholder_value = placeholder_;
        bind(placeholder_, v);

        if (old_placeholder_value == placeholder_)
            ++placeholder_;
    }

    return *this;
}
inline long long statement::last_insert_id()
{
    if (!stmt_)
        throw empty_statement();

    return stmt_->sequence_last(std::string());
}
inline long long statement::sequence_last(std::string const &seq)
{
    if (!stmt_)
        throw empty_statement();

    return stmt_->sequence_last(seq);
}
inline unsigned long long statement::affected()
{
    return stmt_ ? stmt_->affected() : 0;
}
inline row statement::first_row()
{
    if (!stmt_) 
        throw empty_statement();

    rowset<> rs(conn_, stmt_, stmt_->run_query());

    rowset<>::const_iterator ri = rs.begin();

    if (rs.end() == ri)
        throw empty_row_access();

    if (ri.has_next())
        throw multiple_rows_query();

    return *ri;
}
inline rowset<> statement::query()
{
    if (!stmt_)
        throw empty_statement();

    return rowset<>(conn_, stmt_, stmt_->run_query());
}
template<typename T>
statement::operator rowset<T>()
{
    return query();
}
inline void statement::exec() 
{
    if (stmt_)
        stmt_->run_exec();
}

// ------ free functions ------

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
///  edba::row r = sql << "SELECT name where uid=?" << id << edba::first_row;
///  ...
/// \endcode
///
/// Or:
///
/// \code
///  sql << "SELECT name where uid=?" << id << edba::row >> name;
/// \endcode
///
/// Would throw empty_row_access exception if the result is empty.
///
inline row first_row(statement &st)
{
    return st.first_row();
}

///
/// \brief Manipulator that fetches a rowset. Used as:
///
/// \code
///  edba::rowset<> r = sql << "SELECT name where uid=?" << id << edba::query;
///  ...
/// \endcode
///
inline rowset<> query(statement &st)
{
    return st.query();
}

/// Specialization of bind_conversion for native types
template<typename T>
struct bind_conversion<T, typename boost::enable_if< boost::mpl::contains<bind_types, T> >::type>
{
    template<typename ColOrName>
    static void bind(statement& st, ColOrName col_or_name, const T& v)
    {
        st.bind(col_or_name, bind_types_variant(v));
    }
};

/// Specialization for types that are convertible to string_ref except string ref itself
template<typename T>
struct bind_conversion<
    T
  , typename boost::enable_if< 
        boost::mpl::and_<
            boost::is_convertible<T, string_ref>
          , boost::mpl::not_< boost::mpl::contains<bind_types, T> > 
          >
      >::type
  >
{
    template<typename ColOrName>
    static void bind(statement& st, ColOrName col_or_name, const T& v)
    {
        st.bind(col_or_name, bind_types_variant(string_ref(v)));
    }
};

/// Specialization for types that are convertible to std::istream*
template<typename T>
struct bind_conversion<
    T
  , typename boost::enable_if<
        boost::mpl::and_<
            boost::is_convertible<T, std::istream*>
          , boost::mpl::not_< boost::is_same<T, std::istream*> >
          >
      >::type
  >
{
    template<typename ColOrName>
    static void bind(statement& st, ColOrName col_or_name, std::istream* v)
    {
        st.bind(col_or_name, bind_types_variant(bind_types_variant(v)));
    }
};

} // namespace edba

#endif // EDBA_STATEMENT_HPP
