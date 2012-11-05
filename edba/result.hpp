#ifndef EDBA_RESULT_HPP
#define EDBA_RESULT_HPP

#include <edba/string_ref.hpp>
#include <edba/types.hpp>
#include <edba/backend/backend_fwd.hpp>

namespace edba {

///
/// \brief This object represents query result.
///
/// This object and it is generally created by statement::query() call, default constructor
/// is provided for consistency, but access to any member function with exception of empty() would
/// throw an exception.
///
class EDBA_API result 
{
public:
    ///
    /// Create an empty result, it is not useful except for having default constructor
    ///
    result();

    ///
    /// Return the number of columns in the result
    ///
    int cols();

    ///
    /// Move forward to next row, returns false if no more rows available.
    ///
    /// Notes:
    ///
    /// - You should call next() at least once before you use fetch() functions
    /// - You must not call fetch() functions if next() returned false, it would cause empty_row_access exception.
    ///
    bool next();

    ///
    /// Convert column name \a n to its index, throws invalid_column if the name is not valid.
    ///
    int index(const string_ref& n);
    ///
    /// Convert column name \a n to its index, returns -1 if the name is not valid.
    ///
    int find_column(const string_ref& name);

    ///
    /// Convert column index to column name, throws invalid_column if col is not in range 0<= col < cols()
    ///
    std::string name(int col);

    ///
    /// Return true if the column number \a col (starting from 0) has NULL value
    ///
    bool is_null(int col);
    ///
    /// Return true if the column named \a n has NULL value
    ///
    bool is_null(const string_ref& n);

    ///
    /// Clears the result, no further use of the result should be done until it is assigned again with a new statement result.
    ///
    /// It is useful when you want to release all data and return the statement to cache
    ///
    void clear();
    ///
    /// Reset current column index, so fetch without column index can be used once again
    ///
    void rewind_column();
    ///
    /// Check if the current row is empty, it is in 3 cases:
    ///
    /// -# Empty result
    /// -# next() wasn't called first time
    /// -# next() returned false;
    ///
    bool empty();

    ///
    /// Fetch a value from column \a col (starting from 0) into \a v. Returns false
    /// if the value in NULL and \a v is not updated, otherwise returns true.
    ///
    /// If the data type is not same it tries to cast the data, if casting fails or the
    /// data is out of the type range, throws bad_value_cast().
    ///
    template<typename T>
    bool fetch(int col, T& v)
    {
        return fetch_conversion<T>::fetch(*this, col, v);
    }

    ///
    /// Fetch a value from column \a col (starting from 0) into \a v. Returns false
    /// if the value in NULL and \a v is not updated, otherwise returns true.
    ///
    /// If the data type is not same it tries to cast the data, if casting fails or the
    /// data is out of the type range, throws bad_value_cast().
    ///
    bool result::fetch(int col, fetch_types_variant& v);

    ///
    /// Fetch a value from column named \a n into \a v. Returns false
    /// if the value in NULL and \a v is not updated, otherwise returns true.
    ///
    /// If the data type is not same it tries to cast the data, if casting fails or the
    /// data is out of the type range, throws bad_value_cast().
    ///
    /// If the \a n value is invalid throws invalid_column exception
    ///
    template<typename T>
    bool fetch(const string_ref& n, T& v)
    {
        return fetch(index(n),v);
    }

    ///
    /// Fetch a value from the next column in the row starting from the first one. Returns false
    /// if the value in NULL and \a v is not updated, otherwise returns true.
    ///
    /// If the data type is not same it tries to cast the data, if casting fails or the
    /// data is out of the type range, throws bad_value_cast().
    ///
    /// If fetch was called more times then cols() it throws invalid_column exception, to use
    /// it once again from the beginning on the same row call rewind_column() member function.
    /// It is not required to call rewind_column() after calling next() as column index is reset
    /// automatically.
    ///
    template<typename T>
    bool fetch(T& v)
    {
        return fetch(current_col_++, v);
    }

    ///
    /// Get a value of type \a T from column named \a name (starting from 0). If the column
    /// is null throws null_value_fetch(), if the column \a name is invalid throws invalid_column,
    /// if the column value cannot be converted to type T (see fetch functions) it throws bad_value_cast.
    ///	

    template<typename T>
    T get(const string_ref& name)
    {
        T v=T();
        if(!fetch(name,v))
            throw null_value_fetch();
        return v;
    }

    ///
    /// Get a value of type \a T from column \a col (starting from 0). If the column
    /// is null throws null_value_fetch(), if the column index is invalid throws invalid_column,
    /// if the column value cannot be converted to type T (see fetch functions) it throws bad_value_cast.
    ///	
    template<typename T>
    T get(int col)
    {
        T v=T();
        if(!fetch(col,v))
            throw null_value_fetch();
        return v;
    }

private:
    result(const boost::intrusive_ptr<backend::result>& res);

    void check();

    friend class statement;

    bool eof_;
    bool fetched_;
    int current_col_;
    boost::intrusive_ptr<backend::result> res_;
};

///
/// \brief Fetch result parameter by column name
///
template<typename T>
detail::tag<string_ref, T&> into(const string_ref& name, T& v)
{
    return detail::tag<string_ref, T&>(name, v);
}

///
/// \brief Fetch result parameter by index (starting from 0)
///
template<typename T>
detail::tag<int, T&> into(int index, T& v)
{
    return detail::tag<int, T&>(index, v);
}

///
/// Syntactic sugar, used together with into() function.
///
template<typename T1, typename T2>
result& operator>>(result& r, detail::tag<T1, T2> tag)
{
    if(!fetch(tag.first_, tag.second_))
        throw null_value_fetch();

    return r;
}

///
/// Syntactic sugar, same as
/// \code
/// v = result.get();
/// \endcode
template<typename T>
result& operator>>(result& r, T& v)
{
    if(!r.fetch(v))
        throw null_value_fetch();

    return r;
}

}

#endif // EDBA_RESULT_HPP
