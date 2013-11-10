#ifndef EDBA_ROWSET_HPP
#define EDBA_ROWSET_HPP

#include <edba/backend/interfaces.hpp>

#include <edba/string_ref.hpp>
#include <edba/types.hpp>

#include <boost/logic/tribool.hpp>
#include <boost/move/move.hpp>

namespace edba {

namespace detail {
    template<typename T>
    struct value_holder { T value_; };
}

class row;
template<typename Row> class rowset_iterator;
template<typename Row> class rowset;

///
/// Represent single row in result set.
///
class row
{
    template<typename T> friend class rowset_iterator;
    template<typename T> friend class rowset;

    // Row doesn`t support construction by user, only by rowset
    row(const backend::connection_ptr& conn
      , const backend::statement_ptr& stmt
      , const backend::result_ptr& res
      );

public:
    ///
    /// Return true if the column number \a col (starting from 0) has NULL value
    ///
    bool is_null(int col) const;
    ///
    /// Return true if the column named \a n has NULL value
    ///
    bool is_null(const string_ref& n) const;

    ///
    /// Reset current column index to 0
    ///
    void rewind_column() const;

    ///
    /// Fetch a value from column \a col (starting from 0) into \a v. Returns false
    /// if the value in NULL and \a v is not updated, otherwise returns true.
    ///
    /// If the data type is not same it tries to cast the data, if casting fails or the
    /// data is out of the type range, throws bad_value_cast().
    ///
    bool fetch(int col, const fetch_types_variant& v) const;

    ///
    /// Fetch a value from column \a col (starting from 0) into \a v. Returns false
    /// if the value in NULL and \a v is not updated, otherwise returns true.
    ///
    /// If the data type is not same it tries to cast the data, if casting fails or the
    /// data is out of the type range, throws bad_value_cast().
    ///
    template<typename T>
    bool fetch(int col, T& v) const;

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
    bool fetch(const string_ref& n, T& v) const;

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
    bool fetch(T& v) const;

    ///
    /// Get a value of type \a T from column named \a name (starting from 0). If the column
    /// is null throws null_value_fetch(), if the column \a name is invalid throws invalid_column,
    /// if the column value cannot be converted to type T (see fetch functions) it throws bad_value_cast.
    ///
    template<typename T>
    T get(const string_ref& name) const;

    ///
    /// Get a value of type \a T from column named \a name (starting from 0). If the column
    /// is null throws null_value_fetch(), if the column \a name is invalid throws invalid_column,
    /// if the column value cannot be converted to type T (see fetch functions) it throws bad_value_cast.
    ///
    template<typename T>
    void get(const string_ref& name, T& value) const;

    ///
    /// Get a value of type \a T from column \a col (starting from 0). If the column
    /// is null throws null_value_fetch(), if the column index is invalid throws invalid_column,
    /// if the column value cannot be converted to type T (see fetch functions) it throws bad_value_cast.
    ///
    template<typename T>
    T get(int col) const;

    ///
    /// Get a value of type \a T from column \a col (starting from 0). If the column
    /// is null throws null_value_fetch(), if the column index is invalid throws invalid_column,
    /// if the column value cannot be converted to type T (see fetch functions) it throws bad_value_cast.
    ///
    template<typename T>
    void get(int col, T& value) const;

private:
    backend::result_ptr res_;
    backend::statement_ptr stmt_;
    backend::connection_ptr conn_;
    mutable int current_col_;
};

// -------- row implementation ---------

inline row::row(
    const backend::connection_ptr& conn
  , const backend::statement_ptr& stmt
  , const backend::result_ptr& res
  )
  : res_(res)
  , stmt_(stmt)
  , conn_(conn)
  , current_col_(0)
{
}

inline bool row::is_null(int col) const
{
    return res_->is_null(col);
}

inline bool row::is_null(const string_ref& n) const
{
    int c = res_->name_to_column(n);
    if (c < 0)
        throw invalid_column();

    res_->is_null(c);
}

inline void row::rewind_column() const
{
    current_col_ = 0;
}

inline bool row::fetch(int col, const fetch_types_variant& v) const
{
    return res_->fetch(col, v);
}

template<typename T>
bool row::fetch(int col, T& v) const
{
    return fetch_conversion<T>::fetch(*this, col, v);
}

template<typename T>
bool row::fetch(const string_ref& n, T& v) const
{
    int c = res_->name_to_column(n);
    if (c < 0)
        throw invalid_column();
    return fetch(c, v);
}

template<typename T>
bool row::fetch(T& v) const
{
    int old_current_col = current_col_;
    bool res = fetch(current_col_, v);
    if (old_current_col == current_col_)
        ++current_col_;

    return res;
}

template<typename T>
T row::get(const string_ref& name) const
{
    T v = T();
    get(name, v);
    return v;
}

template<typename T>
void row::get(const string_ref& name, T& v) const
{
    if(!fetch(name, v))
        throw null_value_fetch();
}

template<typename T>
T row::get(int col) const
{
    T v = T();
    get(col, v);
    return v;
}

template<typename T>
void row::get(int col, T& v) const
{
    if(!fetch(col, v))
        throw null_value_fetch();
}

// -------- free functions ---------

///
/// \brief Fetch value by column name
///
template<typename T>
detail::tag<string_ref, T&> into(const string_ref& name, T& v)
{
    return detail::tag<string_ref, T&>(name, v);
}

///
/// \brief Fetch value by index (starting from 0)
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
const row& operator>>(const row& r, detail::tag<T1, T2> tag)
{
    if(!r.fetch(tag.first_, tag.second_))
        throw null_value_fetch();

    return r;
}

///
/// Syntactic sugar, same as
/// \code
/// v = row.get();
/// \endcode
template<typename T>
const row& operator>>(const row& r, T& v)
{
    if(!r.fetch(v))
        throw null_value_fetch();

    return r;
}

///
/// Iterator over rows in rowset.
///
template<typename T>
class rowset_iterator
  : public boost::iterator_facade<
        rowset_iterator<T>
      , T
      , boost::single_pass_traversal_tag
      >
{
    typedef boost::iterator_facade<
        rowset_iterator<T>
      , T
      , boost::single_pass_traversal_tag
      > base_type;

    typedef typename boost::remove_cv<T>::type mutable_value_type;
    typedef mutable_value_type& mutable_reference;

    typedef rowset<mutable_value_type> mutable_rowset_type;

    // Type of rowset passed into constructor and stored in iterator
    typedef typename boost::mpl::if_<
        boost::is_const<T>
      , const mutable_rowset_type
      , mutable_rowset_type
      >::type rowset_type;

    template<typename> friend class rowset_iterator;

public:
    typedef typename base_type::reference reference;

    rowset_iterator(rowset_type* rs = 0);

    ///
    /// Copy construction and conversion from mutable to const iterator
    ///
    template<typename T1>
    rowset_iterator(rowset_iterator<T1> other, typename boost::enable_if< boost::is_convertible<T1*, T*> >::type* = 0);

    boost::tribool has_next() const;

private:
    friend class boost::iterator_core_access;

    reference dereference() const;
    void increment();
    bool equal(rowset_iterator const& other) const;

    rowset_type* rs_;
};

///
/// Represent select query result set. Has range interface, implements Single Pass Range concept. rowset can be iterated
/// only once. That means when begin has been called once, all subsequent calls to begin will give undefined behavior.
///
template<typename T = row>
class rowset : private boost::mpl::if_<boost::is_same<T, row>, null_type, detail::value_holder<T> >::type
{
    template<typename T1> friend class rowset_iterator;

public:
    typedef rowset_iterator<T> iterator;
    typedef rowset_iterator<T const> const_iterator;

    ///
    /// Construct rowset from backend result
    ///
    rowset(
        const backend::connection_ptr& conn
      , const backend::statement_ptr& stmt
      , const backend::result_ptr& res
      );

    ///
    /// Open rowset for traversion and return begin iterator
    ///
    iterator begin();
    ///
    /// Open rowset for traversion and return begin iterator
    ///
    const_iterator begin() const;
    ///
    /// Return end iterator for rowset
    ///
    iterator end();
    ///
    /// Return end iterator for rowset
    ///
    const_iterator end() const;

    ///
    /// Provide conversion between rowset`s of different types.
    ///
    template<typename T1>
    operator rowset<T1>() const;

    ///
    /// Return total number of rows in rowset, or -1 if backend doesn`t provide this information
    ///
    boost::uint64_t rows() const;

    ///
    /// Return number of columns in rowset
    ///
    int columns() const;

    ///
    /// Get column name by index. Throw invalid_column or error
    ///
    std::string column_name(int col) const;

    ///
    /// Get column index by name. Throw invalid_column or error
    ///
    int column_index(const string_ref& n) const;

    ///
    /// Return column index by name or -1 if column with provided name doesn`t exists
    ///
    int find_column(const string_ref& name) const;

private:
    row row_;
    mutable bool opened_;                                   //!< User have already called begin method
};

// -------- rowset_iterator<T> implementation ---------

template<typename T>
rowset_iterator<T>::rowset_iterator(rowset_type* rs) : rs_(rs)
{
    increment();
}

template<typename T>
template<typename T1>
rowset_iterator<T>::rowset_iterator(rowset_iterator<T1> other, typename boost::enable_if< boost::is_convertible<T1*, T*> >::type*)
    : rs_(other.rs_)
{
}

template<typename T>
boost::tribool rowset_iterator<T>::has_next() const
{
    backend::result_iface::next_row nr = rs_->row_.res_->has_next();
    if (backend::result_iface::next_row_exists == nr)
        return true;
    else if (backend::result_iface::last_row_reached == nr)
        return false;
    else
        return boost::tribool();
}

template<typename T>
typename rowset_iterator<T>::reference rowset_iterator<T>::dereference() const
{
    BOOST_ASSERT(rs_ && "Attempt to dereference end rowset_iterator");
    return static_cast<T&>(rs_->value_);
}

// row_iterator::dereference specialization for row. We should not call fetch, instead we just return stored row object;
template<>
inline rowset_iterator<row>::reference rowset_iterator<row>::dereference() const
{
    BOOST_ASSERT(rs_ && "Attempt to dereference end rowset_iterator");
    return rs_->row_;
}

// row_iterator::dereference specialization for row. We should not call fetch, instead we just return stored row object;
template<>
inline rowset_iterator<const row>::reference rowset_iterator<const row>::dereference() const
{
    BOOST_ASSERT(rs_ && "Attempt to dereference end rowset_iterator");
    return rs_->row_;
}

template<typename T>
void rowset_iterator<T>::increment()
{
    if (!rs_)
        return;

    if (rs_->row_.res_->next())
    {
        rs_->row_.rewind_column();
        if (!fetch_conversion<mutable_value_type>::fetch(rs_->row_, 0, const_cast<mutable_reference>(rs_->value_)))
            throw null_value_fetch();
    }
    else
        rs_ = 0;
}

template<>
inline void rowset_iterator<row>::increment()
{
    if (!rs_)
        return;

    if (rs_->row_.res_->next())
        rs_->row_.rewind_column();
    else
        rs_ = 0;
}

template<>
inline void rowset_iterator<const row>::increment()
{
    if (!rs_)
        return;

    if (rs_->row_.res_->next())
        rs_->row_.rewind_column();
    else
        rs_ = 0;
}

template<typename T>
bool rowset_iterator<T>::equal(rowset_iterator<T> const& other) const
{
    return rs_ == other.rs_;
}

// -------- rowset implementation ---------

template<typename T>
rowset<T>::rowset(
    const backend::connection_ptr& conn
  , const backend::statement_ptr& stmt
  , const backend::result_ptr& res
  )
  : row_(conn, stmt, res)
  , opened_(false)
{
}

template<typename T>
typename rowset<T>::iterator rowset<T>::begin()
{
    if (opened_)
        throw multiple_rowset_traverse("Impossible to open rowset_iterator twice");

    iterator iter(this);
    opened_ = true;
    return iter;
}

template<typename T>
typename rowset<T>::const_iterator rowset<T>::begin() const
{
    if (opened_)
        throw multiple_rowset_traverse("Impossible to open rowset_iterator twice");

    const_iterator iter(this);
    opened_ = true;
    return iter;
}


template<typename T>
typename rowset<T>::iterator rowset<T>::end()
{
    return iterator();
}

template<typename T>
typename rowset<T>::const_iterator rowset<T>::end() const
{
    return const_iterator();
}

template<typename T>
template<typename T1>
rowset<T>::operator rowset<T1>() const
{
    return rowset<T1>(row_.conn_, row_.stmt_, row_.res_);
}

template<typename T>
boost::uint64_t rowset<T>::rows() const
{
    return row_.res_->rows();
}

template<typename T>
int rowset<T>::columns() const
{
    return row_.res_->cols();
}

template<typename T>
std::string rowset<T>::column_name(int col) const
{
    if (col < 0 || col >= columns())
        throw invalid_column();

    return row_.res_->column_to_name(col);
}

template<typename T>
int rowset<T>::column_index(const string_ref& n) const
{
    int c = row_.res_->name_to_column(n);
    if (c < 0)
        throw invalid_column();

    return c;
}

template<typename T>
int rowset<T>::find_column(const string_ref& name) const
{
    int c = row_.res_->name_to_column(name);
    if (c < 0)
        return -1;
    return c;
}

/// Specialization of fetch_conversion for native types.
template<typename T>
struct fetch_conversion<T, typename boost::enable_if< boost::mpl::contains<fetch_types, T*> >::type >
{
    static bool fetch(const row& r, int col, T& v)
    {
        return r.fetch(col, fetch_types_variant(&v));
    }
};

/// Specialization for fetch_conversion for types derived from ostream
template<typename T>
struct fetch_conversion<
    T
  , typename boost::enable_if<
        boost::mpl::and_<
            boost::is_convertible<T*, std::ostream*>
          , boost::mpl::not_< boost::mpl::contains<fetch_types, T*> >
          >
      >::type
  >
{
    static bool fetch(const row& r, int col, T& v)
    {
        return r.fetch(col, fetch_types_variant(&v));
    }
};

}

#endif // EDBA_ROWSET_HPP
