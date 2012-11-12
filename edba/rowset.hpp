#ifndef EDBA_ROWSET_HPP
#define EDBA_ROWSET_HPP

#include <edba/string_ref.hpp>
#include <edba/types.hpp>

#include <edba/backend/backend.hpp>
#include <boost/logic/tribool.hpp>
#include <boost/move/move.hpp>

namespace edba { 

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
    row(const boost::intrusive_ptr<backend::result>& res, const boost::intrusive_ptr<backend::statement>& stmt);

public:
    ///
    /// Return true if the column number \a col (starting from 0) has NULL value
    ///
    bool is_null(int col);
    ///
    /// Return true if the column named \a n has NULL value
    ///
    bool is_null(const string_ref& n);

    ///
    /// Reset current column index to 0
    ///
    void rewind_column();

    ///
    /// Fetch a value from column \a col (starting from 0) into \a v. Returns false
    /// if the value in NULL and \a v is not updated, otherwise returns true.
    ///
    /// If the data type is not same it tries to cast the data, if casting fails or the
    /// data is out of the type range, throws bad_value_cast().
    ///
    bool fetch(int col, const fetch_types_variant& v);

    ///
    /// Fetch a value from column \a col (starting from 0) into \a v. Returns false
    /// if the value in NULL and \a v is not updated, otherwise returns true.
    ///
    /// If the data type is not same it tries to cast the data, if casting fails or the
    /// data is out of the type range, throws bad_value_cast().
    ///
    template<typename T>
    bool fetch(int col, T& v);

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
    bool fetch(const string_ref& n, T& v);

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
    bool fetch(T& v);

    ///
    /// Get a value of type \a T from column named \a name (starting from 0). If the column
    /// is null throws null_value_fetch(), if the column \a name is invalid throws invalid_column,
    /// if the column value cannot be converted to type T (see fetch functions) it throws bad_value_cast.
    ///	
    template<typename T>
    T get(const string_ref& name);

    ///
    /// Get a value of type \a T from column \a col (starting from 0). If the column
    /// is null throws null_value_fetch(), if the column index is invalid throws invalid_column,
    /// if the column value cannot be converted to type T (see fetch functions) it throws bad_value_cast.
    ///	
    template<typename T>
    T get(int col);

private:
    boost::intrusive_ptr<backend::result> res_;
    boost::intrusive_ptr<backend::statement> stmt_;
    int current_col_;
};

// -------- row implementation ---------

inline row::row(const boost::intrusive_ptr<backend::result>& res, const boost::intrusive_ptr<backend::statement>& stmt) 
  : res_(res)
  , stmt_(stmt)
  , current_col_(0) 
{
}

inline bool row::is_null(int col)
{
    res_->is_null(col);
}

inline bool row::is_null(const string_ref& n)
{
    int c = res_->name_to_column(n);
    if (c < 0)
        throw invalid_column();

    res_->is_null(c);
}

inline void row::rewind_column()
{
    current_col_ = 0;
}

inline bool row::fetch(int col, const fetch_types_variant& v)
{
    return res_->fetch(col, v);
}

template<typename T>
bool row::fetch(int col, T& v)
{
    return fetch_conversion<T>::fetch(*this, col, v);
}

template<typename T>
bool row::fetch(const string_ref& n, T& v)
{
    int c = res_->name_to_column(n);
    if (c < 0)
        throw invalid_column();      
    return fetch(c, v);
}

template<typename T>
bool row::fetch(T& v)
{
    return fetch(current_col_++, v);
}

template<typename T>
T row::get(const string_ref& name)
{
    T v=T();
    if(!fetch(name,v))
        throw null_value_fetch();
    return v;
}

template<typename T>
T row::get(int col)
{
    T v=T();
    if(!fetch(col,v))
        throw null_value_fetch();
    return v;
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
row& operator>>(row& r, detail::tag<T1, T2> tag)
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
row& operator>>(row& r, T& v)
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
  
public:
    rowset_iterator(rowset<T>* rs = 0);

    boost::tribool has_next();

private:
    friend class boost::iterator_core_access;

    reference dereference() const;
    void increment();
    bool equal(rowset_iterator const& other) const;

    rowset<T>* rs_;
};

// -------- rowset_iterator<T> implementation ---------

template<typename T>
rowset_iterator<T>::rowset_iterator(rowset<T>* rs) : rs_(rs) 
{
    increment();
}

template<typename T>
boost::tribool rowset_iterator<T>::has_next()
{
    backend::result::next_row nr = rs_->row_.res_->has_next();
    if (backend::result::next_row_exists == nr)
        return true;
    else if (backend::result::last_row_reached == nr) 
        return false;
    else 
        return boost::tribool();
}

template<typename T>
typename rowset_iterator<T>::reference rowset_iterator<T>::dereference() const
{
    assert(rs_ && "Attempt to dereference end rowset_iterator");

    if (!fetch_conversion<T>::fetch(rs_->row_, 0, static_cast<T&>(*rs_)))
        throw null_value_fetch();

    return static_cast<T&>(*rs_);
}

template<typename T>
void rowset_iterator<T>::increment()
{
    if (rs_)
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


///
/// Represent select query result set. Has range interface, implements Single Pass Range concept. rowset can be iterated
/// only once. That means when begin has been called once, all subsequent calls to begin will give undefined behavior
///
template<typename T = row>
class rowset : private boost::mpl::if_<boost::is_same<T, row>, null_type, T>::type
{
    template<typename T1> friend class rowset_iterator;

public:
    typedef rowset_iterator<T> iterator;
    typedef rowset_iterator<T> const_iterator;

    /// 
    /// Construct rowset from backend result
    ///
    rowset(const boost::intrusive_ptr<backend::result>& res, const boost::intrusive_ptr<backend::statement>& stmt);
    ///
    /// Open rowset for traversion and return begin iterator
    ///
    rowset_iterator<T> begin();
    ///
    /// Return end iterator for rowset
    ///
    rowset_iterator<T> end();

    ///
    /// Provide conversion between rowset`s of different types.
    ///
    template<typename T1>
    operator rowset<T1>() const;

    unsigned long long rows();
    int columns();
    std::string column_name(int col);
    int column_index(const string_ref& n);
    int find_column(const string_ref& name);

private:
    row row_;

    boost::intrusive_ptr<backend::statement> stat_; //!< Rowset doesn`t use anything from statement, however
                                                    //!< we want to ensures that statement backend stay alive until
                                                    //!< alive we are.

    bool opened_;                                   //!< User have already called begin method
};

// -------- rowset implementation ---------

template<typename T>
rowset<T>::rowset(const boost::intrusive_ptr<backend::result>& res, const boost::intrusive_ptr<backend::statement>& stmt) 
    : row_(res, stmt)
    , opened_(false) 
{
}

template<typename T>
rowset_iterator<T> rowset<T>::begin()
{
    if (opened_)
        throw multiple_rowset_traverse("Impossible to open rowset_iterator twice");

    rowset_iterator<T> iter(this);
    opened_ = true;
    return iter;
}

template<typename T>
rowset_iterator<T> rowset<T>::end()
{
    return rowset_iterator<T>();
}

template<typename T>
template<typename T1>
rowset<T>::operator rowset<T1>() const
{
    return rowset<T1>(row_.res_);
}

template<typename T>
unsigned long long rowset<T>::rows()
{
    return row_.res_->rows();
}

template<typename T>
int rowset<T>::columns()
{
    return row_.res_->cols();
}

template<typename T>
std::string rowset<T>::column_name(int col)
{
    if (col < 0 || col >= columns())
        throw invalid_column();

    return row_.res_->column_to_name(col);
}

template<typename T>
int rowset<T>::column_index(const string_ref& n)
{
    int c = row_.res_->name_to_column(n);
    if (c < 0)
        throw invalid_column();

    return c;
}

template<typename T>
int rowset<T>::find_column(const string_ref& name)
{
    int c = row_.res_->name_to_column(name);
    if (c < 0)
        return -1;
    return c;
}


// row_iterator::dereference specialization for row. We should not call fetch, instead we just return stored row object;
template<>
inline rowset_iterator<row>::reference rowset_iterator<row>::dereference() const
{
    assert(rs_ && "Attempt to dereference end rowset_iterator");
    return rs_->row_;
}

/// Specialization of fetch_conversion for native types. 
template<typename T>
struct fetch_conversion<T, typename boost::enable_if< boost::mpl::contains<fetch_types, T*> >::type >
{
    static bool fetch(row& r, int col, T& v)
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
    static bool fetch(row& r, int col, T& v)
    {
        return r.fetch(col, fetch_types_variant(&v));
    }
};

}

#endif // EDBA_ROWSET_HPP
