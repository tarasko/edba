#ifndef EDBA_TYPES_HPP
#define EDBA_TYPES_HPP

#include <edba/string_ref.hpp>

#include <boost/mpl/vector.hpp>
#include <boost/mpl/contains.hpp>
#include <boost/variant/variant.hpp>
#include <boost/variant/get.hpp>

#include <string>
#include <ctime>
#include <iosfwd>

namespace edba {

class statement;
class result;

namespace backend { class statement; }

/// Null type
struct null_type {};

/// Types natively supported by statement::bind method
typedef boost::mpl::vector<
    null_type
  , short
  , unsigned short
  , int
  , unsigned int
  , long
  , unsigned long
  , long long
  , unsigned long long
  , float
  , double
  , long double
  , string_ref
  , std::tm
  , std::istream*
  > bind_types;

/// Types natively supported by result::fetch method
typedef boost::mpl::vector<
    short
  , unsigned short
  , int
  , unsigned int
  , long
  , unsigned long
  , long long
  , unsigned long long
  , float
  , double
  , long double
  , std::string
  , std::tm
  , std::ostream*
  > fetch_types;

typedef boost::make_variant_over<bind_types>::type bind_types_variant;
typedef boost::make_variant_over<fetch_types>::type fetch_types_variant;

template<typename T, typename Enable = void>
struct bind_conversion
{
    template<typename ColOrName>
    static void bind(statement& st, ColOrName col_or_name, const T& v)
    {
        BOOST_MPL_ASSERT_MSG(false, ADD_SPECIALIZATION_OF_BIND_CONVERSION_FOR_TYPE, ((T)));
    }
};

template<typename T, typename Enable = void>
struct fetch_conversion
{
    template<typename ColOrName>
    static void fetch(result& res, ColOrName col_or_name, T& v)
    {
        BOOST_MPL_ASSERT_MSG(false, ADD_SPECIALIZATION_OF_FETCH_CONVERSION_FOR_TYPE, ((T)));
    }
};

/// Specialization for bind native types
template<typename T>
struct bind_conversion<T, typename boost::enable_if< boost::mpl::contains<bind_types, T> >::type>
{
    template<typename ColOrName>
    static void bind(statement& st, ColOrName col_or_name, const T& v)
    {
        st.bind(col_or_name, bind_types_variant(v));
    }
};

/// Specialization for fetch native types
template<typename T>
struct fetch_conversion<T, typename boost::enable_if< boost::mpl::contains<fetch_types, T> >::type>
{
    static bool fetch(result& r, int col, T& v)
    {
        fetch_types_variant var = T();
        if (!r.fetch(col, var))
            return false;

        v = boost::get<T>(var);
        return true;
    }
};

}

#endif // EDBA_TYPES_HPP
