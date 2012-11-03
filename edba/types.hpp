#ifndef EDBA_TYPES_HPP
#define EDBA_TYPES_HPP

#include <edba/string_ref.hpp>

#include <boost/mpl/vector.hpp>
#include <boost/mpl/contains.hpp>
#include <boost/variant/variant.hpp>

#include <string>
#include <ctime>
#include <iosfwd>

namespace edba {

class statement;
class result;
namespace backend { class statement; }

/// Null type
struct null_type {};
// EDBA_API null_type null;

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
    static void bind(statement& st, const T& v);
};

template<typename T, typename Enable = void>
struct fetch_conversion
{
    static void fetch(result& res, T& v);
};

template<typename T>
struct bind_conversion<T, typename boost::enable_if< boost::mpl::contains<T, bind_types> >::type>
{
    static void bind(statement& st, const T& v)
    {
    }
};

template<typename T>
struct fetch_conversion<T, typename boost::enable_if< boost::mpl::contains<T, fetch_types> >::type>
{
    static bool fetch(result& r, T& v, int column)
    {
    }
};

}

#endif // EDBA_TYPES_HPP
