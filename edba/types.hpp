#ifndef EDBA_TYPES_HPP
#define EDBA_TYPES_HPP

#include <edba/string_ref.hpp>
#include <edba/detail/utils.hpp>

#include <boost/mpl/vector.hpp>
#include <boost/mpl/contains.hpp>
#include <boost/mpl/or.hpp>
#include <boost/variant/variant.hpp>
#include <boost/variant/get.hpp>

#include <string>
#include <ctime>
#include <iosfwd>

namespace edba {

class conn_info;
class session;
class session_monitor;
class statement;
class row;

namespace backend { 

struct result_iface;
struct statement_iface;
struct connection_iface;

EDBA_ADD_INTRUSIVE_PTR_SUPPORT_FOR_TYPE(result_iface);
EDBA_ADD_INTRUSIVE_PTR_SUPPORT_FOR_TYPE(statement_iface);
EDBA_ADD_INTRUSIVE_PTR_SUPPORT_FOR_TYPE(connection_iface);

}

/// Null type
struct null_type {};

namespace {
///
/// Global instance of null_type, can be used in bind expressions
///
null_type null;
}

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

/// Types natively supported by backend::result::fetch method
typedef boost::mpl::vector<
    short*
  , unsigned short*
  , int*
  , unsigned int*
  , long*
  , unsigned long*
  , long long*
  , unsigned long long*
  , float*
  , double*
  , long double*
  , std::string*
  , std::tm*
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
        BOOST_MPL_ASSERT_MSG(false, ADD_SPECIALIZATION_OF_BIND_CONVERSION_FOR_TYPE, (T));
    }
};

template<typename T, typename Enable = void>
struct fetch_conversion
{
    template<typename ColOrName>
    static bool fetch(row& res, ColOrName col_or_name, T& v)
    {
        BOOST_MPL_ASSERT_MSG(false, ADD_SPECIALIZATION_OF_FETCH_CONVERSION_FOR_TYPE, (T));
    }
};

}

typedef edba::backend::connection_iface* (*connect_function_type)(const edba::conn_info& cs, edba::session_monitor* sm);

#endif // EDBA_TYPES_HPP
