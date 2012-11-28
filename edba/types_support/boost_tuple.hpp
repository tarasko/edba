#if !defined(BOOST_PP_IS_ITERATING)

#  ifndef EDBA_TYPES_SUPPORT_BOOST_TUPLE_HPP
#  define EDBA_TYPES_SUPPORT_BOOST_TUPLE_HPP

#  include <edba/types.hpp>

#  include <boost/tuple/tuple.hpp>

namespace edba {

#  include <boost/preprocessor/iteration/iterate.hpp>
#  include <boost/preprocessor/iteration/local.hpp>
#  include <boost/preprocessor/repetition/enum_params.hpp>

#  define BOOST_PP_ITERATION_PARAMS_1 (3, (1, 10, "boost_tuple.hpp"))
#  include BOOST_PP_ITERATE()

}

#  endif        // EDBA_TYPES_SUPPORT_BOOST_TUPLE_HPP

#else           // !defined(BOOST_PP_IS_ITERATING)

template<BOOST_PP_ENUM_PARAMS(BOOST_PP_ITERATION(), typename T)>
struct bind_conversion< boost::tuple<BOOST_PP_ENUM_PARAMS(BOOST_PP_ITERATION(), T)>, void >
{
    typedef boost::tuple<BOOST_PP_ENUM_PARAMS(BOOST_PP_ITERATION(), T)> tuple_type;

    template<typename ColOrName>
    static void bind(statement& st, ColOrName, const tuple_type& v)
    {

#  define BOOST_PP_LOCAL_MACRO(n) st << boost::get<n>(v);
#  define BOOST_PP_LOCAL_LIMITS (0, BOOST_PP_ITERATION() - 1)
#  include BOOST_PP_LOCAL_ITERATE()

    }
};

template<BOOST_PP_ENUM_PARAMS(BOOST_PP_ITERATION(), typename T)>
struct fetch_conversion< boost::tuple<BOOST_PP_ENUM_PARAMS(BOOST_PP_ITERATION(), T)>, void >
{
    typedef boost::tuple<BOOST_PP_ENUM_PARAMS(BOOST_PP_ITERATION(), T)> tuple_type;

    template<typename ColOrName>
    static bool fetch(const row& res, ColOrName, tuple_type& v)
    {

#  define BOOST_PP_LOCAL_MACRO(n) res >> boost::get<n>(v);
#  define BOOST_PP_LOCAL_LIMITS (0, BOOST_PP_ITERATION() - 1)
#  include BOOST_PP_LOCAL_ITERATE()

        return true;
    }
};

#endif          // !defined(BOOST_PP_IS_ITERATING)
