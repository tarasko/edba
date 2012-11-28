#ifndef EDBA_TYPES_SUPPORT_BOOST_OPTIONAL_HPP
#define EDBA_TYPES_SUPPORT_BOOST_OPTIONAL_HPP

#include <edba/types.hpp>

#include <boost/optional/optional.hpp>
#include <boost/move/move.hpp>

namespace edba 
{

template<typename T>
struct bind_conversion<boost::optional<T>, void>
{
    template<typename ColOrName>
    static void bind(statement& st, ColOrName col_or_name, const boost::optional<T>& v)
    {
        if (v)
            st.bind(col_or_name, *v);
        else
            st.bind(col_or_name, null);
    }
};

template<typename T>
struct fetch_conversion<boost::optional<T>, void>
{
    template<typename ColOrName>
    static bool fetch(const row& res, ColOrName col_or_name, boost::optional<T>& v)
    {
        T object;
        if (res.fetch(col_or_name, object))
            v = boost::move(object);

        return true;
    }
};

}

#endif // EDBA_TYPES_SUPPORT_BOOST_OPTIONAL_HPP
