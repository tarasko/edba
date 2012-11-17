#ifndef EDBA_TYPES_SUPPORT_BOOST_SCOPED_PTR_HPP
#define EDBA_TYPES_SUPPORT_BOOST_SCOPED_PTR_HPP

#include <edba/types.hpp>

#include <boost/scoped_ptr.hpp>

namespace edba 
{

template<typename T>
struct bind_conversion<boost::scoped_ptr<T>, void>
{
    template<typename ColOrName>
    static void bind(statement& st, ColOrName col_or_name, const boost::scoped_ptr<T>& v)
    {
        if (v)
            st.bind(col_or_name, *v);
        else
            st.bind(col_or_name, null);
    }
};

template<typename T>
struct fetch_conversion<boost::scoped_ptr<T>, typename boost::disable_if< boost::is_const<T> >::type>
{
    template<typename ColOrName>
    static bool fetch(row& res, ColOrName col_or_name, boost::scoped_ptr<T>& v)
    {
        if (!res.is_null(col_or_name))
        {
            boost::scoped_ptr<T> tmp(new T());
            res.fetch(col_or_name, *tmp);
            v.swap(tmp);
        }
        else
            v.reset();

        return true;
    }
};

}

#endif // EDBA_TYPES_SUPPORT_BOOST_SCOPED_PTR_HPP
