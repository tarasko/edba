#include <boost/config.hpp>

#if !defined(EDBA_TYPES_SUPPORT_STD_UNIQUE_PTR_HPP) && !defined(BOOST_NO_CXX11_SMART_PTR)
#define EDBA_TYPES_SUPPORT_STD_UNIQUE_PTR_HPP

#include <edba/statement.hpp>

#include <memory>

namespace edba
{

template<typename T>
struct bind_conversion<std::unique_ptr<T>, void>
{
    template<typename ColOrName>
    static void bind(statement& st, ColOrName col_or_name, const std::unique_ptr<T>& v)
    {
        if (v)
            st.bind(col_or_name, *v);
        else
            st.bind(col_or_name, null);
    }
};

template<typename T>
struct fetch_conversion<std::unique_ptr<T>, typename boost::disable_if< boost::is_const<T> >::type>
{
    template<typename ColOrName>
    static bool fetch(const row& res, ColOrName col_or_name, std::unique_ptr<T>& v)
    {
        std::unique_ptr<T> tmp(new T());

        if (res.fetch(col_or_name, *tmp))
            v = std::move(tmp);
        else
            v.reset();
            
        return true;
    }
};

}

#endif // EDBA_TYPES_SUPPORT_STD_UNIQUE_PTR_HPP
