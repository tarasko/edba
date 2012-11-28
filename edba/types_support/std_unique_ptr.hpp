#ifndef EDBA_TYPES_SUPPORT_STD_UNIQUE_PTR_HPP
#define EDBA_TYPES_SUPPORT_STD_UNIQUE_PTR_HPP

#include <edba/types.hpp>

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
        T tmp;
        if (res.fetch(col_or_name, tmp))
            v.reset(new T(std::move(tmp)));

        return true;
    }
};

}

#endif // EDBA_TYPES_SUPPORT_STD_UNIQUE_PTR_HPP
