#pragma once
#ifndef EDBA_TYPES_SUPPORT_QUANTLIB_DATE_HPP
#define EDBA_TYPES_SUPPORT_QUANTLIB_DATE_HPP

#include <edba/types.hpp>

#include <ql/time/date.hpp>

namespace edba 
{

template<>
struct bind_conversion<QuantLib::Date, void>
{
    template<typename ColOrName>
    static void bind(statement& st, ColOrName col_or_name, const QuantLib::Date& v)
    {
        std::tm tm_struct;
        ::memset(&tm_struct, 0, sizeof(std::tm));
        tm_struct.tm_mday = v.dayOfMonth();
        tm_struct.tm_mon = v.month() - 1;
        tm_struct.tm_year = v.year() - 1900;
        st.bind(col_or_name, tm_struct);
    }
};

template<>
struct fetch_conversion<QuantLib::Date, void>
{
    template<typename ColOrName>
    static bool fetch(const row& res, ColOrName col_or_name, QuantLib::Date& v)
    {
        std::tm tm_struct;
        bool ret = res.fetch(col_or_name, tm_struct);
        if (ret)
            v = QuantLib::Date(
                static_cast<QuantLib::Day>(tm_struct.tm_mday),
                static_cast<QuantLib::Month>(tm_struct.tm_mon + 1),
                static_cast<QuantLib::Year>(tm_struct.tm_year + 1900));

        return ret;
    }
};

}

#endif // EDBA_TYPES_SUPPORT_QUANTLIB_DATE_HPP
