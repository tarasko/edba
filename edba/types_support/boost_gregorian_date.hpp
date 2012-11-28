#ifndef EDBA_TYPES_SUPPORT_BOOST_GREGORIAN_DATE_HPP
#define EDBA_TYPES_SUPPORT_BOOST_GREGORIAN_DATE_HPP

#include <edba/types.hpp>

#include <boost/date_time/gregorian/greg_date.hpp>
#include <boost/date_time/gregorian/conversion.hpp>

namespace edba 
{

template<>
struct bind_conversion<boost::gregorian::date, void>
{
    template<typename ColOrName>
    static void bind(statement& st, ColOrName col_or_name, const boost::gregorian::date& v)
    {
        st.bind(col_or_name, boost::gregorian::to_tm(v));
    }
};

template<>
struct fetch_conversion<boost::gregorian::date, void>
{
    template<typename ColOrName>
    static bool fetch(const row& res, ColOrName col_or_name, boost::gregorian::date& v)
    {
        std::tm tm_struct;
        bool ret = res.fetch(col_or_name, tm_struct);
        if (ret)
            v = boost::gregorian::date_from_tm(tm_struct);

        return ret;
    }
};

}

#endif // EDBA_TYPES_SUPPORT_BOOST_GREGORIAN_DATE_HPP
