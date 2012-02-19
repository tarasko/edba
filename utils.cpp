#define EDBA_SOURCE

#include <edba/utils.hpp>
#include <edba/errors.hpp>

#include <boost/typeof/typeof.hpp>
#include <boost/foreach.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/find_iterator.hpp>

#include <time.h>
#include <stdio.h>
#include <string.h>
#include <sstream>
#include <locale>

#ifdef WIN32
#define EDBA_STRNCPY(Dest, Source, Size) strncpy_s(Dest, Source, Size)
#else
#define EDBA_STRNCPY(Dest, Source, Size) strncpy(Dest, Source, Size)
#endif

namespace edba {

std::string format_time(std::tm const &v)
{
    char buf[64]= {0};
    strftime(buf,sizeof(buf),"%Y-%m-%d %H:%M:%S",&v);
    return buf;
}
    
std::tm parse_time(std::string const &v)
{
    if(strlen(v.c_str())!=v.size())
        throw bad_value_cast();
    return parse_time(v.c_str());
}
std::tm parse_time(char const *v)
{
    std::tm t=std::tm();
    int n;
    double sec = 0;
    n = sscanf(v,"%d-%d-%d %d:%d:%lf",
                &t.tm_year,&t.tm_mon,&t.tm_mday,
                &t.tm_hour,&t.tm_min,&sec);
    if(n!=3 && n!=6)
    {
        throw bad_value_cast();
    }
    t.tm_year-=1900;
    t.tm_mon-=1;
    t.tm_isdst = -1;
    t.tm_sec=static_cast<int>(sec);
    if(mktime(&t)==-1)
        throw bad_value_cast();
    return t;
}

template<typename T>
void parse_number_impl(const chptr_range& r, const char* fmt, T& res)
{
    char buf[std::numeric_limits<T>::digits10 + 1];
    EDBA_STRNCPY(buf, r.begin(), r.size());
    if (0 == sscanf(buf, fmt, &res))
        throw bad_value_cast();
}
void parse_number(const chptr_range& r, short& num)
{
    parse_number_impl(r, "%hd", num);
}
void parse_number(const chptr_range& r, unsigned short& num)
{
    parse_number_impl(r, "%hu", num);
}
void parse_number(const chptr_range& r, int& num)
{
    parse_number_impl(r, "%d", num);
}
void parse_number(const chptr_range& r, unsigned int& num)
{
    parse_number_impl(r, "%u", num);
}
void parse_number(const chptr_range& r, long& num)
{
    parse_number_impl(r, "%ld", num);
}
void parse_number(const chptr_range& r, unsigned long& num)
{
    parse_number_impl(r, "%lu", num);
}
void parse_number(const chptr_range& r, long long& num)
{
    parse_number_impl(r, "%lld", num);
}
void parse_number(const chptr_range& r, unsigned long long& num)
{
    parse_number_impl(r, "%llu", num);
}
void parse_number(const chptr_range& r, float& num)
{
    parse_number_impl(r, "%f", num);
}
void parse_number(const chptr_range& r, double& num)
{
    parse_number_impl(r, "%lf", num);
}
void parse_number(const chptr_range& r, long double& num)
{
    parse_number_impl(r, "%Lf", num);
}

int parse_int_no_throw(const chptr_range& r)
{
    char buf[std::numeric_limits<int>::digits10 + 1];
    EDBA_STRNCPY(buf, r.begin(), r.size());
    return atoi(buf);
}

chptr_range select_statement(
    const chptr_range& _rng
  , const std::string& engine
  , int ver_major
  , int ver_minor
  )
{
    chptr_range rng = trim(_rng);
    if (rng.empty()) 
        throw edba_error("edba::select_statement empty sql provided");

    if ('~' != rng.front())
        return rng;
        
    rng.advance_begin(1);

    using namespace boost::algorithm;
        
    BOOST_AUTO(spl_iter, (make_split_iterator(rng, first_finder("~"))));
    BOOST_TYPEOF(spl_iter) spl_iter_end;

    for(; spl_iter != spl_iter_end; ++spl_iter)
    {
        chptr_range engine_with_version = *spl_iter++;

        if (engine_with_version.empty())
            return *spl_iter;

        BOOST_AUTO(eng_iter, (make_split_iterator(engine_with_version, first_finder("/."))));
        chptr_range eng_name = *eng_iter++;

        if (!eng_name.empty() && !boost::iequals(engine, eng_name))
            continue;

        // ok we have same engine
        int int_ver_major = parse_int_no_throw(*eng_iter++);
        int int_ver_minor = parse_int_no_throw(*eng_iter);

        if (ver_major < int_ver_major)
            continue;

        if (ver_major == int_ver_major && ver_minor < int_ver_minor)
            continue;
            
        return *spl_iter;
    }

    throw edba_error("edba::select_statement statement not found for current database");
}

std::string select_statements_in_batch(
    const chptr_range& rng
  , const std::string& engine
  , int ver_major
  , int ver_minor
  )
{
    std::string result;
    result.reserve(rng.size());

    using namespace boost::algorithm;

    BOOST_AUTO(spl_iter, (make_split_iterator(rng, first_finder(";"))));
    BOOST_TYPEOF(spl_iter) spl_iter_end;

    for(; spl_iter != spl_iter_end; ++spl_iter)
    {
        chptr_range st = trim(*spl_iter);
        if (st.empty()) 
            continue;

        const char* mark = std::find(st.begin(), st.end(), '~');
        if (st.end() == mark) 
            result.append(st.begin(), st.end());
        else
        {
            result.append(st.begin(), mark);
            chptr_range selected_st = select_statement(chptr_range(mark, st.end()), engine, ver_major, ver_minor);
            result.append(selected_st.begin(), selected_st.end());
        }

        result.append(";\n\n");
    }

    return result;
}

}  // namespace edba

