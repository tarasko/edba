#ifndef EDBA_UTIL_H
#define EDBA_UTIL_H

#include <edba/defs.hpp>
#include <edba/errors.hpp>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/range/iterator_range.hpp>
#include <boost/range/as_literal.hpp>
#include <boost/smart_ptr/detail/atomic_count.hpp>

#include <string>
#include <sstream>
#include <ctime>
#include <map>

namespace edba {

class chptr_range : public boost::iterator_range<const char*> 
{
public:
    chptr_range() {}
    chptr_range(const char* str) : boost::iterator_range<const char*>(boost::as_literal(str)) {} 
    chptr_range(const std::string& str) : boost::iterator_range<const char*>(str.c_str(), str.c_str() + str.size()) {}
    chptr_range(const boost::iterator_range<const char*>& r) : boost::iterator_range<const char*>(r) {}
    chptr_range(const char* b, const char* e) : boost::iterator_range<const char*>(b, e) {}
    chptr_range(const char* b, size_t sz) : boost::iterator_range<const char*>(b, b + sz) {}

    struct iless
    {
        bool operator()(const chptr_range& r1, const chptr_range& r2) const
        {
            return boost::algorithm::ilexicographical_compare(r1, r2);
        }
    };

    struct less
    {
        bool operator()(const chptr_range& r1, const chptr_range& r2) const
        {
            return boost::algorithm::lexicographical_compare(r1, r2);
        }
    };
};

///
/// \brief parse a string as time value.
/// 
/// Used by backend implementations;
///
EDBA_API std::tm parse_time(char const *value);
///
/// \brief format a string as time value.
/// 
/// Used by backend implementations;
///
EDBA_API std::string format_time(std::tm const &v);
///
/// \brief parse a string as time value.
/// 
/// Used by backend implementations;
///
EDBA_API std::tm parse_time(std::string const &v);

template<typename IterRange>
void trim(IterRange& rng)
{
    while(!rng.empty() && isspace(rng.front()))
        rng.advance_begin(1);

    while(!rng.empty() && isspace(rng.back()))
        rng.advance_end(-1);
}

template<typename IterRange>
IterRange trim(const IterRange& rng)
{
    IterRange rng_copy = rng;
    trim(rng_copy);
    return rng_copy;
}

///
/// \brief select statement according to engine and version from statements list
///
/// For example:
///~SQLCE~                                                              -- this is only for SQLCE
///CREATE TABLE station_cfg                                             -- component's config, per station
///                         ( id NVARCHAR(36) DEFAULT '',               -- station UUID
///                           value NTEXT DEFAULT '' )                  -- value
///~ORACLE~                                                             -- this is only for ORACLE
///CREATE TABLE station_cfg                                             -- component's config, per station
///                         ( id CHARACTER VARYING(36) DEFAULT '',      -- station UUID
///                           value CLOB DEFAULT '' )                   -- value
///~Microsoft SQL Server/09.00~                                         -- this is only for MSSQL starting from version 09.00
///CREATE TABLE station_cfg                                             -- component's config, per station
///                         ( id CHARACTER VARYING(36) DEFAULT '',      -- station UUID
///                           value VARCHAR(MAX) DEFAULT '' )           -- value
///~~                                                                   -- this is for everything else
///CREATE TABLE station_cfg                                             -- component's config, per station
///                         ( id CHARACTER VARYING(36) DEFAULT '',      -- station UUID
///                           value TEXT DEFAULT '' )                   -- value
///~    
///
chptr_range select_statement(
    const chptr_range& rng
    , const std::string& engine
    , int ver_major
    , int ver_minor
    );

///
/// \brief select statements according to engine and version int statements batch separated with ;
/// 
std::string select_statements_in_batch(
    const chptr_range& rng
    , const std::string& engine
    , int ver_major
    , int ver_minor
    );

EDBA_API void parse_number(const chptr_range& r, short& num);
EDBA_API void parse_number(const chptr_range& r, unsigned short& num);
EDBA_API void parse_number(const chptr_range& r, int& num);
EDBA_API void parse_number(const chptr_range& r, unsigned int& num);
EDBA_API void parse_number(const chptr_range& r, long& num);
EDBA_API void parse_number(const chptr_range& r, unsigned long& num);
EDBA_API void parse_number(const chptr_range& r, long long& num);
EDBA_API void parse_number(const chptr_range& r, unsigned long long& num);
EDBA_API void parse_number(const chptr_range& r, float& num);
EDBA_API void parse_number(const chptr_range& r, double& num);
EDBA_API void parse_number(const chptr_range& r, long double& num);

struct ref_cnt : boost::noncopyable
{
    ref_cnt() : cnt_(0) {}
    virtual ~ref_cnt() {}

    void add_ref()
    {
        ++cnt_;
    }

    void release()
    {
        if (0 == --cnt_)
            delete this;
    }

private:
    boost::detail::atomic_count cnt_;
};

#define EDBA_ADD_INTRUSIVE_PTR_SUPPORT_FOR_TYPE(Type) \
    EDBA_API void intrusive_ptr_add_ref(Type* r); \
    EDBA_API void intrusive_ptr_release(Type* r);

#define EDBA_ADD_INTRUSIVE_PTR_SUPPORT_FOR_TYPE_IMPL(Type) \
    void intrusive_ptr_add_ref(Type* r) \
    { \
        r->add_ref(); \
    } \
    void intrusive_ptr_release(Type* r) \
    { \
        r->release(); \
    }

}          // namespace edba

#endif     // EDBA_UTIL_H
