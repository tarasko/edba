#ifndef EDBA_UTIL_H
#define EDBA_UTIL_H

#include <edba/detail/exports.hpp>
#include <edba/errors.hpp>
#include <edba/string_ref.hpp>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/smart_ptr/detail/atomic_count.hpp>
#include <boost/preprocessor/stringize.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/noncopyable.hpp>

#include <cstdio>
#include <string>
#include <sstream>
#include <ctime>
#include <map>

namespace edba {

#ifdef _WIN32
#  define EDBA_MEMCPY(Dst, BufSize, Src, ToCopy) memcpy_s(Dst, BufSize, Src, ToCopy)
#  define EDBA_STRNCPY(Dest, Source, Size) strncpy_s(Dest, Source, Size)
#  define EDBA_SSCANF sscanf_s
#  define EDBA_SNPRINTF _snprintf_s
#else
#  define EDBA_MEMCPY(Dst, BufSize, Src, ToCopy) memcpy(Dst, Src, ToCopy)
#  define EDBA_STRNCPY(Dest, Source, Size) strncpy(Dest, Source, Size)
#  define EDBA_SSCANF sscanf
#  define EDBA_SNPRINTF snprintf
#endif

#define EDBA_MAKE_BACKEND_LIB_NAME(Name) EDBA_BACKEND_LIB_PREFIX BOOST_PP_STRINGIZE(Name) EDBA_BACKEND_LIB_SUFFIX

inline long long atoll(const char* val)
{
#ifdef _WIN32
    return _strtoi64(val, 0, 10);
#else
    return atoll(val);
#endif
}

/// \cond INTERNAL
namespace detail {
    /// Introduce new vocabulary type like std::pair<T1, T2> for use and into expressions
    template<typename First, typename Second>
    struct tag
    {
        tag(First first, Second second) : first_(first), second_(second) {}
        First first_;
        Second second_;
    };
} // detail
/// \endcond

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

template<typename T>
T* make_pointer(T& value)
{
    return &value;
};

template<typename T>
T* make_pointer(T* value)
{
    return value;
};

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
string_ref select_statement(
    const string_ref& rng
    , const std::string& engine
    , int ver_major
    , int ver_minor
    );

///
/// \brief select statements according to engine and version int statements batch separated with ;
///
std::string select_statements_in_batch(
    const string_ref& rng
    , const std::string& engine
    , int ver_major
    , int ver_minor
    );

EDBA_API void parse_number(const string_ref& r, short& num);
EDBA_API void parse_number(const string_ref& r, unsigned short& num);
EDBA_API void parse_number(const string_ref& r, int& num);
EDBA_API void parse_number(const string_ref& r, unsigned int& num);
EDBA_API void parse_number(const string_ref& r, long& num);
EDBA_API void parse_number(const string_ref& r, unsigned long& num);
EDBA_API void parse_number(const string_ref& r, long long& num);
EDBA_API void parse_number(const string_ref& r, unsigned long long& num);
EDBA_API void parse_number(const string_ref& r, float& num);
EDBA_API void parse_number(const string_ref& r, double& num);
EDBA_API void parse_number(const string_ref& r, long double& num);

struct ref_cnt : boost::noncopyable
{
    ref_cnt() : cnt_(0) {}
    virtual ~ref_cnt() {}

    virtual void before_destroy()
    {
    }

    void add_ref()
    {
        ++cnt_;
    }

    void release()
    {
        if (0 == --cnt_)
        {
            before_destroy();
            delete this;
        }
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
