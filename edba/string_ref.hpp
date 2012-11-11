#ifndef EDBA_STRING_REF_HPP
#define EDBA_STRING_REF_HPP

#include <boost/range/iterator_range.hpp>
#include <boost/range/as_literal.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/functional/hash.hpp>

#include <string>
#include <ostream>

namespace edba
{

class string_ref : public boost::iterator_range<const char*> 
{
public:
    string_ref() {}
    string_ref(const char* str) : boost::iterator_range<const char*>(boost::as_literal(str)) {} 
    string_ref(const std::string& str) : boost::iterator_range<const char*>(str.c_str(), str.c_str() + str.size()) {}
    string_ref(const boost::iterator_range<const char*>& r) : boost::iterator_range<const char*>(r) {}
    string_ref(const char* b, const char* e) : boost::iterator_range<const char*>(b, e) {}
    string_ref(const char* b, size_t sz) : boost::iterator_range<const char*>(b, b + sz) {}
};

inline std::size_t hash_value(string_ref const& str)
{
    return boost::hash_range(str.begin(), str.end());
}

inline std::ostream& operator<<(std::ostream& os, const string_ref& s)
{
    os.write(s.begin(), s.size());
    return os;
}

template<typename T>
string_ref to_string_ref(const T& obj)
{
    return string_ref(obj);
}

template<typename T>
string_ref to_string_ref(const std::pair<std::string, T>& obj)
{
    return string_ref(obj.first);
}

struct string_ref_iless
{
    template<typename T1, typename T2>
    bool operator()(const T1& r1, const T2& r2) const
    {
        return boost::algorithm::ilexicographical_compare(to_string_ref(r1), to_string_ref(r2));
    }
};

struct string_ref_less
{
    template<typename T1, typename T2>
    bool operator()(const T1& r1, const T2& r2) const
    {
        return boost::algorithm::lexicographical_compare(to_string_ref(r1), to_string_ref(r2));
    }
};

}

#endif // EDBA_STRING_REF_HPP
