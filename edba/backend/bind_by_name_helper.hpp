#ifndef EDBA_BACKEND_BIND_BY_NAME_HELPER_HPP
#define EDBA_BACKEND_BIND_BY_NAME_HELPER_HPP

#include <edba/backend/backend.hpp>

#include <boost/function.hpp>
#include <boost/unordered_map.hpp>
#include <boost/range/algorithm/find.hpp>
#include <boost/range/algorithm/find_if.hpp>
#include <boost/typeof/typeof.hpp>
#include <boost/foreach.hpp>

namespace edba { namespace backend {

///
/// \brief Provide implementation for binding::bind_impl(string_ref name, ...) using binding::bind_impl(int col, ...) method
///
/// Parse sql query on construction, and extract all parameters marked as ':paramname', assign numbers for each parameter
/// starting from 1. Use provided function to replace parameteres in the manner familiar for backend. 
/// Prepare new query for backend.
///
class bind_by_name_helper : public bindings
{
    struct is_non_name_char;
    typedef boost::unordered_multimap<std::string, int> name_map_type;

    using bindings::bind_impl;

public:
    typedef boost::function<void(std::ostream& os, int col)> print_func_type;

    bind_by_name_helper(const string_ref& sql, const print_func_type& print_func)
        : sql_(sql.begin(), sql.end())
    {
        std::ostringstream patched_sql;
        
        int idx = 1;
        string_ref rest = skip_until_semicolon(sql, patched_sql);

        while (!rest.empty())
        {
            string_ref name = boost::find_if<boost::return_begin_found>(rest, is_non_name_char());
            name_map_.emplace(std::string(name.begin(), name.end()), idx);

            print_func(patched_sql, idx++);

            rest = skip_until_semicolon(boost::make_iterator_range(name.end(), sql.end()), patched_sql);
        }

        sql_ = patched_sql.str();
    }

    /// 
    /// \brief Return query with binding markers suitable for backend.
    ///
    /// Backend should execute this statement instead of original.
    ///
    const std::string& sql() const
    {
        return sql_;
    }

private:
    struct is_non_name_char
    {
        bool operator()(char c) const
        {
            return !(isalnum(c) || c == '_');
        }
    };

    virtual void bind_impl(string_ref name, const bind_types_variant& v)
    {
        BOOST_AUTO(iter_pair, (name_map_.equal_range(std::string(name.begin(), name.end()))));
        if (boost::empty(iter_pair))
            throw invalid_placeholder();

        BOOST_FOREACH(const name_map_type::value_type& entry, iter_pair)
            bind_impl(entry.second, v);
    }

    // Return range from [next after semicolon, sql.end())
    string_ref skip_until_semicolon(const string_ref& sql, std::ostream& patched_sql)
    {
        string_ref semicolon = boost::find<boost::return_found_end>(sql, ':');
        patched_sql << boost::make_iterator_range(sql.begin(), semicolon.begin());
        if (!semicolon.empty())
            semicolon.advance_begin(1);
        return semicolon;
    }

private:
    name_map_type name_map_;
    std::string sql_;           //!< Sql built from original by replacing bind parameters with appropriate for backend
    int count_;                 //!< Bindings count
};

struct question_marker
{
    void operator()(std::ostream& os, int col)
    {
        os << '?';
    }
};

struct postgresql_style_marker
{
    void operator()(std::ostream& os, int col)
    {
        os << '$' << col;
    }
};

}} // namespace edba, backend

#endif // EDBA_BACKEND_FWD_HPP
