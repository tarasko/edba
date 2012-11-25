#ifndef EDBA_BACKEND_BIND_BY_NAME_HELPER_HPP
#define EDBA_BACKEND_BIND_BY_NAME_HELPER_HPP

#include <edba/backend/implementation_base.hpp>

#include <boost/function.hpp>
#include <boost/unordered_map.hpp>
#include <boost/range/algorithm/find.hpp>
#include <boost/range/algorithm/find_if.hpp>
#include <boost/range/algorithm/sort.hpp>
#include <boost/range/algorithm/equal_range.hpp>
#include <boost/range/algorithm/lexicographical_compare.hpp>
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
class bind_by_name_helper : public statement
{
    struct is_non_name_char
    {
        bool operator()(char c) const
        {
            return !(isalnum(c) || c == '_');
        }
    };

    // Map from parameter name to parameter index
    typedef std::vector< std::pair<std::string, int> > name_map_type;

    using statement::bind_impl;

public:
    typedef boost::function<void(std::ostream& os, int col)> print_func_type;

    bind_by_name_helper(session_monitor* sm, const string_ref& sql, const print_func_type& print_func)
      : statement(sm)
    {
        std::ostringstream patched_query;
        
        int idx = 1;
        string_ref rest = skip_until_semicolon(sql, patched_query);

        while (!rest.empty())
        {
            // Extract parameter name
            BOOST_AUTO(name, (boost::find_if<boost::return_begin_found>(rest, is_non_name_char())));

            // Push back new parameter into name map
            name_map_.resize(name_map_.size() + 1);
            name_map_.back().first.assign(name.begin(), name.end());
            name_map_.back().second = idx;

            // Append parameter into patched sql
            print_func(patched_query, idx++);

            // Evaluate rest of query
            rest = skip_until_semicolon(boost::make_iterator_range(name.end(), sql.end()), patched_query);
        }

        // Sort entries in name map by name
        // We do this because we want to apply equal_range algorithm further
        boost::sort(name_map_, string_ref_less());

        patched_query_ = patched_query.str();
    }

    /// 
    /// \brief Return query with binding markers suitable for backend.
    ///
    /// Backend should execute this statement instead of original.
    ///
    const std::string& patched_query() const
    {
        return patched_query_;
    }

    ///
    /// \brief Return total number of bind parameters in query
    ///
    size_t bindings_count() const
    {
        return name_map_.size();
    }

private:
    virtual void bind_impl(const string_ref& name, const bind_types_variant& v)
    {
        BOOST_AUTO(iter_pair, boost::equal_range(name_map_, name, string_ref_less()));

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
    std::string patched_query_;           //!< Sql built from original by replacing bind parameters with appropriate for backend
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
