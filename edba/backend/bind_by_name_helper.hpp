#ifndef EDBA_BACKEND_BIND_BY_NAME_HELPER_HPP
#define EDBA_BACKEND_BIND_BY_NAME_HELPER_HPP

#include <edba/backend/backend.hpp>

#include <boost/function.hpp>
#include <boost/unordered_map.hpp>

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
public:
    typedef boost::function<void(std::ostream& os, int col)> print_func_type;

    bind_by_name_helper(const string_ref& sql, const print_func_type& print_func)
        : sql_(sql.begin(), sql.end())
    {}

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
    virtual void bind_impl(string_ref name, bind_types_variant const& v)
    {
    }

private:
    typedef boost::unordered_multimap<string_ref, int> name_map_type;

    name_map_type name_map_;
    std::string sql_;
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
