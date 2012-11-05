#ifndef EDBA_CONN_INFO_HPP
#define EDBA_CONN_INFO_HPP

#include <edba/detail/exports.hpp>
#include <edba/string_ref.hpp>

#include <boost/shared_ptr.hpp>

namespace edba {

/// \brief Parse a connection string \a cs into driver name \a driver_name and list of properties \a props
///
/// The connection string format is following:
///
/// \verbatim  driver:[key=value;]*  \endverbatim 
///
/// Where value can be either a sequence of characters (white space is trimmed) or it may be a general
/// sequence encloded in a single quitation marks were double quote is used for insering a single quote value.
///
/// Key values starting with \@ are reserved to be used as special edba  keys
/// For example:
///
/// \verbatim   mysql:username= root;password = 'asdf''5764dg';database=test;@use_prepared=off' \endverbatim 
///
/// Where driver is "mysql", username is "root", password is "asdf'5764dg", database is "test" and
/// special value "@use_prepared" is off - internal edba option.    class conn_info 
class EDBA_API conn_info
{
public:
    ///
    /// Split connection string to key-value pairs
    ///
    conn_info(const string_ref& conn_string);

    ///
    /// Return true if conn_info has specified key
    ///
    bool has(const char* key) const;

    ///
    /// Return value for specified key, if key not found return default value
    ///
    string_ref get(const char* key, const char* def = "") const;

    ///
    /// Return value for specified key, if key not found return default value
    /// Copy internal range to new string, this is helpfull for some driver that accept 
    /// only zero ended strings
    ///
    std::string get_copy(const char* key, const char* def = "") const;

    ///
    /// Return numeric value for specified key, if key not found return default value
    ///
    int get(const char* key, int def) const;

    ///
    /// Return connection string for driver without ebda specific tags
    ///
    const std::string& conn_string() const;

    ///
    /// Return connection string for postgresql driver
    /// Perform escaping and quoting according to postgresql rules 
    ///
    std::string pgsql_conn_string() const;

private:
    static void append_escaped(const string_ref& rng, std::string& dst);

    struct data;
    boost::shared_ptr<data> data_;
};

}

#endif // EDBA_CONN_INFO_HPP


