#ifndef EDBA_ERRORS_H
#define EDBA_ERRORS_H

#include <edba/detail/exports.hpp>

#include <boost/lexical_cast.hpp>

#include <stdexcept>
#include <string>

namespace edba {

/// \brief This is the base error of all errors thrown by edba.
class EDBA_API edba_error : public std::runtime_error
{
public:
    /// Create a edba_error with error message \a v
    edba_error(std::string const &v) : std::runtime_error(v) {}
};

/// \brief Some required part in connection string was ommited
class EDBA_API invalid_connection_string : public edba_error
{
public:
    /// Create a edba_error with error message \a v
    invalid_connection_string(std::string const &v) : edba_error(v) {}
};

/// \brief invalid data conversions
///
/// It may be thrown if the data can't be converted to required format, for example trying to fetch
/// a negative value with unsigned type or parsing invalid string as datatime.
class EDBA_API bad_value_cast : public edba_error
{
public:
    bad_value_cast() : edba_error("edba::bad_value_cast can't convert data")
    {
    }
};

/// \brief attempt to fetch a null value.
///
/// Thrown by edba::result::get functions.
class EDBA_API null_value_fetch : public edba_error
{
public:
    null_value_fetch(const std::string& column_name)
      : edba_error("edba::null_value_fetch attempt to fetch null value from column " + column_name)
    {
    }
};

/// \brief attempt to fetch a value from the row without calling next() first time or when next() returned false.
class EDBA_API empty_row_access : public edba_error
{
public:
    empty_row_access() : edba_error("edba::empty_row_access unable to get row from empty rowset")
    {
    }
};

/// \brief trying to fetch a value using invalid column index
class EDBA_API invalid_column : public edba_error
{
public:
    invalid_column(const std::string& name)
      : edba_error("edba::invalid_column attempt to bind or fetch by invalid column name: " + name)
    {
    }

    invalid_column(const int& col)
      : edba_error("edba::invalid_column attempt to bind or fetch by invalid column index " + boost::lexical_cast<std::string>(col))
    {
    }
};

/// \brief trying to fetch a single row for a query that returned multiple ones.
class EDBA_API multiple_rows_query : public edba_error
{
public:
    multiple_rows_query() : edba_error(	"edba::multiple_rows_query "
        "multiple rows result for a single row request")
    {
    }
};

/// \brief trying to execute empty string as query
class EDBA_API empty_session : public edba_error
{
public:
    empty_session(const char* method)
      : edba_error(std::string("edba::empty_session ") + "attempt to run " + method + " on empty session")
    {
    }
};

/// \brief trying to execute empty string as query
class EDBA_API empty_statement : public edba_error
{
public:
    empty_statement(const char* method)
      : edba_error(std::string("edba::empty_statement ") + "attempt to run " + method + " on empty statement")
    {
    }
};

/// \brief This operation is not supported by the backend
class EDBA_API not_supported_by_backend : public edba_error
{
public:
    /// Create a not_supported_by_backend with error message \a e
    not_supported_by_backend(std::string const &e) : edba_error(e)
    {
    }
};

/// \brief Attempt to create row iterator for a second time
class EDBA_API multiple_rowset_traverse : public edba_error
{
public:
    multiple_rowset_traverse(std::string const &e) : edba_error(e)
    {
    }
};

}

#endif
