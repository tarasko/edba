#ifndef EDBA_ERRORS_H
#define EDBA_ERRORS_H

#include <stdexcept>

namespace edba {

///
/// \brief This is the base error of all errors thrown by edba.
///
class edba_error : public std::runtime_error {
public:
    ///
    /// Create a edba_error with error message \a v
    ///
    edba_error(std::string const &v) : std::runtime_error(v) {}
};

///
/// \brief invalid data conversions
///
/// It may be thrown if the data can't be converted to required format, for example trying to fetch
/// a negative value with unsigned type or parsing invalid string as datatime.
///
class bad_value_cast : public edba_error {
public:
    bad_value_cast() : edba_error("edba::bad_value_cast can't convert data")
    {
    }
};

///
/// \brief attempt to fetch a null value.
///
/// Thrown by edba::result::get functions.
///
class null_value_fetch : public edba_error {
public:
    null_value_fetch() : edba_error("edba::null_value_fetch attempt fetch null column")
    {
    }
};
///
/// \brief attempt to fetch a value from the row without calling next() first time or when next() returned false.
///
class empty_row_access : public edba_error {
public:
    empty_row_access() : edba_error("edba::empty_row_access attempt to fetch from empty column")
    {
    }
};

///
/// \brief trying to fetch a value using invalid column index
///
class invalid_column : public edba_error {
public:
    invalid_column() : edba_error("edba::invalid_column attempt access to invalid column")
    {
    }
};
///
/// \brief trying to fetch a value using invalid placeholder
///
class invalid_placeholder : public edba_error {
public:
    invalid_placeholder() : edba_error("edba::invalid_placeholder attempt bind to invalid placeholder")
    {
    }
};
///
/// \brief trying to fetch a single row for a query that returned multiple ones.
///
class multiple_rows_query : public edba_error {
public:
    multiple_rows_query() : edba_error(	"edba::multiple_rows_query "
        "multiple rows result for a single row request")
    {
    }
};

///
/// \brief trying to execute empty string as query
///
class empty_string_query : public edba_error {
public:
    empty_string_query() : edba_error(	"edba::empty_string_query "
        "attempt to execute and query empty string")
    {
    }
};

///
/// \brief This operation is not supported by the backend
///
class not_supported_by_backend : public edba_error {
public:
    ///
    /// Create a not_supported_by_backend with error message \a e
    ///
    not_supported_by_backend(std::string const &e) : edba_error(e)
    {
    }
};

}

#endif
