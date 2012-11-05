#ifndef EDBA_SESSION_MONITOR_HPP
#define EDBA_SESSION_MONITOR_HPP

#include <string>

namespace edba {

/// 
/// \brief Interface for monitoring session statements executing
///
class session_monitor
{
public:
    virtual ~session_monitor() {}

    ///
    /// Called after statement has been executed. 
    /// \param bindings - commaseparated list of bindings, ready for loggging. Empty if there are no bindings
    /// \param ok - false when error occurred
    /// \param execution_time - time that has been taken to execute row
    /// \param rows_affected - rows affected during execution. 0 on errors
    ///
    virtual void statement_executed(
        const char*        // sql
      , const std::string& // bindings
      , bool               // ok
      , double             // execution_time
      , unsigned long long // rows_affected
      )
    {}

    ///
    /// Called after query has been executed. 
    /// \param bindings - commaseparated list of bindings, ready for loggging. Empty if there are no bindings
    /// \param ok - false when error occurred
    /// \param execution_time - time that has been taken to execute row
    /// \param rows_read - rows read. 0 on errors
    ///
    virtual void query_executed(
        const char*          // sql
      , const std::string&   // bindings
      , bool                 // ok
      , double               // execution_time
      , unsigned long long   // rows_read
      )
    {}

    virtual void transaction_started() {}
    virtual void transaction_committed() {}
    virtual void transaction_reverted() {}
};

}

#endif // EDBA_SESSION_MONITOR_HPP