#pragma once

#include <edba/session_monitor.hpp>

#include <iostream>

struct monitor : edba::session_monitor
{
    ///
    /// Called after statement has been executed. 
    /// \param bindings - commaseparated list of bindings, ready for loggging. Empty if there are no bindings
    /// \param ok - false when error occurred
    /// \param execution_time - time that has been taken to execute row
    /// \param rows_affected - rows affected during execution. 0 on errors
    ///
    virtual void statement_executed(
        const char* sql
      , const std::string& bindings
      , bool ok
      , double execution_time
      , unsigned long long rows_affected
      )
    {
        std::clog << "[SessionMonitor] exec: " << sql << std::endl;
        if (!bindings.empty())
            std::clog << "[SessionMonitor] with bindings:" << bindings << std::endl;
        if (ok)
            std::clog << "[SessionMonitor] took " << execution_time << " sec, rows affected " << rows_affected << std::endl;
        else
            std::clog << "[SessionMonitor] FAILED\n";
    }

    ///
    /// Called after query has been executed. 
    /// \param bindings - commaseparated list of bindings, ready for loggging. Empty if there are no bindings
    /// \param ok - false when error occurred
    /// \param execution_time - time that has been taken to execute row
    /// \param rows_read - rows read. 0 on errors
    ///
    virtual void query_executed(
        const char* sql
      , const std::string& bindings
      , bool ok
      , double execution_time
      , unsigned long long rows_read
      )
    {
        std::clog << "[SessionMonitor] query: " << sql << std::endl;
        if (!bindings.empty())
            std::clog << "[SessionMonitor] with bindings:" << bindings << std::endl;
        if (ok)
        {
            if (rows_read == -1)
                std::clog << "[SessionMonitor] took " << execution_time << " sec\n";
            else
                std::clog << "[SessionMonitor] took " << execution_time << " sec, rows selected " << rows_read << std::endl;
        }
        else
            std::clog << "[SessionMonitor] FAILED\n";
    }

    virtual void transaction_started() 
    {
        std::clog << "[SessionMonitor] Transaction started\n";
    }
    virtual void transaction_committed() 
    {
        std::clog << "[SessionMonitor] Transaction committed\n";
    }
    virtual void transaction_reverted() 
    {
        std::clog << "[SessionMonitor] Transaction reverted\n";
    }
};
