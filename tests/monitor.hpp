#pragma once

#include <edba/session_monitor.hpp>

#include <boost/test/test_tools.hpp>

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
        BOOST_MESSAGE("[SessionMonitor] exec: " << sql);
        if (!bindings.empty())
            BOOST_MESSAGE("[SessionMonitor] with bindings:" << bindings);
        if (ok)
            BOOST_MESSAGE("[SessionMonitor] took " << execution_time << " sec, rows affected " << rows_affected);
        else
            BOOST_MESSAGE("[SessionMonitor] FAILED\n");
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
        BOOST_MESSAGE("[SessionMonitor] query: " << sql);
        if (!bindings.empty())
            BOOST_MESSAGE("[SessionMonitor] with bindings:" << bindings);
        if (ok)
        {
            if (rows_read == -1)
                BOOST_MESSAGE("[SessionMonitor] took " << execution_time << " sec\n");
            else
                BOOST_MESSAGE("[SessionMonitor] took " << execution_time << " sec, rows selected " << rows_read);
        }
        else
            BOOST_MESSAGE("[SessionMonitor] FAILED\n");
    }

    virtual void transaction_started() 
    {
        BOOST_MESSAGE("[SessionMonitor] Transaction started\n");
    }
    virtual void transaction_committed() 
    {
        BOOST_MESSAGE("[SessionMonitor] Transaction committed\n");
    }
    virtual void transaction_reverted() 
    {
        BOOST_MESSAGE("[SessionMonitor] Transaction reverted\n");
    }
};
