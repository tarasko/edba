#ifndef EDBA_BACKEND_STATISTICS_HPP
#define EDBA_BACKEND_STATISTICS_HPP

#include <edba/session_monitor.hpp>
#include <edba/types.hpp>

#include <boost/timer.hpp>

#include <sstream>

namespace edba { namespace backend {

/// Wrap provided user session_monitor object, forward notifications to monitor if it is not null
/// Accumulate total time spent is database queries
struct session_stat
{
    session_stat(session_monitor* sm)
      : sm_(sm)
      , total_sec_(0.0)
    {
    }

    // Events required by connection object
    void transaction_started()
    {
        if (sm_)
            sm_->transaction_started();
    }

    void transaction_commited()
    {
        if (sm_)
            sm_->transaction_committed();
    }

    void transaction_reverted()
    {
        if (sm_)
            sm_->transaction_reverted();
    }

    /// Return total time spent in queries
    double total_execution_time() const
    {
        return total_sec_;
    }

    session_monitor* user_monitor()
    {
        return sm_;
    }

    void add_query_time(double sec)
    {
        total_sec_ += sec;
    }

private:
    session_monitor* sm_;
    double total_sec_;
};

struct statement_stat
{
    struct measure_query
    {
        measure_query(statement_stat* stat, const std::string* query, result_ptr* r);
        ~measure_query();

    private:
        statement_stat* stat_;
        const std::string* query_;
        result_ptr* r_;
    };

    struct measure_statement
    {
        measure_statement(statement_stat* stat, const std::string* query, statement_iface* r);
        ~measure_statement();

    private:
        statement_stat* stat_;
        const std::string* query_;
        statement_iface* st_;
    };

    statement_stat(session_stat* st)
      : session_stat_(st)
    {
    }

    void bind(const string_ref& name, const bind_types_variant& val);
    void bind(int col, const bind_types_variant& val);

    void reset_bindings();

private:
    /// Parent session statistics object
    session_stat* session_stat_;

    /// Accumulator for string representation of bounded parameters
    /// Used in session_monitor calls
    std::ostringstream bindings_;

    /// Used to evaluate time spent in query or statement
    boost::timer timer_;
};


}}

#endif
