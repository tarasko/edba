#include <edba/edba.hpp>

#include <boost/iostreams/device/mapped_file.hpp>
#include <boost/foreach.hpp>
#include <boost/timer.hpp>

#include <iostream>
#include <ctime>

using namespace std;

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
        clog << "[SessionMonitor] exec: " << sql << endl;
        if (!bindings.empty())
            clog << "[SessionMonitor] with bindings:" << bindings << endl;
        if (ok)
            clog << "[SessionMonitor] took " << execution_time << " sec, rows affected " << rows_affected << endl;
        else
            clog << "[SessionMonitor] FAILED\n";
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
        clog << "[SessionMonitor] query: " << sql << endl;
        if (!bindings.empty())
            clog << "[SessionMonitor] with bindings:" << bindings << endl;
        if (ok)
        {
            if (rows_read == -1)
                clog << "[SessionMonitor] took " << execution_time << " sec\n";
            else
                clog << "[SessionMonitor] took " << execution_time << " sec, rows selected " << rows_read << endl;
        }
        else
            clog << "[SessionMonitor] FAILED\n";
    }

    virtual void transaction_started() 
    {
        clog << "[SessionMonitor] Transaction started\n";
    }
    virtual void transaction_committed() 
    {
        clog << "[SessionMonitor] Transaction committed\n";
    }
    virtual void transaction_reverted() 
    {
        clog << "[SessionMonitor] Transaction reverted\n";
    }
};

void test_odbc(const char* conn_string)
{
    const char* create_test1_table = 
        "~Microsoft SQL Server~create table ##test1( "
        "   id int identity(1, 1) primary key clustered, "
        "   dec numeric(18, 3), "
        "   dt datetime, "
        "   dt_small smalldatetime, "
        "   vchar20 nvarchar(20), "
        "   vcharmax nvarchar(max), "
        "   vbin20 varbinary(20), "
        "   vbinmax varbinary(max), "
        "   txt text ) "
        "~~";

    const char* insert_test1_data =
        "~Microsoft SQL Server~insert into ##test1(dec, dt, dt_small, vchar20, vcharmax, vbin20, vbinmax, txt) "
        "   values(:dec, :dt, :dt_small, :vchar20, :vcharmax, :vbin20, :vbinmax, :txt)";

    const char* select_test1_row = 
        "~Microsoft SQL Server~select id, dec, dt, dt_small, vchar20, vcharmax, vbin20, vbinmax, txt from ##test1~~";

    const char* drop_test1 = "drop table ##test1";

    std::string short_binary("binary");
    std::string long_binary(7000, 't');

    std::istringstream long_binary_stream;
    std::istringstream short_binary_stream;

    short_binary_stream.str(short_binary);
    long_binary_stream.str(long_binary);

    std::string text(10000, 'z');

    std::time_t now = std::time(0);

    {
        using namespace edba;

        monitor sm;
        session sess(driver::odbc(), conn_string, &sm);

        // Create table
        sess << create_test1_table << exec;

        // Compile statement for inserting data 
        statement st = sess << insert_test1_data;

        // Bind data to statement and execute two times
        st 
            << use("dec", 10.10) 
            << use("dt", *std::gmtime(&now)) 
            << use("dt_small", *std::gmtime(&now)) 
            << use("vchar20", "Hello!")
            << use("vcharmax", "Hello! max")
            << use("vbin20", &short_binary_stream)
            << use("vbinmax", &long_binary_stream)
            << use("txt", text)
            << exec
            << exec;

        long long id = st.last_insert_id();

        // Exec with null types
        st << reset
           << null 
           << null
           << null
           << null
           << null
           << null
           << null
           << null
           << exec;

        // Query single row
        {
            row r = sess << select_test1_row << id << first_row;
    
            {
                long long id;
                double dec;
                std::tm tm1, tm2;
                std::string short_str;
                std::string long_str;
                std::ostringstream short_oss;
                std::ostringstream long_oss;
                std::string txt;

                r >> id >> dec >> tm1 >> tm2 >> short_str >> long_str >> short_oss >> long_oss >> txt;

                assert(dec == 10.10);
                assert(!memcmp(std::gmtime(&now), &tm1, sizeof(tm1)));
                assert(short_str == "Hello!");
                assert(long_str == "Hello! max");
                assert(short_oss.str() == short_binary);
                assert(long_oss.str() == long_binary);
                assert(text == txt);
            }
        }

        sess.exec_batch("~Microsoft SQL Server~insert into ##test1(dec) values(10.2); insert into ##test1(dec) values(10.3)~~");

        sess << drop_test1 << exec;
    }
}

int main()
{
    test_odbc("DSN=EDBA_TESTING_MSSQL");
    test_odbc("DSN=EDBA_TESTING_MSSQL;@utf=wide");

    return 0;
}
