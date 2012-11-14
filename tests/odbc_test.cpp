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

template<typename Driver>
void test(const char* conn_string)
{
    const char* create_test1_table = 
        "~Microsoft SQL Server~create table ##test1( "
        "   id int identity(1, 1) primary key clustered, "
        "   num numeric(18, 3), "
        "   dt datetime, "
        "   dt_small smalldatetime, "
        "   vchar20 nvarchar(20), "
        "   vcharmax nvarchar(max), "
        "   vbin20 varbinary(20), "
        "   vbinmax varbinary(max), "
        "   txt text ) "
        "~Sqlite3~create temp table test1( "
        "   id integer primary key autoincrement, "
        "   num double, "
        "   dt text, "
        "   dt_small text, "
        "   vchar20 varchar(20), "
        "   vcharmax text, "
        "   vbin20 blob, "
        "   vbinmax blob, "
        "   txt text ) "
        "~Mysql~create temporary table test1( "
        "   id integer AUTO_INCREMENT PRIMARY KEY, "
        "   num numeric(18, 3), "
        "   dt timestamp, "
        "   dt_small date, "    
        "   vchar20 varchar(20), "
        "   vcharmax text, "
        "   vbin20 varbinary(20), "
        "   vbinmax blob, "
        "   txt text ) "
        "~~";

    const char* insert_test1_data =
        "~Microsoft SQL Server~insert into ##test1(num, dt, dt_small, vchar20, vcharmax, vbin20, vbinmax, txt) "
        "   values(:num, :dt, :dt_small, :vchar20, :vcharmax, :vbin20, :vbinmax, :txt)"
        "~Sqlite3~insert into test1(num, dt, dt_small, vchar20, vcharmax, vbin20, vbinmax, txt)"
        "   values(:num, :dt, :dt_small, :vchar20, :vcharmax, :vbin20, :vbinmax, :txt)"
        "~~";

    const char* select_test1_row = 
        "~Microsoft SQL Server~select id, num, dt, dt_small, vchar20, vcharmax, vbin20, vbinmax, txt from ##test1 where id=:id"
        "~Sqlite3~select id, num, dt, dt_small, vchar20, vcharmax, vbin20, vbinmax, txt from test1 where id=:id"
        "~~";

    const char* drop_test1 = 
        "~Microsoft SQL Server~drop table ##test1"
        "~Sqlite3~drop table test1"
        "~~";

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
        session sess(Driver(), conn_string, &sm);

        // Create table
        sess.once() << create_test1_table << exec;

        // Compile statement for inserting data 
        statement st = sess << insert_test1_data;

        // Bind data to statement and execute two times
        st 
            << use("num", 10.10) 
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
                double num;
                std::tm tm1, tm2;
                std::string short_str;
                std::string long_str;
                std::ostringstream short_oss;
                std::ostringstream long_oss;
                std::string txt;

                r >> id >> num >> tm1 >> tm2 >> short_str >> long_str >> short_oss >> long_oss >> txt;

                assert(num == 10.10);
                assert(!memcmp(std::gmtime(&now), &tm1, sizeof(tm1)));
                assert(short_str == "Hello!");
                assert(long_str == "Hello! max");
                assert(short_oss.str() == short_binary);
                assert(long_oss.str() == long_binary);
                assert(text == txt);
            }
        }

        sess.exec_batch(
            "~Microsoft SQL Server~insert into ##test1(num) values(10.2)"
            "~Sqlite3~insert into test1(num) values(10.2)"
            "~~;"
            "~Microsoft SQL Server~insert into ##test1(num) values(10.3)"
            "~Sqlite3~insert into test1(num) values(10.3)"
            "~~");

        // Check that cache works as expected
        statement st1 = sess << insert_test1_data;
        assert(st == st1);

        // Try to bind non prepared statement
        // Test once helper 
        sess.once() << 
            "~Microsoft SQL Server~insert into ##test1(num) values(:num)"
            "~Sqlite3~insert into test1(num) values(:num)"
            "~~"
            << use("num", 10.5) 
            << exec;

        sess.once() << drop_test1 << exec;
    }
}

int main()
{
    try {
        test<edba::driver::mysql>("host=127.0.0.1;database=test;user=root;password=root;");
        test<edba::driver::odbc>("DSN=EDBA_TESTING_MSSQL");
        test<edba::driver::odbc_s>("DSN=EDBA_TESTING_MSSQL;@utf=wide");
        test<edba::driver::sqlite3>("db=test.db");
    }
    catch(std::exception& e)
    {
        cout << e.what() << endl;
    }

    return 0;
}
