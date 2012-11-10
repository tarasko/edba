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
        "   dec decimal(28), "
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

    using namespace edba;

    session sess(driver::odbc(), conn_string);

    // Create table
    sess << create_test1_table << exec;

    // Compile statement for inserting data 
    statement st = sess << insert_test1_data;

    std::string long_string(10000, 'z');
    std::istringstream long_string_stream;
    std::istringstream short_string_stream;

    short_string_stream.str("zzzzzzzzzzz");
    long_string_stream.str("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz");

    std::time_t now = std::time(0);

    // Bind data to statement and execute
    st 
        << use("dec", 10.10) 
        << use("dt", *std::gmtime(&now)) 
        << use("dt_small", *std::gmtime(&now)) 
        << use("vchar20", "Hello!")
        << use("vcharmax", "Hello! max")
        << use("vbin20", &short_string_stream)
        << use("vbinmax", &long_string_stream)
        << use("txt", long_string)
        << exec;

    long long id = st.last_insert_id();

    statement st1 = sess << "~Microsoft SQL Server~insert into ##test1(dec, dt, dt_small, vchar20, vcharmax, vbin20, vbinmax) "
        "   values(:dec, :dt, :dt_small, :vchar20, :vcharmax, :vbin20, :vbinmax)";


    // Exec with null types
    st1 << reset
       << null_type() 
       << null_type()
       << null_type()
       << null_type()
       << null_type()
       << null_type()
       << null_type()
       << exec;
}

int main()
{
    test_odbc("DSN=EDBA_TESTING_MSSQL");

    //test_simple(sqlite3_lib, "sqlite3", "db=db.db");
    //test_simple(postgres_lib, "postgres", "user = postgres; password = postgres;");
    //test_simple(odbc_lib, "odbc", "DSN=PostgreSQL35W; @utf=wide");
    //test_simple(mysql_lib, "mysql", "user=root; password=root; database=test");
    //test_simple(odbc_lib, "odbc", "DSN=MySQLDS");
    //test_simple(oracle_lib, "oracle", "User=system; Password=root; ConnectionString=localhost/XE");

    //test_initdb(sqlite3_lib, "sqlite3", "db=dbinternal.dbs");
    //test_initdb(postgres_lib, "postgres", "user = postgres; password = postgres;");
    //test_initdb(odbc_lib, "odbc", "DSN=PostgreSQL35W; @utf=wide");
    return 0;
}
