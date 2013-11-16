#include "monitor.hpp"

#include <edba/edba.hpp>

#include <boost/foreach.hpp>
#include <boost/thread/thread.hpp>
#include <boost/exception/diagnostic_information.hpp>

#include <boost/test/unit_test.hpp>

using namespace std;
using namespace edba;

string test_string = "fuck fuck fuck";

void init_session(session& sess)
{
    statement st = sess << "insert into test(txt) values(:val)";

    transaction tr(sess);
    for (int i = 0; i < 100; ++i)
        st << test_string << exec << reset;

    tr.commit();
}

void thread_proc(session_pool& pool)
{
    try
    {
        for (int i = 0; i < 1000; ++i)
        {
            session sess = pool.open();
            rowset<string> rs = sess << "select txt from test";
            BOOST_FOREACH(const string& s, rs)
                BOOST_ASSERT(s == test_string);
        }
    }
    catch(...)
    {
        cerr << "Thread from pool failed: " << boost::current_exception_diagnostic_information() << endl;
    }
}

template<typename Driver>
void run_pool_test(const char* conn_str)
{
    monitor m;
    session_pool pool(Driver(), conn_str, 4);

    pool.open().once() << 
        "~Microsoft SQL Server~create table ##test(txt varchar(20))"
        "~Sqlite3~create temp table test(txt varchar(20))" << exec;

    pool.invoke_on_connect(&init_session);

    // Create 4 worker threads and let them concurrently read from database
    boost::thread_group tg;
    for(int i = 0; i < 8; ++i)
        tg.create_thread(boost::bind(thread_proc, boost::ref(pool)));

    tg.join_all();
}

BOOST_AUTO_TEST_CASE(SessionPoolSqlite3)
{
    run_pool_test<driver::sqlite3>("db=test.db");
}

BOOST_AUTO_TEST_CASE(SessionPoolODBC)
{
    run_pool_test<driver::odbc>("Driver={SQL Server Native Client 10.0}; Server=edba-test\\SQLEXPRESS; Database=EDBA; UID=sa;PWD=1;");
}

