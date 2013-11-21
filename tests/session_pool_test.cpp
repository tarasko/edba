#include "monitor.hpp"

#include <edba/edba.hpp>

#include <boost/foreach.hpp>
#include <boost/thread/thread.hpp>
#include <boost/exception/diagnostic_information.hpp>
#include <boost/atomic/atomic.hpp>

#include <boost/test/unit_test.hpp>

using namespace std;
using namespace edba;

const size_t DB_POOL_SIZE = 4;
const size_t THREAD_POOL_SIZE = 8;

boost::atomic<size_t> total_initialized_sessions(size_t(0));

void init_session(session& sess)
{
    sess.once() <<
        "~Microsoft SQL Server~create table #test(txt varchar(20))"
        "~~create temp table test(txt varchar(20))" << exec;

    total_initialized_sessions++;
}

void thread_proc(session_pool& pool)
{
    string test_string = "fuck fuck fuck";

    try
    {
        {
            session sess = pool.open();

            statement st = sess <<
                "~Microsoft SQL Server~insert into #test(txt) values(:val)"
                "~~insert into test(txt) values(:val)";

            transaction tr(sess);
            for (int i = 0; i < 100; ++i)
                st << test_string << exec << reset;

            tr.commit();
        }

        for (int i = 0; i < 100; ++i)
        {
            rowset<string> rs = pool.open() <<
                "~Microsoft SQL Server~select txt from #test"
                "~~select txt from test";

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
    session_pool pool(Driver(), conn_str, DB_POOL_SIZE);

    total_initialized_sessions = 0;
    pool.invoke_on_connect(&init_session);

    //pool.open().once() <<
    //    "~Microsoft SQL Server~create table ##test(txt varchar(20))"
    //    "~~create temp table test(txt varchar(20))" << exec;

    // Create 4 worker threads and let them concurrently read from database
    boost::thread_group tg;
    for(int i = 0; i < THREAD_POOL_SIZE; ++i)
        tg.create_thread(boost::bind(thread_proc, boost::ref(pool)));

    tg.join_all();

    BOOST_CHECK_LE(total_initialized_sessions, DB_POOL_SIZE);

    BOOST_TEST_MESSAGE(conn_str);
    BOOST_TEST_MESSAGE("session pool total exec time: " << pool.total_execution_time());
}

void throw_something(session sess)
{
    throw std::logic_error("intentional error");
}

// Regression, check that deadlock doesn`t occur when user init_session function throw exception
BOOST_AUTO_TEST_CASE(SessionPoolExceptionFromSessionInit)
{
    session_pool pool(driver::sqlite3(), "db=test.db", DB_POOL_SIZE);

    pool.invoke_on_connect(&throw_something);

    BOOST_CHECK_THROW((pool.open().once() <<
        "~Microsoft SQL Server~create table ##test(txt varchar(20))"
        "~Sqlite3~create temp table test(txt varchar(20))" << exec)
      , std::logic_error
      );
}

BOOST_AUTO_TEST_CASE(SessionPoolSqlite3)
{
    run_pool_test<driver::sqlite3>("db=test.db");
}

BOOST_AUTO_TEST_CASE(SessionPoolODBC)
{
    run_pool_test<driver::odbc>("Driver={SQL Server Native Client 10.0}; Server=edba-test\\SQLEXPRESS; Database=EDBA; UID=sa;PWD=1;");
}

