#include "monitor.hpp"

#include <edba/edba.hpp>

#include <boost/test/minimal.hpp>
#include <boost/foreach.hpp>
#include <boost/thread/thread.hpp>

using namespace std;
using namespace edba;

string test_string = "fuck fuck fuck";

void init_session(session& sess)
{
    sess.once() << "create temp table test(txt varchar(20))" << exec;

    statement st = sess << "insert into test(txt) values(:val)";

    transaction tr(sess);
    for (int i = 0; i < 100; ++i)
        st << test_string << exec << reset;

    tr.commit();
}

void thread_proc(session_pool& pool)
{
    for (int i = 0; i < 1000; ++i)
    {
        session sess = pool.open();
        rowset<string> rs = sess << "select txt from test";
        BOOST_FOREACH(const string& s, rs)
            BOOST_CHECK(s == test_string);
    }
}

int test_main(int, char* [])
{
    monitor m;
    session_pool pool(driver::sqlite3_s(), "db=test.db", 2);
    pool.invoke_on_connect(&init_session);

    // Create 4 worker threads and let them concurrently read from database
    boost::thread_group tg;
    for(int i = 0; i < 4; ++i)
        tg.create_thread(boost::bind(thread_proc, boost::ref(pool)));

    tg.join_all();

    return 0;
}

