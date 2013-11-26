#include "monitor.hpp"

#include <edba/edba.hpp>

#include <edba/types_support/boost_gregorian_date.hpp>
#include <edba/types_support/boost_posix_time_ptime.hpp>
#include <edba/types_support/boost_optional.hpp>

#include <boost/date_time/gregorian/greg_date.hpp>
#include <boost/date_time.hpp>
#include <boost/optional/optional_io.hpp>
#include <boost/foreach.hpp>

#include <iostream>

using namespace std;
using namespace edba;
using namespace boost::posix_time;
using namespace boost::gregorian;
using boost::optional;

struct data 
{
    int foo;
    string bar;
};

int main()
{
    try 
    {
        monitor m;

        // Use sqlite3 driver to open database connection with provided connection string
        session sess(driver::sqlite3(), "db=test.db", &m);

        // Execute query. Special once marker specify that edba should not prepare statement and cache it.
        // This is the best option for queries executed only once during application lifetime.
        sess.once() << "create temp table hello(id integer primary key autoincrement, dt datetime, txt text)" << exec;

        // Prepare, cache and execute query.
        statement st = sess << "insert into hello(dt, txt) values(:dt, :txt)" 
            << date(2013, 7, 14)        // bind Boost.DateTime date
            << "Hello world"            // bind text
            << exec;                    // execute statement

        // Rebind parameters in previous statement and execute it
        st  << reset                                        // reset previous bindings
            << time_from_string("2013-7-14 7:40:00")        // bind Boost.DateTime ptime
            << null                                         // bind null
            << exec;                                        // re-execute insert statement

        cout << "Rows affected by last statement: " << st.affected() << endl;
        cout << "Last insert row id: " << st.last_insert_id() << endl;

        // Select rows.
        // Query execution is done by implicitly converting to rowset<T>
        rowset<> rs = sess << "select * from hello";

        // Loop over rows in rowset
        BOOST_FOREACH(row r, rs)
        {
            cout << "id: " << r.get<int>("id")
                 << "\tdt: " << r.get<ptime>("dt")
                 << "\ttxt: " << r.get< optional<string> >("txt") 
                 << endl;
        }

        cout << "Total time spent in queries in sec: " << sess.total_execution_time() << endl;

        data d;
        d.foo = 42;
        d.bar = "Hello";

        sess.set_specific(d);
        data& dref = sess.get_specific<data&>();
    }
    catch(std::exception& e)
    {
        cout << e.what() << endl;
    }

    return 0;
}
