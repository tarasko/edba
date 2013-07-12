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

int main()
{
    try 
    {
        session sess(driver::sqlite3(), "db=test.db");

        sess << "create temp table hello(id integer primary key autoincrement, dt datetime, txt text)" << exec;

        statement st = sess << "insert into hello(dt, txt) values(:dt, :txt)" 
            << date(2013, 7, 14)        // bind Boost.DateTime date
            << "Hello world"            // bind text
            << exec;                    // execute statement

        st  << reset                                        // reset previous bindings
            << time_from_string("2013-7-14 7:40:00")        // bind Boost.DateTime ptime
            << null                                         // bind null
            << exec;                                        // re-execute insert statement

        cout << "Rows affected: " << st.affected() << endl;
        cout << "Last insert row id: " << st.last_insert_id() << endl;

        rowset<> rs = sess << "select * from hello";

        BOOST_FOREACH(row r, rs)
        {
            cout << "id: " << r.get<int>("id")
                 << "\tdt: " << r.get<ptime>("dt")
                 << "\ttxt: " << r.get< optional<string> >("txt") 
                 << endl;
        }
    }
    catch(std::exception& e)
    {
        cout << e.what() << endl;
    }

    return 0;
}
