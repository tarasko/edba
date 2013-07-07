#include "monitor.hpp"

#include <edba/edba.hpp>

#include <boost/iostreams/device/mapped_file.hpp>
#include <boost/foreach.hpp>
#include <boost/timer.hpp>

#include <iostream>
#include <ctime>

using namespace std;

int main()
{
    locale::global(locale(""));

    try {
        edba::session_pool pool(edba::driver::odbc(), "DSN=EDBA", 4);
        edba::session sess = pool.open();

        edba:: rowset<> rs = sess << "select * from invalid_table_name";
    }
    catch(std::exception& e)
    {
        cout << e.what() << endl;
    }

    return 0;
}
