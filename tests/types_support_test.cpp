#include <edba/types_support/boost_fusion.hpp>
#include <edba/types_support/boost_gregorian_date.hpp>
#include <edba/types_support/boost_optional.hpp>
#include <edba/types_support/boost_posix_time_ptime.hpp>
#include <edba/types_support/boost_scoped_ptr.hpp>
#include <edba/types_support/boost_shared_ptr.hpp>
#include <edba/types_support/boost_tuple.hpp>
#include <edba/types_support/std_shared_ptr.hpp>
#include <edba/types_support/std_tuple.hpp>
#include <edba/types_support/std_unique_ptr.hpp>

#include <edba/edba.hpp>

#include <boost/test/minimal.hpp>

using namespace edba;
using namespace std;

int test_main(int, char* [])
{
    session sess(driver::sqlite3(), "db=test.db");
    sess.once() << "create temp table test(id integer, dt datetime, txt text)" << exec;

    // Test bind conversion

    statement st = sess << "insert into test(id, dt, txt) values(:id, :dt, :txt)";

    // 1. Test std::shared_ptr and boost::gregorian::date
    auto case1_dt = make_shared<boost::gregorian::date>(2011, 1, 1);
    shared_ptr<string> case1_str;
    st << 1 << case1_dt << case1_str << exec << reset;

    // 2. Test boost::shared_ptr and boost::posix_time::ptime
    auto case2_dt = boost::make_shared<boost::posix_time::ptime>(*case1_dt, boost::posix_time::hours(2));
    boost::shared_ptr<string> case2_str;
    st << 2 << case2_dt << case2_str << exec << reset;

    // 3. Test std::tuple
    auto case3 = std::make_tuple(3, case1_dt, case1_str);
    st << case3 << exec << reset;

    // 4. Test boost::tuple
    auto case4 = boost::make_tuple(4, case1_dt, case1_str);
    st << case4 << exec << reset;

    // 5. Test std::unique_ptr
    unique_ptr<boost::gregorian::date> case5_dt(new boost::gregorian::date(2011, 1, 1));
    unique_ptr<string> case5_str;
    st << 5 << case5_dt << case5_str << exec << reset;

    // 6. Test boost::scoped_ptr
    boost::scoped_ptr<boost::gregorian::date> case6_dt(new boost::gregorian::date(2011, 1, 1));
    boost::scoped_ptr<string> case6_str;
    st << 6 << case6_dt << case6_str << exec << reset;

    // 7. Test boost::optional
    boost::optional<boost::gregorian::date> case7_dt = boost::gregorian::date(2011, 1, 1);
    boost::optional<string> case7_str;
    st << 7 << case7_dt << case7_str << exec << reset;

    return 0;
}
