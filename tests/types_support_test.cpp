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

#include <boost/fusion/container/vector.hpp>
#include <boost/fusion/sequence/comparison.hpp>
#include <boost/foreach.hpp>
#include <boost/tuple/tuple_io.hpp>
#include <boost/date_time/posix_time/posix_time_io.hpp>

#include <boost/test/minimal.hpp>

using namespace edba;
using namespace std;

namespace std {

template<typename T>
ostream& operator<<(ostream& os, const boost::optional<T>& v)
{
    if (v) 
        os << *v;
    else 
        os << "NULL";

    return os;
}

}

void print_test_table(session sess)
{
    typedef boost::tuple<int, boost::posix_time::ptime, boost::optional<std::string>> tuple_type;
    
    rowset<tuple_type> rs = sess.once() << "select id, dt, txt from test"; 

    BOOST_FOREACH(auto& t, rs)
        cout << t << endl;
}

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

    // 8. Test boost::fusion::vector
    boost::fusion::vector<int, boost::gregorian::date, std::string> case8(8, boost::gregorian::date(2011, 1, 1), "aaa");
    st << case8 << exec << reset;

    // Assert
    {
        rowset<> rs = sess.once() << "select dt, txt from test"; 

        // Test fetch_conversion

        // 1. Test std::shared_ptr and boost::gregorian::date
        auto iter = rs.begin();
        shared_ptr<boost::gregorian::date> case1_dt_res;
        shared_ptr<string> case1_str_res;
        *iter >> case1_dt_res >> case1_str_res;
        BOOST_CHECK(*case1_dt == *case1_dt_res);
        BOOST_CHECK(!case1_str_res);

        // 2. Test boost::shared_ptr and boost::posix_time::ptime
        boost::shared_ptr<boost::posix_time::ptime> case2_dt_res;
        boost::shared_ptr<string> case2_str_res;
        *++iter >> case2_dt_res >> case2_str_res;
        BOOST_CHECK(*case2_dt_res == *case2_dt);
        BOOST_CHECK(!case2_str);

        // 3. Test std::tuple
        tuple<boost::gregorian::date, std::shared_ptr<string>> case3_res;
        *++iter >> case3_res;
        BOOST_CHECK(get<0>(case3_res) == *get<1>(case3));
        BOOST_CHECK(!get<1>(case3_res));

        // 4. Test boost::tuple
        boost::tuple<boost::gregorian::date, std::shared_ptr<string>> case4_res;
        *++iter >> case4_res;
        BOOST_CHECK(get<0>(case4_res) == *get<1>(case4));
        BOOST_CHECK(!get<1>(case4_res));

        // 5. Test std::unique_ptr
        unique_ptr<boost::gregorian::date> case5_dt_res;
        unique_ptr<string> case5_str_res;
        *++iter >> case5_dt_res >> case5_str_res;
        BOOST_CHECK(*case5_dt_res == *case5_dt);
        BOOST_CHECK(!case5_str_res);

        // 6. Test boost::scoped_ptr
        boost::scoped_ptr<boost::gregorian::date> case6_dt_res;
        boost::scoped_ptr<string> case6_str_res;
        *++iter >> case6_dt_res >> case6_str_res;
        BOOST_CHECK(*case6_dt_res == *case6_dt);
        BOOST_CHECK(!case6_str_res);

        // 7. Test boost::optional
        boost::optional<boost::gregorian::date> case7_dt_res;
        boost::optional<string> case7_str_res;
        *++iter >> case7_dt_res >> case7_str_res;
        BOOST_CHECK(*case7_dt_res == *case7_dt);
        BOOST_CHECK(!case7_str_res);

        // 8. Test boost::fusion::vector
        boost::fusion::vector<boost::gregorian::date, std::string> case8_res;
        *++iter >> case8_res;

        namespace fus = boost::fusion::result_of ;
        typedef fus::next< fus::begin<decltype(case8)>::type >::type A;
        typedef fus::end<decltype(case8)>::type B;

        A a(case8);
        B b(case8);

        boost::fusion::iterator_range<A, B> rng(a, b);

        BOOST_CHECK(rng == case8_res);
    }

    // Dump data
    print_test_table(sess);


    return 0;
}
