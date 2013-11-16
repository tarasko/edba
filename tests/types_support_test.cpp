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
#include <boost/typeof/typeof.hpp>

#include <boost/test/unit_test.hpp>

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
    typedef boost::tuple< int, boost::posix_time::ptime, boost::optional<std::string> > tuple_type;

    rowset<tuple_type> rs = sess.once() << "select id, dt, txt from test";

    BOOST_FOREACH(const tuple_type& t, rs)
        BOOST_TEST_MESSAGE(t);
}

struct types_support_fixture
{
    types_support_fixture()
      : sess(driver::sqlite3(), "db=test.db")
    {
        sess.once() << "create temp table test(id integer, dt datetime, txt text)" << exec;
        st = sess << "insert into test(id, dt, txt) values(:id, :dt, :txt)";
        select_st = sess << "select dt, txt from test where id = :id";
    }

    ~types_support_fixture()
    {
        // Dump data
        print_test_table(sess);
    }

    session sess;
    statement st;
    statement select_st;
};

BOOST_FIXTURE_TEST_CASE(BoostSharedPtrAndPtime, types_support_fixture)
{
    // 6. Test boost::scoped_ptr
    boost::scoped_ptr<boost::gregorian::date> case6_dt(new boost::gregorian::date(2011, 1, 1));
    boost::scoped_ptr<string> case6_str;
    st << 6 << case6_dt << case6_str << exec << reset;

    // 6. Test boost::scoped_ptr
    boost::scoped_ptr<boost::gregorian::date> case6_dt_res;
    boost::scoped_ptr<string> case6_str_res;
    select_st << 6 << first_row >> case6_dt_res >> case6_str_res;
    BOOST_CHECK(*case6_dt_res == *case6_dt);
    BOOST_CHECK(!case6_str_res);
}

BOOST_FIXTURE_TEST_CASE(BoostSharedPtrAndGregDate, types_support_fixture)
{
    BOOST_AUTO(case1_dt, (boost::make_shared<boost::gregorian::date>(2011, 1, 1)));
    BOOST_AUTO(case2_dt, (boost::make_shared<boost::posix_time::ptime>(*case1_dt, boost::posix_time::hours(2))));
    boost::shared_ptr<string> case2_str;
    st << 2 << case2_dt << case2_str << exec << reset;

    boost::shared_ptr<boost::posix_time::ptime> case2_dt_res;
    boost::shared_ptr<string> case2_str_res;
    select_st << 2 << first_row >> case2_dt_res >> case2_str_res;
    BOOST_CHECK(*case2_dt_res == *case2_dt);
    BOOST_CHECK(!case2_str);
}

BOOST_FIXTURE_TEST_CASE(BoostOptional, types_support_fixture)
{
    // 7. Test boost::optional
    boost::optional<boost::gregorian::date> case7_dt = boost::gregorian::date(2011, 1, 1);
    boost::optional<string> case7_str;
    st << 7 << case7_dt << case7_str << exec << reset;

    // 7. Test boost::optional
    boost::optional<boost::gregorian::date> case7_dt_res;
    boost::optional<string> case7_str_res;
    select_st << 7 << first_row >> case7_dt_res >> case7_str_res;
    BOOST_CHECK(*case7_dt_res == *case7_dt);
    BOOST_CHECK(!case7_str_res);
}

BOOST_FIXTURE_TEST_CASE(BoostTuple, types_support_fixture)
{
    boost::shared_ptr<string> case1_str;

    BOOST_AUTO(case1_dt, (boost::make_shared<boost::gregorian::date>(2011, 1, 1)));
    // 4. Test boost::tuple
    BOOST_AUTO(case4, (boost::make_tuple(4, case1_dt, case1_str)));
    st << case4 << exec << reset;

    // 4. Test boost::tuple
    boost::tuple< boost::gregorian::date, boost::shared_ptr<string> > case4_res;
    select_st << 4 << first_row >> case4_res;
    BOOST_CHECK(boost::get<0>(case4_res) == *boost::get<1>(case4));
    BOOST_CHECK(!boost::get<1>(case4_res));
}

BOOST_FIXTURE_TEST_CASE(BoostFusionVector, types_support_fixture)
{
    // 8. Test boost::fusion::vector
    boost::fusion::vector<int, boost::gregorian::date, std::string> case8(8, boost::gregorian::date(2011, 1, 1), "aaa");
    st << case8 << exec << reset;

    // 8. Test boost::fusion::vector
    boost::fusion::vector<boost::gregorian::date, std::string> case8_res;
    select_st << 8 << first_row >> case8_res;

    namespace fus = boost::fusion::result_of ;
    typedef fus::next< fus::begin<BOOST_TYPEOF(case8)>::type >::type A;
    typedef fus::end<BOOST_TYPEOF(case8)>::type B;

    A a(case8);
    B b(case8);

    boost::fusion::iterator_range<A, B> rng(a, b);

    BOOST_CHECK(rng == case8_res);
}

BOOST_FIXTURE_TEST_CASE(MutableRowset, types_support_fixture)
{
    // Test mutable rowset
    typedef boost::tuple< boost::gregorian::date, boost::shared_ptr<string> > data_type;

    rowset<data_type> rs = sess.once() << "select dt, txt from test";
    std::vector<data_type> v(rs.begin(), rs.end());
}

BOOST_FIXTURE_TEST_CASE(ConstRowset, types_support_fixture)
{
    typedef boost::tuple< boost::gregorian::date, boost::shared_ptr<string> > data_type;

    const rowset<data_type> rs = sess.once() << "select dt, txt from test";
    std::vector<data_type> v(rs.begin(), rs.end());
}

#ifndef BOOST_NO_CXX11_SMART_PTR

BOOST_FIXTURE_TEST_CASE(StdSharedPtr, types_support_fixture)
{
    // 1. Test std::shared_ptr and boost::gregorian::date
    BOOST_AUTO(case1_dt, (std::make_shared<boost::gregorian::date>(2011, 1, 1)));
    shared_ptr<string> case1_str;
    st << 1 << case1_dt << case1_str << exec << reset;

    // 1. Test std::shared_ptr and boost::gregorian::date
    shared_ptr<boost::gregorian::date> case1_dt_res;
    shared_ptr<string> case1_str_res;
    select_st << 1 << first_row >> case1_dt_res >> case1_str_res;
    BOOST_CHECK(*case1_dt == *case1_dt_res);
    BOOST_CHECK(!case1_str_res);
}

BOOST_FIXTURE_TEST_CASE(StdUniquePtr, types_support_fixture)
{
    // 5. Test std::unique_ptr
    unique_ptr<boost::gregorian::date> case5_dt(new boost::gregorian::date(2011, 1, 1));
    unique_ptr<string> case5_str;
    st << 5 << case5_dt << case5_str << exec << reset;

    // 5. Test std::unique_ptr
    unique_ptr<boost::gregorian::date> case5_dt_res;
    unique_ptr<string> case5_str_res;
    select_st << 5 << first_row >> case5_dt_res >> case5_str_res;
    BOOST_CHECK(*case5_dt_res == *case5_dt);
    BOOST_CHECK(!case5_str_res);
}

#endif

#ifndef BOOST_NO_CXX11_HDR_TUPLE

BOOST_FIXTURE_TEST_CASE(StdTuple, types_support_fixture)
{
    shared_ptr<string> case1_str;
    BOOST_AUTO(case1_dt, (std::make_shared<boost::gregorian::date>(2011, 1, 1)));

    // 3. Test std::tuple
    auto case3 = std::make_tuple(3, case1_dt, case1_str);
    st << case3 << exec << reset;

    // 3. Test std::tuple
    tuple<boost::gregorian::date, std::shared_ptr<string>> case3_res;
    select_st << 3 << first_row >> case3_res;
    BOOST_CHECK(get<0>(case3_res) == *get<1>(case3));
    BOOST_CHECK(!get<1>(case3_res));

}

#endif
