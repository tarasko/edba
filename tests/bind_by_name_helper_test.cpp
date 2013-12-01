#include <edba/detail/bind_by_name_helper.hpp>

#include <boost/test/unit_test.hpp>

using namespace edba;

BOOST_AUTO_TEST_CASE(BindByNameHelper)
{
    detail::bind_by_name_helper questions("zzz :p1,:2p,:p1 zzz", detail::question_marker());
    detail::bind_by_name_helper pgstyle("zzz :p1,:2p,:p1 zzz", detail::postgresql_style_marker());

    BOOST_CHECK_EQUAL(questions.bindings_count(), 3);
    BOOST_CHECK_EQUAL(questions.patched_query(), boost::as_literal("zzz ?,?,? zzz"));
    BOOST_CHECK_EQUAL(pgstyle.patched_query(), boost::as_literal("zzz $1,$2,$3 zzz"));

    std::vector<int> qp1 = questions.name_to_idx("p1");
    std::vector<int> pp1 = pgstyle.name_to_idx("p1");

    std::vector<int> expected;
    expected.push_back(1);
    expected.push_back(3);

    BOOST_CHECK_EQUAL_COLLECTIONS(qp1.begin(), qp1.end(), expected.begin(), expected.end()); 
    BOOST_CHECK_EQUAL_COLLECTIONS(pp1.begin(), pp1.end(), expected.begin(), expected.end()); 
}
