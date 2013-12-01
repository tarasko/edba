#include <edba/conn_info.hpp>

#include <boost/test/unit_test.hpp>

using namespace edba;

BOOST_AUTO_TEST_CASE(ConnInfo)
{
    BOOST_CHECK_THROW(conn_info ci("user=system; password=1"), invalid_connection_string);

    conn_info ci("oracle:user=system; password=1;@use_prepared=off");
    BOOST_CHECK_EQUAL(ci.conn_string(), boost::as_literal("user=system; password=1; "));
    BOOST_CHECK_EQUAL(ci.driver_name(), boost::as_literal("oracle"));

    BOOST_CHECK(ci.has("user"));
    BOOST_CHECK(ci.has("@use_prepared"));
    BOOST_CHECK(!ci.has("foo"));

    BOOST_CHECK_EQUAL(ci.get("user"), boost::as_literal("system"));
    BOOST_CHECK_EQUAL(ci.get("user1", "test"), boost::as_literal("test"));
    BOOST_CHECK_EQUAL(ci.get("password", 3), 1);
    BOOST_CHECK_EQUAL(ci.get("password1", 2), 2);
}