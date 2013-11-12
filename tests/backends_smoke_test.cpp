#include "monitor.hpp"

#include <edba/edba.hpp>
#include <edba/types_support/boost_optional.hpp>

#include <boost/optional/optional_io.hpp>
#include <boost/format.hpp>
#include <boost/iostreams/device/mapped_file.hpp>
#include <boost/foreach.hpp>
#include <boost/timer.hpp>
#include <boost/scope_exit.hpp>
#include <boost/locale/encoding_utf.hpp>
#include <boost/locale/encoding.hpp>
#include <boost/locale/generator.hpp>

#define BOOST_TEST_MAIN
#include <boost/test/unit_test.hpp>

#include <iostream>
#include <ctime>

using namespace std;
using namespace edba;

#define SERVER_IP "edba-test"

const char* oracle_cleanup_seq = "~Oracle~drop sequence test1_seq_id~;";
const char* oracle_cleanup_tbl = "~Oracle~drop table test1~;";

const char* create_test1_table_tpl = 
    "~Oracle~create sequence test1_seq_id~;"
    "~Microsoft SQL Server~create table ##test1( "
    "   id int identity(1, 1) primary key clustered, "
    "   num numeric(18, 3), "
    "   dt datetime, "
    "   dt_small smalldatetime, "
    "   nvchar100 nvarchar(100), "
    "   vcharmax varchar(max), "
    "   vbin100 varbinary(100), "
    "   vbinmax varbinary(max), "
    "   txt text, "
    "   ntxt ntext"
    "   ) "
    "~Sqlite3~create temp table test1( "
    "   id integer primary key autoincrement, "
    "   num double, "
    "   dt text, "
    "   dt_small text, "
    "   nvchar100 nvarchar(100), "
    "   vcharmax text, "
    "   vbin100 blob, "
    "   vbinmax blob, "
    "   txt text, "
    "   ntxt ntext"
    "   ) "
    "~Mysql~create temporary table test1( "
    "   id integer AUTO_INCREMENT PRIMARY KEY, "
    "   num numeric(18, 3), "
    "   dt timestamp, "
    "   dt_small date, "    
    "   nvchar100 nvarchar(100), "
    "   vcharmax text, "
    "   vbin100 varbinary(100), "
    "   vbinmax blob, "
    "   txt text, "
    "   ntxt text "
    "   ) "
    "~PgSQL~create temp table test1( "
    "   id serial primary key, "
    "   num numeric(18, 3), "
    "   dt timestamp, "
    "   dt_small date, "
    "   nvchar100 varchar(100), "          // postgres don`t have nvarchar
    "   vcharmax varchar(15000), "
    "   vbin100 %1%, "
    "   vbinmax %1%, "
    "   txt text, "
    "   ntxt text "
    "   ) "
    "~Oracle~create table test1( "
    "   id number primary key, "
    "   num number(18, 3), "
    "   dt timestamp, "
    "   dt_small date, "
    "   nvchar100 nvarchar2(100),  "
    "   vcharmax varchar2(4000), " 
    "   vbin100 raw(100), "
    "   vbinmax blob, "
    "   txt clob, "
    "   ntxt clob "
    "   ) "
    "~";

const char* insert_test1_data =
    "~Microsoft SQL Server~insert into ##test1(num, dt, dt_small, nvchar100, vcharmax, vbin100, vbinmax, txt) "
    "   values(:num, :dt, :dt_small, :nvchar100, :vcharmax, :vbin100, :vbinmax, :txt)"
    "~Oracle~insert into test1(id, num, dt, dt_small, nvchar100, vcharmax, vbin100, vbinmax, txt)"
    "   values(test1_seq_id.nextval, :num, :dt, :dt_small, :nvchar100, :vcharmax, :vbin100, :vbinmax, :txt)"
    "~~insert into test1(num, dt, dt_small, nvchar100, vcharmax, vbin100, vbinmax, txt)"
    "   values(:num, :dt, :dt_small, :nvchar100, :vcharmax, :vbin100, :vbinmax, :txt)"
    "~";

const char* select_test1_row_where_id = 
    "~Microsoft SQL Server~select * from ##test1 where id=:id"
    "~~select * from test1 where id=:id"
    "~";

const char* drop_test1 =
    "~Oracle~drop sequence test1_seq_id"
    "~Oracle~drop table test1"
    "~Microsoft SQL Server~drop table ##test1"
    "~~drop table test1"
    "~";

const char* create_test_escaping = 
    "~Microsoft SQL Server~create table ##test_escaping(txt nvarchar(100))"
    "~Sqlite3~create temp table test_escaping(txt nvarchar(100)) "
    "~Mysql~create temporary table test_escaping(txt nvarchar(100))"
    "~PgSQL~create temp table test_escaping(txt varchar(100))"
    "~Oracle~create table test_escaping( txt nvarchar2(100) )"
    "~";

const char* insert_into_test_escaping_tpl = 
    "~Microsoft SQL Server~insert into ##test_escaping(txt) values('%1%')"
    "~~insert into test_escaping(txt) values('%1%')"
    "~";

const char* select_from_test_escaping = 
    "~Microsoft SQL Server~select txt from ##test_escaping"
    "~~select txt from test_escaping"
    "~";

void test_escaping(session sess)
{
    try 
    {
        try { sess.once() << "~Oracle~drop table test_escaping~" << exec; } catch(...) {}

        sess.once() << create_test_escaping << exec;

        string bad_string = "\\''\\' insert into char'";
        string good_string = sess.escape(bad_string);

        string insert_query = boost::str(boost::format(insert_into_test_escaping_tpl) % good_string);

        sess.once() << insert_query << exec;

        string result;
        sess.once() << select_from_test_escaping << first_row >> result;

        BOOST_CHECK_EQUAL(result, bad_string);
    }
    catch(edba::not_supported_by_backend&)
    {
        // This is ok if backend doesn`t support escaping
    }
}

void test_utf8(session sess)
{
    wstring utf16_short = L"Привет Мир ( Hello world )";
    string utf8_short = boost::locale::conv::utf_to_utf<char>(utf16_short);

    wstring utf16_long(L'Т', 20000);
    string utf8_long = boost::locale::conv::utf_to_utf<char>(utf16_long);

    vector<long long> ids_to_check;

    {
        statement st = sess << 
            "~Microsoft SQL Server~insert into ##test1(nvchar100, ntxt) values(:short, :long)"
            "~Oracle~insert into test1(id, nvchar100, ntxt) values(test1_seq_id.nextval, :short, :long)"
            "~~insert into test1(nvchar100, ntxt) values(:short, :long)"
            << utf8_short 
            << utf8_long
            << exec;
    
        ids_to_check.push_back(sess.backend() == "oracle" ? st.sequence_last("test1_seq_id") : st.last_insert_id());
    }

    {
        istringstream oss_utf8_short(utf8_short);
        istringstream oss_utf8_long(utf8_long);

        statement st = sess << 
            "~Microsoft SQL Server~insert into ##test1(nvchar100, ntxt) values(:short, :long)"
            "~Oracle~insert into test1(id, nvchar100, ntxt) values(test1_seq_id.nextval, :short, :long)"
            "~~insert into test1(nvchar100, ntxt) values(:short, :long)"
            << &oss_utf8_short 
            << &oss_utf8_long
            << exec;

        ids_to_check.push_back(sess.backend() == "oracle" ? st.sequence_last("test1_seq_id") : st.last_insert_id());
    }

    BOOST_FOREACH(long long id, ids_to_check)
    {
        string vc;
        string txt;
        sess << select_test1_row_where_id << id << first_row >> into("nvchar100", vc) >> into("ntxt", txt);

        BOOST_CHECK_EQUAL(utf8_short, vc);
        BOOST_CHECK_EQUAL(utf8_long, txt);

        ostringstream vc_ss;
        ostringstream txt_ss;
        sess << select_test1_row_where_id << id << first_row >> into("nvchar100", vc_ss) >> into("ntxt", txt_ss);

        BOOST_CHECK_EQUAL(utf8_short, vc_ss.str());
        BOOST_CHECK_EQUAL(utf8_long, txt_ss.str());
    }
}

void test_transactions_and_cursors(session sess)
{
    const char* INSERT_QUERY = 
        "~Microsoft SQL Server~insert into ##test1(nvchar100, txt) values(:txt, :txt)"
        "~Oracle~insert into test1(id, nvchar100, txt) values(test1_seq_id.nextval, :txt, :txt)"
        "~~insert into test1(nvchar100, txt) values(:txt, :txt)";

    const char* SELECT_QUERY = 
        "~Microsoft SQL Server~select id, num, dt, dt_small, nvchar100, vcharmax, vbin100, vbinmax, txt from ##test1 where id=:id"
        "~~select id, num, dt, dt_small, nvchar100, vcharmax, vbin100, vbinmax, txt from test1 where id=:id"
        "~";

    // Create and execute prepared statement inside transaction, commit transaction
    statement st1;
    {
        transaction tr(sess);
        st1 = sess << INSERT_QUERY << use("txt", "1") << exec;
        tr.commit();
    }

    // Execute query rollback transaction
    {
        transaction tr(sess);
        sess << INSERT_QUERY << use("txt", "1") << exec;
    }

    // Reexecute statement outside of transaction
    st1 << reset << use("txt", "2") << exec;

    // Extract ref to statement from cache and execute it
    statement st2 = sess << INSERT_QUERY << use("txt", "3") << exec;

    // Get some valid id for select query
    long long id = sess.backend() == "oracle" ? st2.sequence_last("test1_seq_id") : id = st2.last_insert_id();

    string vc;
    string txt;
    // commit query
    // use row object to make cursor formally life after tr.commit
    // odbc drivers are tend to behave wierdly in that case
    {
        transaction tr(sess);
        row r = sess << SELECT_QUERY << id << first_row >> into("nvchar100", vc) >> into("txt", txt);
        tr.commit();
    }

    BOOST_CHECK_EQUAL(vc, "3");
    BOOST_CHECK_EQUAL(txt, "3");

    // rollback query
    {
        transaction tr(sess);
        sess << SELECT_QUERY << id << first_row >> into("nvchar100", vc) >> into("txt", txt);
    }

    BOOST_CHECK_EQUAL(vc, "3");
    BOOST_CHECK_EQUAL(txt, "3");

    // auto commit mode
    sess << SELECT_QUERY << id << first_row >> into("nvchar100", vc) >> into("txt", txt);

    BOOST_CHECK_EQUAL(vc, "3");
    BOOST_CHECK_EQUAL(txt, "3");
}

void test_incorrect_query(session sess)
{
    // Some backends may successfully compile incorrect statements
    // However they all must fail on execution step

    BOOST_CHECK_THROW(sess.exec_batch("incorrect statement"), edba_error);
    BOOST_CHECK_THROW(sess.exec_batch("incorrect statement;"), edba_error);
    BOOST_CHECK_THROW(sess << "incorrect statement" << exec, edba_error);
    BOOST_CHECK_THROW(sess.once() << "incorrect statement" << exec, edba_error);
}

void test_empty_query(session sess)
{
    // Test empty query, should not throw
    sess << "" << exec;
    sess << "~~" << exec;
}

template<typename Driver>
void test(const char* conn_string)
{
    // Workaround for postgres lobs
    conn_info ci(conn_string);
    const char* postgres_lob_type;
    if (ci.has("@blob") && boost::iequals(ci.get("@blob"), "bytea"))
        postgres_lob_type = "bytea";
    else
        postgres_lob_type = "oid";

    std::string oracle_cleanup_seq = "~Oracle~drop sequence test1_seq_id~;";
    std::string oracle_cleanup_tbl = "~Oracle~drop table test1~;";

    std::string create_test1_table = boost::str(boost::format(create_test1_table_tpl) % postgres_lob_type);

    std::string short_binary("binary");
    std::string long_binary(10000, 't');

    std::istringstream long_binary_stream;
    std::istringstream short_binary_stream;

    short_binary_stream.str(short_binary);
    long_binary_stream.str(long_binary);

    std::string text(10000, 'z');

    std::time_t now = std::time(0);

    {
        using namespace edba;

        BOOST_TEST_MESSAGE("Run backend test for " << conn_string);

        monitor sm;
        session sess(Driver(), conn_string, 0);

        test_incorrect_query(sess);
        test_empty_query(sess);

        // Cleanup previous oracle testing
        // Oracle don`t have temporary tables
        try {sess.exec_batch(oracle_cleanup_seq);} catch(...) {}
        try {sess.exec_batch(oracle_cleanup_tbl);} catch(...) {}

        // Create table
        sess.exec_batch(create_test1_table);

        // Transaction for inserting data. Postgresql backend require explicit transaction if you want to bind blobs.
        long long id;
        {
            transaction tr(sess);

            // Compile statement for inserting data 
            statement st = sess << insert_test1_data;

            // Exec when part of parameters are nulls
            st << reset
                << 10.10 
                << null
                << *std::gmtime(&now)
                << null
                << null
                << null
                << null
                << null
                << exec;

            // Bind data to statement and execute two times
            st << reset
                << use("num", 10.10) 
                << use("dt", *std::gmtime(&now)) 
                << use("dt_small", *std::gmtime(&now)) 
                << use("nvchar100", "Hello!")
                << use("vcharmax", "Hello! max")
                << use("vbin100", &short_binary_stream)
                << use("vbinmax", &long_binary_stream)
                << use("txt", text)
                << exec
                << exec;

            if (sess.backend() == "oracle")
                id = st.sequence_last("test1_seq_id");
            else
                id = st.last_insert_id();

            // Exec with all null types
            st << reset
               << null 
               << null
               << null
               << null
               << null
               << null
               << null
               << null
               << exec;

            tr.commit();

            // Check that cache works as expected
            statement st1 = sess << insert_test1_data;
            BOOST_CHECK(st == st1);
        }

        // Query single row
        {
            transaction tr(sess);

            row r = sess << select_test1_row_where_id << id << first_row;
    
            int id;
            double num;
            std::tm tm1, tm2;
            std::string short_str;
            std::string long_str;
            std::ostringstream short_oss;
            std::ostringstream long_oss;
            std::string txt;

            r >> id >> num >> tm1 >> tm2 >> short_str >> long_str >> short_oss >> long_oss >> txt;

            BOOST_CHECK_EQUAL(num, 10.10);
            BOOST_CHECK(!memcmp(std::gmtime(&now), &tm1, sizeof(tm1)));
            BOOST_CHECK_EQUAL(short_str, "Hello!");
            BOOST_CHECK_EQUAL(long_str, "Hello! max");
            BOOST_CHECK_EQUAL(short_oss.str(), short_binary);
            BOOST_CHECK_EQUAL(long_oss.str(), long_binary);
            BOOST_CHECK_EQUAL(text, txt);

            tr.commit();
        }

        sess.exec_batch(
            "~Microsoft SQL Server~insert into ##test1(num) values(10.2)"
            "~Oracle~insert into test1(id, num) values(test1_seq_id.nextval, 10.2)"
            "~~insert into test1(num) values(10.2)"
            "~;"
            "~Microsoft SQL Server~insert into ##test1(num) values(10.3)"
            "~Oracle~insert into test1(id, num) values(test1_seq_id.nextval, 10.3)"
            "~~insert into test1(num) values(10.3)"
            "~");

        // Try to bind non prepared statement
        // Test once helper 
        sess.once() << 
            "~Microsoft SQL Server~insert into ##test1(num) values(:num)"
            "~Oracle~insert into test1(id, num) values(test1_seq_id.nextval, :num)"
            "~~insert into test1(num) values(:num)"
            "~"
            << use("num", 10.5) 
            << exec;

        // Exec when part of parameters are nulls
        sess.once() << 
            "~Microsoft SQL Server~insert into ##test1(num, dt, dt_small) values(:num, :dt, :dt_small)"
            "~Oracle~insert into test1(id, num, dt, dt_small) values(test1_seq_id.nextval, :num, :dt, :dt_small)"
            "~~insert into test1(num, dt, dt_small) values(:num, :dt, :dt_small)"
            "~"
            << 10.5
            << *std::gmtime(&now)
            << null
            << exec;

        {
            // Regression case: rowset doesn`t support non class types
            rowset<int> rs = sess << 
                "~Microsoft SQL Server~select id from ##test1"
                "~~select id from test1"
                "~";

            BOOST_CHECK_EQUAL(boost::distance(rs), 8);
        }

        test_transactions_and_cursors(sess);
        test_escaping(sess);
        test_utf8(sess);

        sess.exec_batch(drop_test1);
    }
}

BOOST_AUTO_TEST_CASE(Oracle)
{
    test<edba::driver::oracle>("user=system; password=1; ConnectionString=" SERVER_IP ":1521/xe");
}

BOOST_AUTO_TEST_CASE(ODBCWide)
{
    test<edba::driver::odbc>("Driver={SQL Server Native Client 10.0}; Server=" SERVER_IP "\\SQLEXPRESS; Database=EDBA; UID=sa;PWD=1;@utf=wide");
}

BOOST_AUTO_TEST_CASE(ODBC)
{
    test<edba::driver::odbc>("Driver={SQL Server Native Client 10.0}; Server=" SERVER_IP "\\SQLEXPRESS; Database=EDBA; UID=sa;PWD=1;");
}

BOOST_AUTO_TEST_CASE(MySQL)
{
    test<edba::driver::mysql>("host=" SERVER_IP ";database=edba;user=edba;password=1111;");
}

BOOST_AUTO_TEST_CASE(SQLite3)
{
    test<edba::driver::sqlite3>("db=test.db");
}

BOOST_AUTO_TEST_CASE(Postgres)
{
    test<edba::driver::postgresql>("user=postgres; password=1; host=" SERVER_IP "; port=5432; dbname=test");
}

BOOST_AUTO_TEST_CASE(PostgresBytea)
{
    test<edba::driver::postgresql>("user=postgres; password=1; host=" SERVER_IP "; port=5432; dbname=test; @blob=bytea");
}

