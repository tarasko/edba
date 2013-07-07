#include "monitor.hpp"

#include <edba/edba.hpp>

#include <boost/format.hpp>
#include <boost/iostreams/device/mapped_file.hpp>
#include <boost/foreach.hpp>
#include <boost/timer.hpp>
#include <boost/scope_exit.hpp>

#define BOOST_TEST_MAIN
#include <boost/test/unit_test.hpp>

#include <iostream>
#include <ctime>

using namespace std;
using namespace edba;

// Set global locale to system default
struct set_locale_to_system_default_
{
    set_locale_to_system_default_() 
    {
        locale::global(locale(""));
    }
} set_locale_to_system_default;

void test_escaping(session sess)
{
    try 
    {
        try { sess.once() << "~Oracle~drop table test_escaping~" << exec; } catch(...) {}

        sess.once() <<
            "~Microsoft SQL Server~create table ##test_escaping(txt nvarchar(100))"
            "~Sqlite3~create temp table test_escaping(txt nvarchar(100)) "
            "~Mysql~create temporary table test_escaping(txt nvarchar(100))"
            "~PgSQL~create temp table test_escaping(txt nvarchar(100))"
            "~Oracle~create table test_escaping( txt nvarchar2(100) )"
            "~"
            << exec;

        string bad_string = "\\''\\' insert into char'";
        string good_string = sess.escape(bad_string);

        string insert_query = boost::str(boost::format(
            "~Microsoft SQL Server~insert into ##test_escaping(txt) values('%1%')"
            "~~insert into test_escaping(txt) values('%1%')"
            "~") % good_string);

        sess.once() << insert_query << exec;

        string result;
        sess.once() << 
            "~Microsoft SQL Server~select txt from ##test_escaping"
            "~~select txt from test_escaping"
            "~"
            << first_row >> result;

        assert(result == bad_string);
    }
    catch(edba::not_supported_by_backend&)
    {
    }
}

template<typename Driver>
void test(const char* conn_string)
{
    const std::locale& loc = std::locale::classic();

    // Workaround for postgres lobs
    conn_info ci(conn_string);
    const char* postgres_lob_type;
    if (ci.has("@blob") && boost::iequals(ci.get("@blob"), "bytea", loc))
        postgres_lob_type = "bytea";
    else
        postgres_lob_type = "oid";

    std::string oracle_cleanup_seq = "~Oracle~drop sequence test1_seq_id~;";
    std::string oracle_cleanup_tbl = "~Oracle~drop table test1~;";

    std::string create_test1_table = boost::str(boost::format(
        "~Oracle~create sequence test1_seq_id~;"
        "~Microsoft SQL Server~create table ##test1( "
        "   id int identity(1, 1) primary key clustered, "
        "   num numeric(18, 3), "
        "   dt datetime, "
        "   dt_small smalldatetime, "
        "   vchar20 nvarchar(20), "
        "   vcharmax nvarchar(max), "
        "   vbin20 varbinary(20), "
        "   vbinmax varbinary(max), "
        "   txt text, "
        "   national nvarchar(100) "
        "   ) "
        "~Sqlite3~create temp table test1( "
        "   id integer primary key autoincrement, "
        "   num double, "
        "   dt text, "
        "   dt_small text, "
        "   vchar20 varchar(20), "
        "   vcharmax text, "
        "   vbin20 blob, "
        "   vbinmax blob, "
        "   txt text, "
        "   national nvarchar(100) "
        "   ) "
        "~Mysql~create temporary table test1( "
        "   id integer AUTO_INCREMENT PRIMARY KEY, "
        "   num numeric(18, 3), "
        "   dt timestamp, "
        "   dt_small date, "    
        "   vchar20 varchar(20), "
        "   vcharmax text, "
        "   vbin20 varbinary(20), "
        "   vbinmax blob, "
        "   txt text, "
        "   national nvarchar(100) "
        "   ) "
        "~PgSQL~create temp table test1( "
        "   id serial primary key, "
        "   num numeric(18, 3), "
        "   dt timestamp, "
        "   dt_small date, "
        "   vchar20 varchar(20), "
        "   vcharmax varchar(15000), "
        "   vbin20 %1%, "
        "   vbinmax %1%, "
        "   txt text, "
        "   national nvarchar(100) "
        "   ) "
        "~Oracle~create table test1( "
        "   id number primary key, "
        "   num number(18, 3), "
        "   dt timestamp, "
        "   dt_small date, "
        "   vchar20 varchar2(20),  "
        "   vcharmax varchar2(4000), " 
        "   vbin20 raw(20), "
        "   vbinmax blob, "
        "   txt clob, "
        "   national nvarchar2(100) "
        "   ) "
        "~") % postgres_lob_type);

    const char* insert_test1_data =
        "~Microsoft SQL Server~insert into ##test1(num, dt, dt_small, vchar20, vcharmax, vbin20, vbinmax, txt) "
        "   values(:num, :dt, :dt_small, :vchar20, :vcharmax, :vbin20, :vbinmax, :txt)"
        "~Oracle~insert into test1(id, num, dt, dt_small, vchar20, vcharmax, vbin20, vbinmax, txt)"
        "   values(test1_seq_id.nextval, :num, :dt, :dt_small, :vchar20, :vcharmax, :vbin20, :vbinmax, :txt)"
        "~~insert into test1(num, dt, dt_small, vchar20, vcharmax, vbin20, vbinmax, txt)"
        "   values(:num, :dt, :dt_small, :vchar20, :vcharmax, :vbin20, :vbinmax, :txt)"
        "~";

    const char* select_test1_row = 
        "~Microsoft SQL Server~select id, num, dt, dt_small, vchar20, vcharmax, vbin20, vbinmax, txt from ##test1 where id=:id"
        "~~select id, num, dt, dt_small, vchar20, vcharmax, vbin20, vbinmax, txt from test1 where id=:id"
        "~";

    const char* drop_test1 =
        "~Oracle~drop sequence test1_seq_id~;"
        "~Oracle~drop table test1~;"
        "~Microsoft SQL Server~drop table ##test1"
        "~~drop table test1"
        "~";

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

        monitor sm;
        session sess(Driver(), conn_string, &sm);

        // Test empty query
        sess << "" << exec;
        sess << "~~" << exec;

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
            st 
                << use("num", 10.10) 
                << use("dt", *std::gmtime(&now)) 
                << use("dt_small", *std::gmtime(&now)) 
                << use("vchar20", "Hello!")
                << use("vcharmax", "Hello! max")
                << use("vbin20", &short_binary_stream)
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
            assert(st == st1);
        }

        // Query single row
        {
            transaction tr(sess);

            row r = sess << select_test1_row << id << first_row;
    
            int id;
            double num;
            std::tm tm1, tm2;
            std::string short_str;
            std::string long_str;
            std::ostringstream short_oss;
            std::ostringstream long_oss;
            std::string txt;

            r >> id >> num >> tm1 >> tm2 >> short_str >> long_str >> short_oss >> long_oss >> txt;

            assert(num == 10.10);
            assert(!memcmp(std::gmtime(&now), &tm1, sizeof(tm1)));
            assert(short_str == "Hello!");
            assert(long_str == "Hello! max");
            assert(short_oss.str() == short_binary);
            assert(long_oss.str() == long_binary);
            assert(text == txt);

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
            "~Microsoft SQL Server~insert into ##test1(num) values(:num)"
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

            copy(rs.begin(), rs.end(), ostream_iterator<int>(cout, " "));
            cout << endl;
        }

        test_escaping(sess);

        sess.exec_batch(drop_test1);
    }
}

BOOST_AUTO_TEST_CASE(BackendSmoke)
{
    test<edba::driver::odbc>("Server=192.168.1.103\\SQLEXPRESS; Database=edba; User Id=sa;Password=1;@utf=wide");
    test<edba::driver::mysql>("host=192.168.1.103;database=edba;user=edba;password=1111;");
    test<edba::driver::oracle>("user=system; password=1; ConnectionString=192.168.1.103:1521/xe");
    test<edba::driver::sqlite3>("db=test.db");

    //test<edba::driver::odbc>("DSN=EDBA");
    //test<edba::driver::postgresql>("user=postgres; password=postgres; host=localhost; port=5433; dbname=test; @blob=bytea");
    //test<edba::driver::postgresql>("user=postgres; password=postgres; host=localhost; port=5433; dbname=test");
}
