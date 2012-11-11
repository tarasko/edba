#include <edba/backend/backend.hpp>
#include <edba/backend/bind_by_name_helper.hpp>
#include <edba/detail/utils.hpp>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/find_iterator.hpp>
#include <boost/locale/encoding_utf.hpp>
#include <boost/typeof/typeof.hpp>
#include <boost/static_assert.hpp>
#include <boost/make_shared.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/mpl/map.hpp>
#include <boost/mpl/int.hpp>
#include <boost/mpl/at.hpp>
#include <boost/type_traits/is_arithmetic.hpp>

#include <list>
#include <vector>
#include <sstream>
#include <limits>
#include <iomanip>

#include <string.h>

#if defined(_WIN32) || defined(__WIN32) || defined(WIN32) || defined(__CYGWIN__)
#include <windows.h>
#endif
#include <sqlext.h>

namespace mpl = boost::mpl;

namespace edba { 

namespace {

struct column_info
{
    std::string name_;  // name
    int index_;         // index
    SQLSMALLINT type_;  // type
};

}

string_ref to_string_ref(const column_info& ci)
{
    return string_ref(ci.name_);
}

namespace odbc_backend {

using boost::locale::conv::utf_to_utf;    
    
const std::string g_backend("odbc");

typedef unsigned odbc_u32;
typedef unsigned short odbc_u16;

BOOST_STATIC_ASSERT(sizeof(unsigned) == 4);
BOOST_STATIC_ASSERT(sizeof(unsigned short) == 2);
BOOST_STATIC_ASSERT(sizeof(SQLWCHAR) == 2);


void check_odbc_errorW(SQLRETURN error,SQLHANDLE h,SQLSMALLINT type)
{
    if(SQL_SUCCEEDED(error))
        return;
    std::basic_string<SQLWCHAR> error_message;
    int rec=1,r;
    for(;;){
        SQLWCHAR msg[SQL_MAX_MESSAGE_LENGTH + 2] = {0};
        SQLWCHAR stat[SQL_SQLSTATE_SIZE + 1] = {0};
        SQLINTEGER err;
        SQLSMALLINT len;
        r = SQLGetDiagRecW(type,h,rec,stat,&err,msg,sizeof(msg)/sizeof(SQLWCHAR),&len);
        rec++;
        if(r==SQL_SUCCESS || r==SQL_SUCCESS_WITH_INFO) {
            if(!error_message.empty()) {
                SQLWCHAR nl = '\n';
                error_message+=nl;
            }
            error_message.append(msg);
        }
        else 
            break;

    }
    std::string utf8_str = "Unconvertable string";
    try 
    { 
        std::string tmp = utf_to_utf<char>(error_message); 
        utf8_str = tmp;         
    } 
    catch(...){}
    
    throw edba_error("edba::odbc_backend::Failed with error `" + utf8_str +"'");
}

void check_odbc_errorA(SQLRETURN error,SQLHANDLE h,SQLSMALLINT type)
{
    if(SQL_SUCCEEDED(error))
        return;
    std::string error_message;
    int rec=1,r;
    for(;;){
        SQLCHAR msg[SQL_MAX_MESSAGE_LENGTH + 2] = {0};
        SQLCHAR stat[SQL_SQLSTATE_SIZE + 1] = {0};
        SQLINTEGER err;
        SQLSMALLINT len;
        r = SQLGetDiagRecA(type,h,rec,stat,&err,msg,sizeof(msg),&len);
        rec++;
        if(r==SQL_SUCCESS || r==SQL_SUCCESS_WITH_INFO) {
            if(!error_message.empty())
                error_message+='\n';
            error_message +=(char *)msg;
        }
        else 
            break;

    } 
    throw edba_error("edba::odbc::Failed with error `" + error_message +"'");
}

void check_odbc_error(SQLRETURN error,SQLHANDLE h,SQLSMALLINT type,bool wide)
{
    if(wide)
        check_odbc_errorW(error,h,type);
    else
        check_odbc_errorA(error,h,type);
}

class result : public backend::result, public boost::static_visitor<bool>
{
    static const SQLUINTEGER MAX_READ_BUFFER_SIZE = 4096;

    typedef std::vector<column_info> columns_set;

public:
    result(SQLHSTMT stmt, bool wide) : stmt_(stmt), wide_(wide) 
    {
        // Read number of columns
        SQLSMALLINT columns_count;
        SQLRETURN r = SQLNumResultCols(stmt_, &columns_count);
        check_odbc_error(r, stmt_, SQL_HANDLE_STMT, wide_);
        columns_.reserve(columns_count);

        // This variable will hold maximum column size over all parameters
        SQLULEN max_column_size = 0;

        // For each column get name, and push back into columns_
        for(SQLSMALLINT col = 0; col < columns_count; col++) 
        {
            column_info ci;
            SQLSMALLINT name_length = 0;
            SQLULEN column_size = 0;

            if(wide_) 
            {
                SQLWCHAR name[257] = {0};
                r = SQLDescribeColW(stmt_, col + 1, name, 256, &name_length, &ci.type_, &column_size, 0, 0);
                check_odbc_error(r, stmt_, SQL_HANDLE_STMT, wide_);
                ci.name_ = utf_to_utf<char>(name);
            }
            else 
            {
                SQLCHAR name[257] = {0};
                r=SQLDescribeColA(stmt_, col + 1, name, 256, &name_length, &ci.type_, &column_size, 0, 0);
                check_odbc_error(r, stmt_, SQL_HANDLE_STMT, wide_);
                ci.name_ = (char*)name;
            }

            // For types like varchar(max) and varbinary(max) column_size == 0
            // Explicitly assign column size
            if (SQL_VARCHAR == ci.type_ ||
                SQL_WVARCHAR == ci.type_ ||
                SQL_LONGVARCHAR == ci.type_ ||
                SQL_VARBINARY == ci.type_ || 
                SQL_LONGVARBINARY)
            {
                if (0 == column_size)
                    column_size = MAX_READ_BUFFER_SIZE;
            }

            if (column_size > max_column_size)
                max_column_size = column_size;

            ci.index_ = col;
            columns_.push_back(ci);
        }

        // Prepare columns_ for equal_range algorithm
        boost::sort(columns_, string_ref_less());

        max_column_size = (std::min)(max_column_size, MAX_READ_BUFFER_SIZE);
        column_char_buf_.resize(max_column_size + 1);
    }

    ~result()
    {
        SQLCloseCursor(stmt_);
    }

    template<typename T>
    bool operator()(T* data, typename boost::enable_if< boost::is_arithmetic<T> >::type* = 0 )
    {
        typedef mpl::map<
            mpl::pair< char,                mpl::pair< mpl::int_<SQL_C_STINYINT>,   char> >
          , mpl::pair< unsigned char,       mpl::pair< mpl::int_<SQL_C_UTINYINT>,   unsigned char> >
          , mpl::pair< short,               mpl::pair< mpl::int_<SQL_C_SSHORT>,     short> >
          , mpl::pair< unsigned short,      mpl::pair< mpl::int_<SQL_C_USHORT>,     unsigned short> >
          , mpl::pair< int,                 mpl::pair< mpl::int_<SQL_C_SLONG>,      long> >
          , mpl::pair< unsigned int,        mpl::pair< mpl::int_<SQL_C_ULONG>,      unsigned long> >
          , mpl::pair< long,                mpl::pair< mpl::int_<SQL_C_SLONG>,      long> >
          , mpl::pair< unsigned long,       mpl::pair< mpl::int_<SQL_C_ULONG>,      unsigned long> >
          , mpl::pair< long long,           mpl::pair< mpl::int_<SQL_C_SBIGINT>,    long long> >
          , mpl::pair< unsigned long long,  mpl::pair< mpl::int_<SQL_C_UBIGINT>,    unsigned long long> >
          , mpl::pair< float,               mpl::pair< mpl::int_<SQL_C_FLOAT>,      float> >
          , mpl::pair< double,              mpl::pair< mpl::int_<SQL_C_DOUBLE>,     double> >
          , mpl::pair< long double,         mpl::pair< mpl::int_<SQL_C_DOUBLE>,     double> >
          > type_ids_map;

        typedef typename mpl::at<type_ids_map, T>::type data_pair;
        typedef typename data_pair::first c_type_id;
        typedef typename data_pair::second c_type;

        c_type tmp;
        SQLLEN indicator;
        
        SQLRETURN r = SQLGetData(stmt_, fetch_col_, c_type_id::value, &tmp, sizeof(tmp), &indicator);
        if (r == SQL_SUCCESS || r == SQL_SUCCESS_WITH_INFO)
        {
            if (SQL_NULL_DATA == indicator)
                return false;

            *data = static_cast<T>(tmp);

            return true;
        }

        check_odbc_error(r, stmt_, SQL_HANDLE_STMT, wide_);

        return false;
    }

    bool operator()(std::tm* data)
    {
        TIMESTAMP_STRUCT tmp;
        SQLLEN indicator;

        SQLRETURN r = SQLGetData(stmt_, fetch_col_, SQL_C_TYPE_TIMESTAMP, &tmp, sizeof(tmp), &indicator);
        if (r == SQL_SUCCESS || r == SQL_SUCCESS_WITH_INFO)
        {
            if (SQL_NULL_DATA == indicator)
                return false;

            data->tm_isdst = -1;
            data->tm_year = tmp.year - 1900;
            data->tm_mon = tmp.month - 1;
            data->tm_mday = tmp.day;
            data->tm_hour = tmp.hour;
            data->tm_min = tmp.minute;
            data->tm_sec = tmp.second;

            // normalize and compute the remaining fields
            std::mktime(data);

            return true;
        }

        check_odbc_error(r, stmt_, SQL_HANDLE_STMT, wide_);
        return false;
    }

    bool operator()(std::string* _data)
    {
        SQLLEN indicator;
        std::string data;

        SQLRETURN r;

        do 
        {
            r = SQLGetData(stmt_, fetch_col_, SQL_C_CHAR, &column_char_buf_[0], column_char_buf_.size(), &indicator);
            check_odbc_error(r, stmt_, SQL_HANDLE_STMT, wide_);
            
            if (SQL_NULL_DATA == indicator)
                return false;

            SQLLEN bytes_read = (std::min)(indicator, (SQLLEN)column_char_buf_.size() - 1);

            data.append(column_char_buf_.begin(), column_char_buf_.begin() + bytes_read);
        } while(SQL_SUCCESS_WITH_INFO == r);

        _data->swap(data);

        return true;
    }

    bool operator()(std::ostream* data)
    {
        SQLLEN indicator;
        SQLRETURN r;
        
        do
        {
            r = SQLGetData(stmt_, fetch_col_, SQL_C_BINARY, &column_char_buf_[0], column_char_buf_.size(), &indicator);
            check_odbc_error(r, stmt_, SQL_HANDLE_STMT, wide_);
            
            if (SQL_NULL_DATA == indicator)
                return false;
            
            SQLLEN bytes_read = (std::min)(indicator, (SQLLEN)column_char_buf_.size());
            data->write(&column_char_buf_[0], bytes_read);
        } while(SQL_SUCCESS_WITH_INFO == r);

        return true;
    }

    virtual next_row has_next() 
    {
        // not supported by odbc
        return next_row_unknown;
    }

    virtual bool next()
    {
        SQLRETURN r = SQLFetch(stmt_);

        if (r == SQL_SUCCESS || r == SQL_SUCCESS_WITH_INFO) 
            return true;

        if(r == SQL_NO_DATA)
            return false;

        check_odbc_error(r, stmt_, SQL_HANDLE_STMT, wide_);

        return false;
    }
    
    virtual bool fetch(int col, const fetch_types_variant& v)
    {
        fetch_col_ = col + 1;
        return v.apply_visitor(*this);
    }

    virtual bool is_null(int col)
    {
        char buf[4];
        SQLLEN indicator;
        SQLRETURN r = SQLGetData(stmt_, col + 1, SQL_C_DEFAULT, buf, sizeof(buf), &indicator);

        if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO)
            check_odbc_error(r, stmt_, SQL_HANDLE_STMT, wide_);

        return indicator != SQL_NULL_DATA;
    }

    virtual int cols()
    {
        return (int)columns_.size();
    }

    virtual unsigned long long rows()
    {
        // not supported by odbc
        return (unsigned long long)-1;
    }

    virtual int name_to_column(const string_ref& name)
    {
        BOOST_AUTO(found, boost::equal_range(columns_, name, string_ref_less()));
        return boost::empty(found) ? -1 : found.first->index_;
    }

    virtual std::string column_to_name(int col)
    {
        BOOST_FOREACH(columns_set::const_reference elem, columns_)
        {
            if (elem.index_ == col)
                return elem.name_;
        }

        throw invalid_column();
    }

private:
    SQLHSTMT stmt_;
    bool wide_;
    int fetch_col_;
    columns_set columns_;
    std::vector<char> column_char_buf_;  
};

class odbc_bindings : public backend::bind_by_name_helper, public boost::static_visitor<boost::shared_ptr<std::pair<SQLLEN, std::string> > >
{
    typedef std::pair<SQLLEN, std::string> holder;
    typedef boost::shared_ptr<holder> holder_sp;

public:
    odbc_bindings(const string_ref& sql, bool wide) 
        : backend::bind_by_name_helper(sql, backend::question_marker())
        , stmt_(0)
        , wide_(wide) 
    {}

    void set_stmt(SQLHSTMT stmt)
    {
        stmt_ = stmt;
    }

    template<typename T>
    holder_sp operator()(T v)
    {
        std::ostringstream ss;
        ss.imbue(std::locale::classic());

        if(!std::numeric_limits<T>::is_integer)
            ss << std::setprecision(std::numeric_limits<T>::digits10 + 1);

        ss << v;

        SQLSMALLINT sqltype = std::numeric_limits<T>::is_integer ? SQL_INTEGER : SQL_DOUBLE;

        holder_sp value = boost::make_shared<holder>(0, ss.str());
        do_bind(false, SQL_C_CHAR, sqltype, *value);
        return value;
    }

    holder_sp operator()(null_type)
    {
        holder_sp value = boost::make_shared<holder>();

        SQLSMALLINT sqltype_;
        SQLULEN size_;
        SQLSMALLINT digits_;
        SQLSMALLINT nullable_;

        SQLRETURN r = SQLDescribeParam(stmt_, bind_col_, &sqltype_, &size_, &digits_, &nullable_);
        check_odbc_error(r, stmt_, SQL_HANDLE_STMT, wide_);

        do_bind(true, SQL_C_CHAR, sqltype_, *value);
        return value;
    }

    holder_sp operator()(const string_ref& v)
    {
        holder_sp value;

        if(wide_)
        {
            std::basic_string<SQLWCHAR> wstr = utf_to_utf<SQLWCHAR>(v.begin(), v.end());
            
            value = boost::make_shared<holder>(0, std::string((const char*)&wstr[0], wstr.size() * sizeof(SQLWCHAR)));
            do_bind(false, SQL_C_WCHAR, SQL_WLONGVARCHAR, *value);
        }
        else 
        {
            value = boost::make_shared<holder>();
            value->second.assign(v.begin(), v.end());
            do_bind(false, SQL_C_CHAR, SQL_LONGVARCHAR, *value);
        }

        return value;
    }

    holder_sp operator()(const std::tm& v)
    {
        holder_sp value = boost::make_shared<holder>(0, format_time(v));
        do_bind(false, SQL_C_CHAR, SQL_TYPE_TIMESTAMP, *value);
        return value;
    }

    holder_sp operator()(std::istream* v)
    {
        std::ostringstream ss;
        ss << v->rdbuf();
        holder_sp value = boost::make_shared<holder>(0, ss.str());
        do_bind(false, SQL_C_BINARY, SQL_LONGVARBINARY, *value);
        return value;
    }

private:
    void do_bind(bool null, SQLSMALLINT ctype, SQLSMALLINT sqltype, holder& value)
    {
        int r;

        if(null) 
        {
            value.first = SQL_NULL_DATA;
            r = SQLBindParameter(stmt_,
                bind_col_,
                SQL_PARAM_INPUT,
                ctype, 
                sqltype, // for null
                10, // COLUMNSIZE
                0, //  Presision
                0, // string
                0, // size
                &value.first);
        }
        else 
        {
            value.first = value.second.size();
            size_t column_size = value.second.size();
            if(ctype == SQL_C_WCHAR)
                column_size/=2;
            if(value.second.empty())
                column_size=1;
            r = SQLBindParameter(	
                stmt_,
                bind_col_,
                SQL_PARAM_INPUT,
                ctype,
                sqltype,
                column_size, // COLUMNSIZE
                0, //  Presision
                (void*)value.second.c_str(), // string
                value.second.size(),
                &value.first);
        }

        check_odbc_error(r,stmt_,SQL_HANDLE_STMT,wide_);
    }

    virtual void bind_impl(int col, bind_types_variant const& v)
    {
        bind_col_ = col;
        params_.push_back(v.apply_visitor(*this));
    }

    virtual void reset_impl()
    {
        SQLFreeStmt(stmt_, SQL_UNBIND);
        SQLCloseCursor(stmt_);
        params_.resize(0);
    }

private:
    SQLHSTMT stmt_;
    bool wide_;
    int bind_col_;
    std::vector<holder_sp> params_;
};

class statement : public backend::statement 
{
public:
    statement(const string_ref& q, SQLHDBC dbc, bool wide, bool prepared, session_monitor* sm) 
        : backend::statement(sm, q)
        , bindings_(q, wide)
        , dbc_(dbc)
        , wide_(wide)
        , prepared_(prepared)
    {
        bool stmt_created = false;
        SQLRETURN r = SQLAllocHandle(SQL_HANDLE_STMT,dbc,&stmt_);
        check_odbc_error(r,dbc,SQL_HANDLE_DBC,wide_);
        stmt_created = true;
        if(prepared_) 
        {
            try 
            {
                if(wide_) 
                {
                    r = SQLPrepareW(
                        stmt_,
                        (SQLWCHAR*)utf_to_utf<SQLWCHAR>(bindings_.sql()).c_str(),
                        SQL_NTS);
                }
                else {
                    r = SQLPrepareA(
                        stmt_,
                        (SQLCHAR*)bindings_.sql().c_str(),
                        SQL_NTS);
                }
                check_error(r);
            }
            catch(...) {
                SQLFreeHandle(SQL_HANDLE_STMT,stmt_);
                throw;
            }

            SQLSMALLINT params_no;
            r = SQLNumParams(stmt_,&params_no);
            check_error(r);
        }

        bindings_.set_stmt(stmt_);
    }
    ~statement()
    {
        SQLFreeHandle(SQL_HANDLE_STMT,stmt_);
    }

    virtual const char* orig_sql() const
    {
        return bindings_.sql().c_str();
    }

    virtual edba::backend::bindings& bindings()
    {
        return bindings_;
    }

    virtual long long sequence_last(std::string const &sequence) 
    {
        // evaluate statement
        boost::intrusive_ptr<statement> st;
        if (sequence.empty() && !last_insert_id_.empty())
            st.reset(new statement(last_insert_id_,dbc_,wide_,false, 0));
        else if (!sequence.empty() && !sequence_last_.empty()) 
        {
            st.reset(new statement(sequence_last_,dbc_,wide_,false, 0));
            st->bindings().bind(1,sequence);
        }
        else         
        {
            if (sequence.empty()) 
                throw not_supported_by_backend(
                "edba::odbc::last_insert_id is not supported by odbc backend "
                "unless properties @last_insert_id is specified "
                "or @engine is one of mysql, sqlite3, postgresql, mssql");
            else
                throw not_supported_by_backend(
                "edba::odbc::sequence_last is not supported by odbc backend "
                "unless properties @sequence_last is specified "
                "or @engine is one of mysql, sqlite3, postgresql, mssql");
        }

        // execute query
        boost::intrusive_ptr<result> res = boost::static_pointer_cast<result>(st->query());
        long long last_id;
        if(!res->next() || res->cols()!=1 || !res->fetch(0, fetch_types_variant(&last_id)))
            throw edba_error("edba::odbc::sequence_last failed to fetch last value");
        
        return last_id;
    }
    virtual unsigned long long affected() 
    {
        SQLLEN rows = 0;
        int r = SQLRowCount(stmt_,&rows);
        check_error(r);
        return rows;
    }
    virtual boost::intrusive_ptr<backend::result> query_impl()
    {
        int r = real_exec();
        check_error(r);
        return boost::intrusive_ptr<result>(new result(stmt_, wide_));
    }

    int real_exec()
    {
        int r = 0;
        if(prepared_) {
            r=SQLExecute(stmt_);
        }
        else {
            if(wide_)
                r=SQLExecDirectW(stmt_,(SQLWCHAR*)utf_to_utf<SQLWCHAR>(bindings_.sql()).c_str(),SQL_NTS);
            else
                r=SQLExecDirectA(stmt_,(SQLCHAR*)bindings_.sql().c_str(),SQL_NTS);
        }
        return r;
    }
    virtual void exec_impl()
    {
        int r=real_exec();
        if(r!=SQL_NO_DATA)
            check_error(r);
    }
    // End of API

private:
    void check_error(int code)
    {
        check_odbc_error(code,stmt_,SQL_HANDLE_STMT,wide_);
    }

private:
    friend class connection;

    odbc_bindings bindings_;
    SQLHDBC dbc_;
    SQLHSTMT stmt_;
    bool wide_;

    std::string sequence_last_;
    std::string last_insert_id_;
    bool prepared_;

};

class connection : public backend::connection {
public:

    connection(const conn_info& ci, session_monitor* sm) : backend::connection(ci, sm), ci_(ci)
    {
        string_ref utf = ci.get("@utf", "narrow");
        if(boost::iequals(utf, "narrow"))
            wide_ = false;
        else if(boost::iequals(utf, "wide"))
            wide_ = true;
        else
            throw edba_error("edba::odbc:: @utf property can be either 'narrow' or 'wide'");

        bool env_created = false;
        bool dbc_created = false;
        bool dbc_connected = false;

        try {
            SQLRETURN r = SQLAllocHandle(SQL_HANDLE_ENV,SQL_NULL_HANDLE,&env_);
            if(!SQL_SUCCEEDED(r)) {
                throw edba_error("edba::odbc::Failed to allocate environment handle");
            }
            env_created = true;
            r = SQLSetEnvAttr(env_,SQL_ATTR_ODBC_VERSION,(SQLPOINTER)SQL_OV_ODBC3, 0);
            check_odbc_error(r,env_,SQL_HANDLE_ENV,wide_);
            r = SQLAllocHandle(SQL_HANDLE_DBC,env_,&dbc_);
            check_odbc_error(r,env_,SQL_HANDLE_ENV,wide_);
            dbc_created = true;
            if(wide_) {
                r = SQLDriverConnectW(dbc_,0,
                    (SQLWCHAR*)utf_to_utf<SQLWCHAR>(ci.conn_string()).c_str(),
                    SQL_NTS,0,0,0,SQL_DRIVER_COMPLETE);
            }
            else {
                r = SQLDriverConnectA(dbc_,0,
                    (SQLCHAR*)ci.conn_string().c_str(),
                    SQL_NTS,0,0,0,SQL_DRIVER_COMPLETE);
            }
            check_odbc_error(r,dbc_,SQL_HANDLE_DBC,wide_);
        }
        catch(...) {
            if(dbc_connected)
                SQLDisconnect(dbc_);
            if(dbc_created)
                SQLFreeHandle(SQL_HANDLE_DBC,dbc_);
            if(env_created)
                SQLFreeHandle(SQL_HANDLE_ENV,env_);
            throw;
        }

        // TODO: Reimplement to use unicode SQLGetInfo when wide_ is true

        // remeber real database name and database version
        // get database name
        char buf[256];
        SQLSMALLINT len = 0;
        SQLRETURN rc = SQLGetInfoA(dbc_, SQL_DBMS_NAME, &buf, sizeof buf, &len );

        if( SQL_SUCCESS == rc || SQL_SUCCESS_WITH_INFO == rc ) 
        {
            if (boost::iequals(buf, "Postgresql")) 
                engine_ = "PgSQL";
            else
                engine_ = buf;
        }
        else
            engine_ = "Unknown";

        // get version
        rc = SQLGetInfoA( dbc_, SQL_DBMS_VER, &buf, sizeof buf, &len );

        if( SQL_SUCCESS == rc || SQL_SUCCESS_WITH_INFO == rc ) 
        {
            if (2 != EDBA_SSCANF(buf, "%2d.%2d", &ver_major_, &ver_minor_))
                ver_major_ = ver_minor_ = -1;
        }

        // get user name
        rc = SQLGetInfoA( dbc_, SQL_USER_NAME, &buf, sizeof buf, &len );

        char desc_buf[256];
        if( SQL_SUCCESS == rc || SQL_SUCCESS_WITH_INFO == rc ) 
            EDBA_SNPRINTF(desc_buf, 255, "%s version %d.%d, user is '%s'", engine_.c_str(), ver_major_, ver_minor_, buf);
        else
            EDBA_SNPRINTF(desc_buf, 255, "%s version %d.%d", engine_.c_str(), ver_major_, ver_minor_);

        // remember description
        description_ = desc_buf;

        // initialize sequance_last_ and last_insert_id_
        string_ref seq = ci_.get("@sequence_last","");
        if(seq.empty()) 
        {
            const std::string& eng=engine();
            if(boost::iequals(eng, "sqlite3"))
                last_insert_id_ = "select last_insert_rowid()";
            else if(boost::iequals(eng, "mysql"))
                last_insert_id_ = "select last_insert_id()";
            else if(boost::iequals(eng, "pgsql"))
            {
                last_insert_id_ = "select lastval()";
                sequence_last_ = "select currval(?)";
            }
            else if(boost::iequals(eng, "Microsoft SQL Server"))
                last_insert_id_ = "select @@identity";
        }
        else 
        {
            // TODO: avoid copying
            if(std::find(seq.begin(), seq.end(), '?') == seq.end())
                last_insert_id_.assign(seq.begin(), seq.end());
            else
                sequence_last_.assign(seq.begin(), seq.end());
        }
    }

    ~connection()
    {
        SQLDisconnect(dbc_);
        SQLFreeHandle(SQL_HANDLE_DBC,dbc_);
        SQLFreeHandle(SQL_HANDLE_ENV,env_);
    }

    /// API 
    virtual void begin_impl()
    {
        set_autocommit(false);
    }
    virtual void commit_impl()
    {
        SQLRETURN r = SQLEndTran(SQL_HANDLE_DBC,dbc_,SQL_COMMIT);
        check_odbc_error(r,dbc_,SQL_HANDLE_DBC,wide_);
        set_autocommit(true);
    }

    virtual void rollback_impl() 
    {
        try
        {
            SQLRETURN r = SQLEndTran(SQL_HANDLE_DBC,dbc_,SQL_ROLLBACK);
            check_odbc_error(r,dbc_,SQL_HANDLE_DBC,wide_);
        } 
        catch(...) {}
        
        try 
        {
            set_autocommit(true);
        } 
        catch(...){}
    }
    boost::intrusive_ptr<backend::statement> real_prepare(const string_ref& q,bool prepared)
    {
        boost::intrusive_ptr<statement> st(new statement(q,dbc_,wide_,prepared, sm_));
        st->sequence_last_ = sequence_last_;
        st->last_insert_id_ = last_insert_id_;

        return st;
    }

    virtual boost::intrusive_ptr<backend::statement> prepare_statement_impl(const string_ref& q)
    {
        return real_prepare(q,true);
    }

    virtual boost::intrusive_ptr<backend::statement> create_statement_impl(const string_ref& q)
    {
        return real_prepare(q,false);
    }

    virtual void exec_batch_impl(const string_ref& q)
    {
        using namespace boost::algorithm;

        BOOST_AUTO(spl_iter, (make_split_iterator(q, first_finder(";"))));
        BOOST_TYPEOF(spl_iter) spl_iter_end;

        for (;spl_iter != spl_iter_end; ++spl_iter)
        {
            if (spl_iter->empty()) 
                continue;

            create_statement_impl(*spl_iter)->exec();    
        }
    }

    virtual std::string escape(std::string const &s)
    {
        return escape(s.c_str(),s.c_str()+s.size());
    }
    virtual std::string escape(char const *s)
    {
        return escape(s,s+strlen(s));
    }
    virtual std::string escape(char const * /*b*/,char const * /*e*/)
    {
        throw not_supported_by_backend("cppcms::odbc:: string escaping is not supported");
    }
    virtual const std::string& backend() 
    {
        return g_backend;
    }
    virtual const std::string& engine() 
    {
        return engine_;
    }
    virtual void version(int& major, int& minor)
    {
        major = ver_major_;
        minor = ver_minor_;
    }
    virtual const std::string& description()
    {
        return description_;
    }

    void set_autocommit(bool on)
    {
        SQLPOINTER mode = (SQLPOINTER)(on ? SQL_AUTOCOMMIT_ON : SQL_AUTOCOMMIT_OFF);
        SQLRETURN r = SQLSetConnectAttr(
            dbc_, // handler
            SQL_ATTR_AUTOCOMMIT, // option
            mode, //value
            0);
        check_odbc_error(r,dbc_,SQL_HANDLE_DBC,wide_);
    }

private:
    conn_info ci_;
    SQLHENV env_;
    SQLHDBC dbc_;
    bool wide_;
    std::string engine_;
    int ver_major_;
    int ver_minor_;
    std::string description_;
    std::string sequence_last_;
    std::string last_insert_id_;
};


}} // edba, odbc_backend

extern "C" {
    EDBA_DRIVER_API edba::backend::connection *edba_odbc_get_connection(const edba::conn_info& cs, edba::session_monitor* sm)
    {
        return new edba::odbc_backend::connection(cs, sm);
    }
}
