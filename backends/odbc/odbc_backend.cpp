#define EDBA_DRIVER_SOURCE
#include <edba/backend.hpp>
#include <edba/utils.hpp>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/find_iterator.hpp>
#include <boost/typeof/typeof.hpp>
#include <boost/static_assert.hpp>

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

namespace edba { namespace odbc_backend {

const std::string g_backend("odbc");

typedef unsigned odbc_u32;
typedef unsigned short odbc_u16;

BOOST_STATIC_ASSERT(sizeof(unsigned) == 4);
BOOST_STATIC_ASSERT(sizeof(unsigned short) == 2);
BOOST_STATIC_ASSERT(sizeof(SQLWCHAR) == 2);

std::string widen(const chptr_range& utf8)
{
#ifdef _WIN32    
    // determine required size
    int required = MultiByteToWideChar(CP_UTF8, 0, utf8.begin(), int(utf8.size()), 0, 0);

    // convert
    std::string utf16;
    utf16.resize(required*2);
    MultiByteToWideChar(
        CP_UTF8
      , 0
      , utf8.begin()
      , int(utf8.size())
      , reinterpret_cast<wchar_t*>(&utf16[0])
      , required
      );
    return utf16;
#else
    NOT_IMPLEMENTED;
#endif
}


std::basic_string<SQLWCHAR> tosqlwide(const chptr_range& utf8)
{
#ifdef _WIN32    
    // determine required size
    int required = MultiByteToWideChar(CP_UTF8, 0, utf8.begin(), int(utf8.size()), 0, 0);

    // convert
    std::basic_string<SQLWCHAR> utf16;
    utf16.resize(required);
    MultiByteToWideChar(CP_UTF8, 0, utf8.begin(), int(utf8.size()), &utf16[0], required);
    return utf16;
#else
    NOT_IMPLEMENTED;
#endif
}

std::string narrower(const std::basic_string<SQLWCHAR>& utf16)
{
#ifdef _WIN32
    int required = WideCharToMultiByte(CP_UTF8, 0, utf16.c_str(), int(utf16.size()), 0, 0, 0, 0);

    std::string utf8;
    utf8.resize(required);
    WideCharToMultiByte(CP_UTF8, 0, utf16.c_str(), int(utf16.size()), const_cast<char*>(utf8.data()), required, 0, 0);
    return utf8;
#else
    NOT_IMPLEMENTED;
#endif
}

std::string narrower(const std::string& utf16)
{
#ifdef _WIN32
    int required = WideCharToMultiByte(
        CP_UTF8
      , 0
      , reinterpret_cast<LPCWSTR>(utf16.c_str())
      , int(utf16.size()/2)
      , 0, 0, 0, 0
      );

    std::string utf8;
    utf8.resize(required);
    WideCharToMultiByte(
        CP_UTF8
      , 0
      , reinterpret_cast<LPCWSTR>(utf16.c_str())
      , int(utf16.size()/2)
      , const_cast<char*>(utf8.data())
      , required
      , 0, 0
      );

    return utf8;
#else
    NOT_IMPLEMENTED;
#endif
}
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
    try { std::string tmp = narrower(error_message); utf8_str = tmp; } catch(...){}
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

class result : public backend::result {
public:
    typedef std::pair<bool,std::string> cell_type;
    typedef std::vector<cell_type> row_type;
    typedef std::list<row_type> rows_type;

    virtual next_row has_next()
    {
        rows_type::iterator p=current_;
        if(p == rows_.end() || ++p==rows_.end())
            return last_row_reached;
        else
            return next_row_exists;
    }
    virtual bool next() 
    {
        if(started_ == false) {
            current_ = rows_.begin();
            started_ = true;
        }
        else if(current_!=rows_.end()) {
            ++current_;
        }
        return current_!=rows_.end();
    }
    template<typename T>
    bool do_fetch(int col,T& v)
    {
        if(at(col).first)
            return false;

        parse_number(at(col).second, v);
        return true;
    }
    virtual bool fetch(int col,short &v)
    {
        return  do_fetch(col,v);	
    }
    virtual bool fetch(int col,unsigned short &v)
    {
        return  do_fetch(col,v);	
    }
    virtual bool fetch(int col,int &v)
    {
        return  do_fetch(col,v);	
    }
    virtual bool fetch(int col,unsigned &v)
    {
        return  do_fetch(col,v);	
    }
    virtual bool fetch(int col,long &v)
    {
        return  do_fetch(col,v);	
    }
    virtual bool fetch(int col,unsigned long &v)
    {
        return  do_fetch(col,v);	
    }
    virtual bool fetch(int col,long long &v)
    {
        return  do_fetch(col,v);	
    }
    virtual bool fetch(int col,unsigned long long &v)
    {
        return  do_fetch(col,v);	
    }
    virtual bool fetch(int col,float &v)
    {
        return  do_fetch(col,v);	
    }
    virtual bool fetch(int col,double &v)
    {
        return  do_fetch(col,v);	
    }
    virtual bool fetch(int col,long double &v)
    {
        return  do_fetch(col,v);	
    }
    virtual bool fetch(int col,std::string &v)
    {
        if(at(col).first)
            return false;
        v=at(col).second;
        return true;
    }
    virtual bool fetch(int col,std::ostream &v) 
    {
        if(at(col).first)
            return false;
        v << at(col).second;
        return true;
    }
    virtual bool fetch(int col,std::tm &v)
    {
        if(at(col).first)
            return false;
        v = parse_time(at(col).second);
        return true;
    }
    virtual bool is_null(int col)
    {
        return at(col).first;
    }
    virtual int cols()
    {
        return cols_;
    }
    virtual unsigned long long rows() 
    {
        return rows_.size();
    }
    virtual int name_to_column(const chptr_range& cn) 
    {
        for(unsigned i=0; i<names_.size(); i++)
            if(boost::algorithm::iequals(names_[i], cn))
                return i;
        return -1;
    }
    virtual std::string column_to_name(int c) 
    {
        if(c < 0 || c >= int(names_.size()))
            throw invalid_column();
        return names_[c];
    }

    result(rows_type &rows,std::vector<std::string> &names,int cols) : cols_(cols)
    {
        names_.swap(names);
        rows_.swap(rows);
        started_ = false;
        current_ = rows_.end();
    }
    cell_type &at(int col)
    {
        if(current_!=rows_.end() && col >= 0 && col <int(current_->size()))
            return current_->at(col);
        throw invalid_column();
    }
private:
    int cols_;
    bool started_;
    std::vector<std::string> names_;
    rows_type::iterator current_;
    rows_type rows_;
};

class statement : public backend::statement {
    struct parameter {
        parameter() : 
            null(true),
            ctype(SQL_C_CHAR),
            sqltype(SQL_C_NUMERIC)
        {
        }
        void set_binary(char const *b,char const *e)
        {
            value.assign(b,e-b);
            null=false;
            ctype=SQL_C_BINARY;
            sqltype = SQL_LONGVARBINARY;
        }
        void set_text(char const *b,char const *e,bool wide)
        {
            if(!wide) {
                value.assign(b,e-b);
                null=false;
                ctype=SQL_C_CHAR;
                sqltype = SQL_LONGVARCHAR;
            }
            else {
                std::string tmp = widen(chptr_range(b,e));
                value.swap(tmp);
                null=false;
                ctype=SQL_C_WCHAR;
                sqltype = SQL_WLONGVARCHAR;
            }
        }
        void set(std::tm const &v)
        {
            value = format_time(v);
            null=false;
            sqltype = SQL_C_TIMESTAMP;
            ctype = SQL_C_CHAR;
        }

        template<typename T>
        void set(T v)
        {
            std::ostringstream ss;
            ss.imbue(std::locale::classic());
            if(!std::numeric_limits<T>::is_integer)
                ss << std::setprecision(std::numeric_limits<T>::digits10+1);
            ss << v;

            value=ss.str();
            null=false;
            ctype = SQL_C_CHAR;
            if(std::numeric_limits<T>::is_integer) 
                sqltype = SQL_INTEGER;
            else
                sqltype = SQL_DOUBLE;

        }
        void bind(int col,SQLHSTMT stmt,bool wide)
        {
            int r;
            if(null) {
                lenval = SQL_NULL_DATA;
                r = SQLBindParameter(	stmt,
                    col,
                    SQL_PARAM_INPUT,
                    SQL_C_CHAR, 
                    SQL_NUMERIC, // for null
                    10, // COLUMNSIZE
                    0, //  Presision
                    0, // string
                    0, // size
                    &lenval);
            }
            else {
                lenval=value.size();
                size_t column_size = value.size();
                if(ctype == SQL_C_WCHAR)
                    column_size/=2;
                if(value.empty())
                    column_size=1;
                r = SQLBindParameter(	
                    stmt,
                    col,
                    SQL_PARAM_INPUT,
                    ctype,
                    sqltype,
                    column_size, // COLUMNSIZE
                    0, //  Presision
                    (void*)value.c_str(), // string
                    value.size(),
                    &lenval);
            }
            check_odbc_error(r,stmt,SQL_HANDLE_STMT,wide);
        }

        std::string value;
        bool null;
        SQLSMALLINT ctype;
        SQLSMALLINT sqltype;
        SQLLEN lenval;
    };

public:
    // Begin of API
    virtual void reset_impl()
    {
        SQLFreeStmt(stmt_,SQL_UNBIND);
        SQLCloseCursor(stmt_);
        params_.resize(0);
        if(params_no_ > 0)
            params_.resize(params_no_);

    }
    parameter &param_at(int col)
    {
        col --;
        if(col < 0)
            throw invalid_placeholder();
        if(params_no_ < 0) {
            if(params_.size() < size_t(col+1))
                params_.resize(col+1);
        }
        else if(col >= params_no_) {
            throw invalid_placeholder();
        }
        return params_[col];
    }
    virtual void bind_impl(int col,const chptr_range& rng)
    {
        param_at(col).set_text(rng.begin(),rng.end(),wide_);
    }
    virtual void bind_impl(int col,std::tm const &s)
    {
        param_at(col).set(s);
    }
    virtual void bind_impl(int col,std::istream &in) 
    {
        std::ostringstream ss;
        ss << in.rdbuf();
        std::string s = ss.str();
        param_at(col).set_binary(s.c_str(),s.c_str()+s.size());
    }
    template<typename T>
    void do_bind_num(int col,T v)
    {
        param_at(col).set(v);
    }
    virtual void bind_impl(int col,int v) 
    {
        do_bind_num(col,v);
    }
    virtual void bind_impl(int col,unsigned v)
    {
        do_bind_num(col,v);
    }
    virtual void bind_impl(int col,long v)
    {
        do_bind_num(col,v);
    }
    virtual void bind_impl(int col,unsigned long v)
    {
        do_bind_num(col,v);
    }
    virtual void bind_impl(int col,long long v)
    {
        do_bind_num(col,v);
    }
    virtual void bind_impl(int col,unsigned long long v)
    {
        do_bind_num(col,v);
    }
    virtual void bind_impl(int col,double v)
    {
        do_bind_num(col,v);
    }
    virtual void bind_impl(int col,long double v)
    {
        do_bind_num(col,v);
    }
    virtual void bind_null_impl(int col)
    {
        param_at(col) = parameter();
    }
    void bind_all()
    {
        for(unsigned i=0;i<params_.size();i++) {
            params_[i].bind(i+1,stmt_,wide_);
        }

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
            st->bind(1,sequence);
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
        if(!res->next() || res->cols()!=1 || !res->fetch(0,last_id))
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
        bind_all();
        int r = real_exec();
        check_error(r);
        result::rows_type rows;
        result::row_type row;

        std::string value;
        bool is_null = false;
        SQLSMALLINT ocols;
        r = SQLNumResultCols(stmt_,&ocols);
        check_error(r);
        int cols = ocols;

        std::vector<std::string> names(cols);
        std::vector<int> types(cols,SQL_C_CHAR);

        for(int col=0;col < cols;col++) {
            SQLSMALLINT name_length=0,data_type=0,digits=0,nullable=0;
            SQLULEN collen = 0;

            if(wide_) {
                SQLWCHAR name[257] = {0};
                r=SQLDescribeColW(stmt_,col+1,name,256,&name_length,&data_type,&collen,&digits,&nullable);
                check_error(r);
                names[col]=narrower(name);
            }
            else {
                SQLCHAR name[257] = {0};
                r=SQLDescribeColA(stmt_,col+1,name,256,&name_length,&data_type,&collen,&digits,&nullable);
                check_error(r);
                names[col]=(char*)name;
            }
            switch(data_type) {
    case SQL_CHAR:
    case SQL_VARCHAR:
    case SQL_LONGVARCHAR:
        types[col]=SQL_C_CHAR;
        break;
    case SQL_WCHAR:
    case SQL_WVARCHAR:
    case SQL_WLONGVARCHAR:
        types[col]=SQL_C_WCHAR ;
        break;
    case SQL_BINARY:
    case SQL_VARBINARY:
    case SQL_LONGVARBINARY:
        types[col]=SQL_C_BINARY ;
        break;
    default:
        types[col]=SQL_C_DEFAULT;
        // Just a hack, actually I'm going to use C
        ;
            }
        }

        while((r=SQLFetch(stmt_))==SQL_SUCCESS || r==SQL_SUCCESS_WITH_INFO) {
            row.resize(cols);
            for(int col=0;col < cols;col++) {
                SQLLEN len = 0;
                is_null=false;
                int type = types[col];
                if(type==SQL_C_DEFAULT) {
                    char buf[64];
                    int r = SQLGetData(stmt_,col+1,SQL_C_CHAR,buf,sizeof(buf),&len);
                    check_error(r);
                    if(len == SQL_NULL_DATA) {
                        is_null = true;
                    }
                    else if(len <= 64) {
                        value.assign(buf,len);
                    }
                    else {
                        throw edba_error("edba::odbc::query - data too long");
                    }
                }
                else {
                    char buf[1024];
                    size_t real_len;
                    if(type == SQL_C_CHAR) {
                        real_len = sizeof(buf)-1;
                    }
                    else if(type == SQL_C_BINARY) {
                        real_len = sizeof(buf);
                    }
                    else { // SQL_C_WCHAR
                        real_len = sizeof(buf) - sizeof(SQLWCHAR);
                    }

                    r = SQLGetData(stmt_,col+1,type,buf,sizeof(buf),&len);
                    check_error(r);
                    if(len == SQL_NULL_DATA) {
                        is_null = true;	
                    }
                    else if(len == SQL_NO_TOTAL) {
                        while(len==SQL_NO_TOTAL) {
                            value.append(buf,real_len);
                            r = SQLGetData(stmt_,col+1,type,buf,sizeof(buf),&len);
                            check_error(r);
                        }
                        value.append(buf,len);
                    }
                    else if(0<= len && size_t(len) <= real_len) {
                        value.assign(buf,len);
                    }
                    else if(len>=0) {
                        value.assign(buf,real_len);
                        size_t rem_len = len - real_len;
                        std::vector<char> tmp(rem_len+2,0);
                        r = SQLGetData(stmt_,col+1,type,&tmp[0],tmp.size(),&len);
                        check_error(r);
                        value.append(&tmp[0],rem_len);
                    }
                    else {
                        throw edba_error("edba::odbc::query invalid result length");
                    }
                    if(!is_null && type == SQL_C_WCHAR) {
                        std::string tmp=narrower(value);
                        value.swap(tmp);
                    }
                }

                row[col].first = is_null;
                row[col].second.swap(value);
            }
            rows.push_back(result::row_type());
            rows.back().swap(row);
        }
        if(r!=SQL_NO_DATA) {
            check_error(r);
        }
        return boost::intrusive_ptr<result>(new result(rows,names,cols));
    }

    int real_exec()
    {
        int r = 0;
        if(prepared_) {
            r=SQLExecute(stmt_);
        }
        else {
            if(wide_)
                r=SQLExecDirectW(stmt_,(SQLWCHAR*)tosqlwide(orig_sql()).c_str(),SQL_NTS);
            else
                r=SQLExecDirectA(stmt_,(SQLCHAR*)orig_sql().c_str(),SQL_NTS);
        }
        return r;
    }
    virtual void exec_impl()
    {
        bind_all();
        int r=real_exec();
        if(r!=SQL_NO_DATA)
            check_error(r);
    }
    // End of API

    statement(const chptr_range& q,SQLHDBC dbc,bool wide,bool prepared,session_monitor* sm) :
        backend::statement(sm, q),
        dbc_(dbc),
        wide_(wide),
        params_no_(-1),
        prepared_(prepared)
    {
        bool stmt_created = false;
        SQLRETURN r = SQLAllocHandle(SQL_HANDLE_STMT,dbc,&stmt_);
        check_odbc_error(r,dbc,SQL_HANDLE_DBC,wide_);
        stmt_created = true;
        if(prepared_) {
            try {
                if(wide_) {
                    r = SQLPrepareW(
                        stmt_,
                        (SQLWCHAR*)tosqlwide(orig_sql()).c_str(),
                        SQL_NTS);
                }
                else {
                    r = SQLPrepareA(
                        stmt_,
                        (SQLCHAR*)orig_sql().c_str(),
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
            params_no_ = params_no;
            params_.resize(params_no_);
        }
        else {
            params_.reserve(50);
        }
    }
    ~statement()
    {
        SQLFreeHandle(SQL_HANDLE_STMT,stmt_);
    }
private:
    void check_error(int code)
    {
        check_odbc_error(code,stmt_,SQL_HANDLE_STMT,wide_);
    }


    SQLHDBC dbc_;
    SQLHSTMT stmt_;
    bool wide_;
    std::vector<parameter> params_;
    int params_no_;

    friend class connection;
    std::string sequence_last_;
    std::string last_insert_id_;
    bool prepared_;

};

class connection : public backend::connection {
public:

    connection(const conn_info& ci, session_monitor* sm) : backend::connection(ci, sm), ci_(ci)
    {
        chptr_range utf = ci.get("@utf", "narrow");
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
                    (SQLWCHAR*)tosqlwide(ci.conn_string()).c_str(),
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
            if (2 != sscanf_s(buf, "%2d.%2d", &ver_major_, &ver_minor_))
                ver_major_ = ver_minor_ = -1;
        }

        // get user name
        rc = SQLGetInfoA( dbc_, SQL_USER_NAME, &buf, sizeof buf, &len );

        char desc_buf[256];
        if( SQL_SUCCESS == rc || SQL_SUCCESS_WITH_INFO == rc ) 
            _snprintf_s(desc_buf, 255, "%s version %d.%d, user is '%s'", engine_.c_str(), ver_major_, ver_minor_, buf);
        else
            _snprintf_s(desc_buf, 255, "%s version %d.%d", engine_.c_str(), ver_major_, ver_minor_);

        // remember description
        description_ = desc_buf;

        // initialize sequance_last_ and last_insert_id_
        chptr_range seq = ci_.get("@sequence_last","");
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
        try {
            SQLRETURN r = SQLEndTran(SQL_HANDLE_DBC,dbc_,SQL_ROLLBACK);
            check_odbc_error(r,dbc_,SQL_HANDLE_DBC,wide_);
        } catch(...) {}
        try {
            set_autocommit(true);
        } catch(...){}
    }
    boost::intrusive_ptr<backend::statement> real_prepare(const chptr_range& q,bool prepared)
    {
        boost::intrusive_ptr<statement> st(new statement(q,dbc_,wide_,prepared, sm_));
        st->sequence_last_ = sequence_last_;
        st->last_insert_id_ = last_insert_id_;

        return st;
    }

    virtual boost::intrusive_ptr<backend::statement> prepare_statement(const chptr_range& q)
    {
        return real_prepare(q,true);
    }

    virtual boost::intrusive_ptr<backend::statement> create_statement(const chptr_range& q)
    {
        return real_prepare(q,false);
    }

    virtual void exec_batch_impl(const chptr_range& q)
    {
        using namespace boost::algorithm;

        BOOST_AUTO(spl_iter, (make_split_iterator(q, first_finder(";"))));
        BOOST_TYPEOF(spl_iter) spl_iter_end;

        for (;spl_iter != spl_iter_end; ++spl_iter)
        {
            if (spl_iter->empty()) 
                continue;

            create_statement(*spl_iter)->exec();    
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
