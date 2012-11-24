#include <edba/backend/bind_by_name_helper.hpp>

#include <edba/errors.hpp>
#include <edba/detail/utils.hpp>

#include <boost/scope_exit.hpp>

#include <iostream>
#include <sstream>
#include <vector>
#include <limits>
#include <iomanip>

#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#  include <windows.h>
#endif

#include <mysql.h>

namespace edba { namespace mysql_backend {	

std::string g_backend_and_engine = "mysql";

class edba_myerror : public edba_error 
{
public:
    edba_myerror(std::string const &str) : edba_error("edba::mysql::" + str) {}
};

namespace unprep {

class result : public backend::result, public boost::static_visitor<bool>
{
public:
    result(MYSQL *conn) : 
        res_(0)
      , cols_(0)
      , current_row_(0)
      , row_(0)
    {
        res_ = mysql_store_result(conn);
        if(!res_) {
            cols_ = mysql_field_count(conn);
            if(cols_ == 0)
                throw edba_myerror("Seems that the query does not produce any result");
        }
        else {
            cols_ = mysql_num_fields(res_);
        }

    }
    ~result()
    {
        if(res_)
            mysql_free_result(res_);
    }

    ///
    /// Check if the next row in the result exists. If the DB engine can't perform
    /// this check without loosing data for current row, it should return next_row_unknown.
    ///
    virtual next_row has_next() 
    {
        if(!res_)
            return last_row_reached;
        if(current_row_ >= mysql_num_rows(res_))
            return last_row_reached;
        else
            return next_row_exists;
    }
    ///
    /// Move to next row. Should be called before first access to any of members. If no rows remain
    /// return false, otherwise return true
    ///
    virtual bool next() 
    {
        if(!res_)
            return false;
        current_row_ ++;
        row_ = mysql_fetch_row(res_);
        if(!row_)
            return false;
        return true;
    }

    virtual bool fetch(int col, const fetch_types_variant& v)
    {
        fetch_col_ = col;
        return v.apply_visitor(*this);
    }

    template<typename T>
    bool operator()(T* v, typename boost::enable_if< boost::is_arithmetic<T> >::type* = 0)
    {
        size_t len;
        char const *s = at(fetch_col_, len);
        if(!s)
            return false;
        parse_number(string_ref(s, len), *v);
        return true;
    }
    
    bool operator()(std::string* v)
    {
        size_t len;
        char const *s = at(fetch_col_, len);
        if(!s)
            return false;
        v->assign(s, len);
        return true;
    }

    bool operator()(std::ostream* v)
    {
        size_t len;
        char const *s = at(fetch_col_, len);
        if(!s)
            return false;
        v->write(s, len);
        return true;
    }
    ///
    /// Fetch a date-time value for column \a col starting from 0.
    /// Returns true if ok, returns false if the column value is NULL and the referenced object should remain unchanged
    ///
    /// Should throw invalid_column() \a col value is invalid. If the data can't be converted
    /// to date-time it should throw bad_value_cast()
    ///
    bool operator()(std::tm* v) 
    {
        size_t len;
        char const *s = at(fetch_col_, len);
        if(!s)
            return false;
        std::string tmp(s, len);
        *v = parse_time(tmp);
        return true;
    }
    ///
    /// Check if the column \a col is NULL starting from 0, should throw invalid_column() if the index out of range
    ///
    virtual bool is_null(int col) 
    {
        return at(col) == 0;
    }
    ///
    /// Return the number of columns in the result. Should be valid even without calling next() first time.
    ///
    virtual int cols() 
    {
        return cols_;
    }
    virtual unsigned long long rows() 
    {
        return unsigned long long(mysql_num_rows(res_));
    }
    virtual std::string column_to_name(int col) 
    {
        if(col < 0 || col >=cols_)
            throw invalid_column();
        if(!res_)
            throw empty_row_access();
        MYSQL_FIELD *flds=mysql_fetch_fields(res_);
        if(!flds) {
            throw edba_myerror("Internal error empty fileds");
        }
        return flds[col].name;
    }
    virtual int name_to_column(const string_ref& name) 
    {
        if(!res_)
            throw empty_row_access();
        MYSQL_FIELD *flds=mysql_fetch_fields(res_);
        if(!flds) {
            throw edba_myerror("Internal error empty fileds");
        }
        for(int i=0;i<cols_;i++)
            if(boost::iequals(name, flds[i].name))
                return i;
        return -1;
    }

    // End of API
private:
    ///
    /// Fetch an integer value for column \a col starting from 0.
    ///
    /// Should throw invalid_column() \a col value is invalid, should throw bad_value_cast() if the underlying data
    /// can't be converted to integer or its range is not supported by the integer type.
    ///
    char const *at(int col)
    {
        if(!res_)
            throw empty_row_access();
        if(col < 0 || col >= cols_)
            throw invalid_column();
        return row_[col];
    }

    char const *at(int col,size_t &len)
    {
        if(!res_)
            throw empty_row_access();
        if(col < 0 || col >= cols_)
            throw invalid_column();
        unsigned long *lengths = mysql_fetch_lengths(res_);
        if(lengths==0) 
            throw edba_myerror("Can't get length of column");
        len = lengths[col];
        return row_[col];
    }

    MYSQL_RES *res_;
    int cols_;
    unsigned current_row_;
    int fetch_col_;
    MYSQL_ROW row_;
};

class statement : public backend::bind_by_name_helper, public boost::static_visitor<>
{
public:
    statement(const string_ref& q, MYSQL *conn, session_monitor* sm) 
      : backend::bind_by_name_helper(sm, q, backend::question_marker())
      , conn_(conn)
      , params_no_(0)
    {
        fmt_.imbue(std::locale::classic());
        bool inside_text = false;
        std::string patched_sql = patched_query();

        for (size_t i = 0; i < patched_sql.size(); i++) 
        {
            if(patched_sql[i]=='\'') 
                inside_text=!inside_text;

            if(patched_sql[i]=='?' && !inside_text) 
            {
                params_no_++;
                binders_.push_back(i);
            }
        }

        if(inside_text)
            throw edba_myerror("Unterminated string found in query");

        reset_params();
    }

    virtual void bindings_reset_impl()
    {
    }

    virtual void bind_impl(int col, bind_types_variant const& v)
    {
        bind_col_ = col;
        v.apply_visitor(*this);
    }
    
    template<typename T>
    void operator()(T v, typename boost::enable_if< boost::is_arithmetic<T> >::type* = 0)
    {
        fmt_.str(std::string());
        if(!std::numeric_limits<T>::is_integer)
            fmt_ << std::setprecision(std::numeric_limits<T>::digits10+1);
        fmt_ << v;
        std::string tmp = fmt_.str();
        at(bind_col_).swap(tmp);
    }
    
    void operator()(const string_ref& rng)
    {
        std::vector<char> buf(2*rng.size() + 1);
        size_t len = mysql_real_escape_string(conn_, &buf.front(), rng.begin(), rng.size());
        std::string& s = at(bind_col_);
        s.clear();
        s.reserve(rng.size()+2);
        s += '\'';
        s.append(&buf.front(),len);
        s += '\'';
    }

    void operator()(const std::tm& v) 
    {
        std::string& s = at(bind_col_);
        s.clear();
        s.reserve(30);
        s += '\'';
        s += format_time(v);
        s += '\'';
    }

    void operator()(std::istream* v)
    {
        std::ostringstream ss;
        ss << v->rdbuf();
        std::string tmp = ss.str();
        bind(bind_col_, tmp);
    }

    void operator()(null_type)
    {
        at(bind_col_) = "NULL";
    }

    // backend::statement implementation

    virtual long long sequence_last(std::string const &/*sequence*/) 
    {
        return mysql_insert_id(conn_);
    }

    virtual unsigned long long affected()
    {
        return mysql_affected_rows(conn_);
    }

    virtual boost::intrusive_ptr<edba::backend::result> query_impl() 
    {
        std::string real_query;	
        bind_all(real_query);
        reset_params();
        if(mysql_real_query(conn_,real_query.c_str(),real_query.size())) {
            throw edba_myerror(mysql_error(conn_));
        }
        return new result(conn_);
    }

    virtual void exec_impl() 
    {
        std::string real_query;	
        bind_all(real_query);
        reset_params();
        if(mysql_real_query(conn_,real_query.c_str(),real_query.size())) 
            throw edba_myerror(mysql_error(conn_));

        MYSQL_RES *r = mysql_store_result(conn_);
        if (r)
        {
            mysql_free_result(r);
            throw edba_myerror("Calling exec() on query!");
        }
    }

private:
    std::string &at(int col)
    {
        if(col < 1 || col > params_no_) 
            throw invalid_placeholder();

        return params_[col-1];
    }

    void reset_params()
    {
        params_.clear();
        params_.resize(params_no_,"NULL");
    }

    void bind_all(std::string& real_query)
    {
        std::string patched_sql = patched_query();

        size_t total = patched_sql.size();
        for(unsigned i=0;i<params_.size();i++) 
            total+=params_[i].size();

        real_query.clear();
        real_query.reserve(total);
        size_t pos_ = 0;
        for(unsigned i=0;i<params_.size();i++) 
        {
            size_t marker = binders_[i];
            real_query.append(patched_sql, pos_, marker-pos_);
            pos_ = marker+1;
            real_query.append(params_[i]);
        }
        real_query.append(patched_sql, pos_, std::string::npos);
    }

    std::ostringstream fmt_;
    std::vector<std::string> params_;
    std::vector<size_t> binders_;

    MYSQL *conn_;
    int params_no_;
    int bind_col_;
};

} // namespace uprep

namespace prep {

class result : public backend::result, public boost::static_visitor<bool>
{
    struct bind_data 
    {
        bind_data() : ptr(0), length(0), is_null(0), error(0)
        {
            memset(&buf,0,sizeof(buf));
        }

        char buf[128];
        std::vector<char> vbuf;
        char *ptr;
        unsigned long length;
        my_bool is_null;
        my_bool error;
    };

public:
    result(MYSQL_STMT *stmt) : stmt_(stmt), current_row_(0),meta_(0)
    {
        cols_ = mysql_stmt_field_count(stmt_);
        if(mysql_stmt_store_result(stmt_)) {
            throw edba_myerror(mysql_stmt_error(stmt_));
        }
        meta_ = mysql_stmt_result_metadata(stmt_);
        if(!meta_) {
            throw edba_myerror("Seems that the query does not produce any result");
        }
    }
    ~result()
    {
        mysql_free_result(meta_);
    }

    ///
    /// Check if the next row in the result exists. If the DB engine can't perform
    /// this check without loosing data for current row, it should return next_row_unknown.
    ///
    virtual next_row has_next() 
    {
        if(current_row_ >= mysql_stmt_num_rows(stmt_))
            return last_row_reached;
        else
            return next_row_exists;
    }
    ///
    /// Move to next row. Should be called before first access to any of members. If no rows remain
    /// return false, otherwise return true
    ///
    virtual bool next() 
    {
        current_row_ ++;
        reset();
        if(cols_ > 0) {
            if(mysql_stmt_bind_result(stmt_,&bind_[0])) {
                throw edba_myerror(mysql_stmt_error(stmt_));
            }
        }
        int r = mysql_stmt_fetch(stmt_);
        if(r==MYSQL_NO_DATA) { 
            return false;
        }
        if(r==MYSQL_DATA_TRUNCATED) {
            for(int i=0;i<cols_;i++) {
                if(bind_data_[i].error && !bind_data_[i].is_null && bind_data_[i].length >= sizeof(bind_data_[i].buf)) {
                    bind_data_[i].vbuf.resize(bind_data_[i].length);
                    MYSQL_BIND b=MYSQL_BIND();
                    bind_[i].buffer = &bind_data_[i].vbuf.front();
                    bind_[i].buffer_length = bind_data_[i].length;
                    if(mysql_stmt_fetch_column(stmt_,&bind_[i],i,0)) {
                        throw edba_myerror(mysql_stmt_error(stmt_));
                    }
                    bind_data_[i].ptr = &bind_data_[i].vbuf.front();
                }
            }
        }
        return true;
    }

    virtual bool fetch(int col, const fetch_types_variant& v)
    {
        fetch_col_ = col;
        return v.apply_visitor(*this);
    }

    template<typename T>
    bool operator()(T* v, typename boost::enable_if< boost::is_arithmetic<T> >::type* = 0)
    {
        bind_data& d = at(fetch_col_);
        if(d.is_null)
            return false;

        parse_number(string_ref(d.ptr,d.length), *v);

        return true;
    }

    bool operator()(std::string* v)
    {
        bind_data &d = at(fetch_col_);
        if(d.is_null)
            return false;
        v->assign(d.ptr,d.length);
        return true;
    }

    bool operator()(std::ostream* v)
    {
        bind_data &d = at(fetch_col_);
        if(d.is_null)
            return false;
        v->write(d.ptr,d.length);
        return true;
    }

    bool operator()(std::tm* v)
    {
        std::string tmp;
        if(!this->operator()(&tmp))
            return false;
        *v = parse_time(tmp);
        return true;
    }

    ///
    /// Check if the column \a col is NULL starting from 0, should throw invalid_column() if the index out of range
    ///
    virtual bool is_null(int col) 
    {
        return at(col).is_null ? true : false;
    }
    ///
    /// Return the number of columns in the result. Should be valid even without calling next() first time.
    ///
    virtual int cols() 
    {
        return cols_;
    }
    virtual unsigned long long rows() 
    {
        return unsigned long long(mysql_num_rows(meta_));
    }
    virtual std::string column_to_name(int col) 
    {
        if(col < 0 || col >=cols_)
            throw invalid_column();
        MYSQL_FIELD *flds=mysql_fetch_fields(meta_);
        if(!flds) {
            throw edba_myerror("Internal error empty fileds");
        }
        return flds[col].name;
    }
    virtual int name_to_column(const string_ref& name) 
    {
        MYSQL_FIELD *flds=mysql_fetch_fields(meta_);
        if(!flds) {
            throw edba_myerror("Internal error empty fileds");
        }
        for(int i=0;i<cols_;i++)
            if(boost::algorithm::iequals(name, flds[i].name))
                return i;
        return -1;
    }

private:
    void reset()
    {
        bind_.resize(0);
        bind_data_.resize(0);
        bind_.resize(cols_,MYSQL_BIND());
        bind_data_.resize(cols_,bind_data());
        for(int i=0;i<cols_;i++) {
            bind_[i].buffer_type = MYSQL_TYPE_STRING;
            bind_[i].buffer = bind_data_[i].buf;
            bind_[i].buffer_length = sizeof(bind_data_[i].buf);
            bind_[i].length = &bind_data_[i].length;
            bind_[i].is_null = &bind_data_[i].is_null;
            bind_[i].error = &bind_data_[i].error;
            bind_data_[i].ptr = bind_data_[i].buf;
        }
    }

    bind_data &at(int col)
    {
        if(col < 0 || col >= cols_)
            throw invalid_column();
        if(bind_data_.empty())
            throw edba_myerror("Attempt to access data without fetching it first");
        return bind_data_.at(col);
    }

    int cols_;
    MYSQL_STMT *stmt_;
    unsigned current_row_;
    MYSQL_RES *meta_;
    std::vector<MYSQL_BIND> bind_;
    std::vector<bind_data> bind_data_;
    int fetch_col_;
};

class statement : public backend::bind_by_name_helper, public boost::static_visitor<>
{
    struct param 
    {
        my_bool is_null;
        bool is_blob;
        unsigned long length;
        std::string value;
        void *buffer;

        param() : 
            is_null(1)
          , is_blob(false)
          , length(0)
          , buffer(0)
        {
        }
        void set(char const *b,char const *e,bool blob=false)
        {
            length = e - b;
            buffer = const_cast<char *>(b);
            is_blob = blob;
            is_null = 0;
        }
        void set_str(std::string const &s)
        {
            value = s;
            buffer = const_cast<char *>(value.c_str());
            length = value.size();
            is_null = 0;
        }
        void set(std::tm const &t)
        {
            set_str(format_time(t));
        }
        void bind_it(MYSQL_BIND *b) 
        {
            b->is_null = &is_null;
            if(!is_null) {
                b->buffer_type = is_blob ? MYSQL_TYPE_BLOB : MYSQL_TYPE_STRING;
                b->buffer = buffer;
                b->buffer_length = length;
                b->length = &length;
            }
            else {
                b->buffer_type = MYSQL_TYPE_NULL;
            }
        }
    };

public:
    statement(const string_ref& q, MYSQL *conn, session_monitor* sm) :
        backend::bind_by_name_helper(sm, q, backend::question_marker())
      , stmt_(0)
      , params_count_(0)
    {
        fmt_.imbue(std::locale::classic());

        stmt_ = mysql_stmt_init(conn);
        try {
            if(!stmt_) {
                throw edba_myerror(" Failed to create a statement");
            }

            std::string query = patched_query();

            if(mysql_stmt_prepare(stmt_, query.c_str(), query.size())) {
                throw edba_myerror(mysql_stmt_error(stmt_));
            }
            params_count_ = mysql_stmt_param_count(stmt_);
            reset_data();
        }
        catch(...) {
            if(stmt_)
                mysql_stmt_close(stmt_);
            throw;
        }
    }
    virtual ~statement()
    {
        mysql_stmt_close(stmt_);
    }

    virtual void bindings_reset_impl()
    {
        reset_data();
        mysql_stmt_reset(stmt_);
    }

    virtual void bind_impl(int col, bind_types_variant const& v)
    {
        bind_col_ = col;
        v.apply_visitor(*this);
    }

    template<typename T>
    void operator()(T v, typename boost::enable_if< boost::is_arithmetic<T> >::type* = 0)
    {
        fmt_.str(std::string());
        if(!std::numeric_limits<T>::is_integer)
            fmt_ << std::setprecision(std::numeric_limits<T>::digits10+1);
        fmt_ << v;
        at(bind_col_).set_str(fmt_.str());
    }

    void operator()(const string_ref& rng)
    {
        at(bind_col_).set(rng.begin(), rng.end());
    }

    void operator()(const std::tm& v)
    {
        at(bind_col_).set(v);
    }

    void operator()(std::istream* v)
    {
        std::ostringstream ss;
        ss << v->rdbuf();
        at(bind_col_).set_str(ss.str());
        at(bind_col_).is_blob = true;
    }

    void operator()(null_type)
    {
        at(bind_col_)=param();
    }

    // ----------- backend::statement -----------

    ///
    /// Fetch the last sequence generated for last inserted row. May use sequence as parameter
    /// if the database uses sequences, should ignore the parameter \a sequence if the last
    /// id is fetched without parameter.
    ///
    /// Should be called after exec() for insert statement, otherwise the behavior is undefined.
    ///
    /// MUST throw not_supported_by_backend() if such option is not supported by the DB engine.
    ///
    virtual long long sequence_last(std::string const &/*sequence*/) 
    {
        return mysql_stmt_insert_id(stmt_);
    }
    ///
    /// Return the number of affected rows by last statement.
    ///
    /// Should be called after exec(), otherwise behavior is undefined.
    ///
    virtual unsigned long long affected()
    {
        return mysql_stmt_affected_rows(stmt_);
    }

    ///
    /// Return SQL Query result, MAY throw edba_error if the statement is not a query
    ///
    virtual boost::intrusive_ptr<backend::result> query_impl() 
    {
        bind_all();
        if(mysql_stmt_execute(stmt_)) {
            throw edba_myerror(mysql_stmt_error(stmt_));
        }
        return boost::intrusive_ptr<backend::result>(new result(stmt_));
    }
    ///
    /// Execute a statement, MAY throw edba_error if the statement returns results.
    ///
    virtual void exec_impl() 
    {
        bind_all();
        if(mysql_stmt_execute(stmt_)) {
            throw edba_myerror(mysql_stmt_error(stmt_));
        }
        if(mysql_stmt_store_result(stmt_)) {
            throw edba_myerror(mysql_stmt_error(stmt_));
        }
        MYSQL_RES *r = mysql_stmt_result_metadata(stmt_);
        if(r) {
            mysql_free_result(r);
            throw edba_myerror("Calling exec() on query!");
        }
    }

private:
    void reset_data()
    {
        params_.resize(0);
        params_.resize(params_count_);
        bind_.resize(0);
        bind_.resize(params_count_,MYSQL_BIND());
    }

    param &at(int col)
    {
        if(col < 1 || col > params_count_)
            throw invalid_placeholder();
        return params_[col-1];
    }

    void bind_all()
    {
        if(!params_.empty()) {
            for(unsigned i=0;i<params_.size();i++)
                params_[i].bind_it(&bind_[i]);
            if(mysql_stmt_bind_param(stmt_,&bind_.front())) {
                throw edba_myerror(mysql_stmt_error(stmt_));
            }
        }
    }

    std::ostringstream fmt_;
    std::vector<param> params_;
    std::vector<MYSQL_BIND> bind_;
    MYSQL_STMT *stmt_;
    int params_count_;
    int bind_col_;
};

} // namespace prep

class connection : public backend::connection 
{
public:
    connection(conn_info const &ci, session_monitor* sm) : 
        backend::connection(ci, sm)
      , conn_(0)
    {
        conn_ = mysql_init(0);
        if(!conn_) {
              throw edba_error("edba::mysql failed to create connection");
        }
        std::string host = ci.get_copy("host","");
        char const *phost = host.empty() ? 0 : host.c_str();
        std::string user = ci.get_copy("user","");
        char const *puser = user.empty() ? 0 : user.c_str();
        std::string password = ci.get_copy("password","");
        char const *ppassword = password.empty() ? 0 : password.c_str();
        std::string database = ci.get_copy("database","");
        char const *pdatabase = database.empty() ? 0 : database.c_str();
        int port = ci.get("port",0);
        std::string unix_socket = ci.get_copy("unix_socket","");
        char const *punix_socket = unix_socket.empty() ? 0 : unix_socket.c_str();

#if MYSQL_VERSION_ID >= 50507 && MYSQL_VERSION_ID < 60000
        std::string default_auth = ci.get_copy("default_auth","");
        if (!default_auth.empty()) {
            mysql_set_option(MYSQL_DEFAULT_AUTH, default_auth.c_str());
        }
#endif
        std::string init_command = ci.get_copy("init_command","");
        if(!init_command.empty()) {
            mysql_set_option(MYSQL_INIT_COMMAND, init_command.c_str());
        }
        if(ci.has("opt_compress")) {
            if(ci.get("opt_compress", 1)) {
                mysql_set_option(MYSQL_OPT_COMPRESS, NULL);
            }
        }
        if(ci.has("opt_connect_timeout")) {
            if(unsigned connect_timeout = ci.get("opt_connect_timeout", 0)) {
                mysql_set_option(MYSQL_OPT_CONNECT_TIMEOUT, &connect_timeout);
            }
        }
        if(ci.has("opt_guess_connection")) {
            if(ci.get("opt_guess_connection", 1)) {
                mysql_set_option(MYSQL_OPT_GUESS_CONNECTION, NULL);
            }
        }
        if(ci.has("opt_local_infile")) {
            if(unsigned local_infile = ci.get("opt_local_infile", 0)) {
                mysql_set_option(MYSQL_OPT_CONNECT_TIMEOUT, &local_infile);
            }
        }
        if(ci.has("opt_named_pipe")) {
            if(ci.get("opt_named_pipe", 1)) {
                mysql_set_option(MYSQL_OPT_NAMED_PIPE, NULL);
            }
        }
        if(ci.has("opt_protocol")) {
            if(unsigned protocol = ci.get("opt_protocol", 0)) {
                mysql_set_option(MYSQL_OPT_PROTOCOL, &protocol);
            }
        }
        if(ci.has("opt_read_timeout")) {
            if(unsigned read_timeout = ci.get("opt_read_timeout", 0)) {
                mysql_set_option(MYSQL_OPT_READ_TIMEOUT, &read_timeout);
            }
        }
        if(ci.has("opt_reconnect")) {
            if(unsigned reconnect = ci.get("opt_reconnect", 1)) {
                my_bool value = reconnect;
                mysql_set_option(MYSQL_OPT_RECONNECT, &value);
            }
        }
#if MYSQL_VERSION_ID >= 50507 && MYSQL_VERSION_ID < 60000
        std::string plugin_dir = ci.get_copy("plugin_dir", "");
        if(!plugin_dir.empty()) {
            mysql_set_option(MYSQL_PLUGIN_DIR, plugin_dir.c_str());
        }
#endif
        std::string set_client_ip = ci.get_copy("set_client_ip", "");
        if(!set_client_ip.empty()) {
            mysql_set_option(MYSQL_SET_CLIENT_IP, set_client_ip.c_str());
        }
        if(ci.has("opt_ssl_verify_server_cert")) {
            if(unsigned verify = ci.get("opt_ssl_verify_server_cert", 1)) {
                my_bool value = verify;
                mysql_set_option(MYSQL_OPT_SSL_VERIFY_SERVER_CERT, &value);
            }
        }
        if(ci.has("opt_use_embedded_connection")) {
            if(ci.get("opt_use_embedded_connection", 1)) {
                mysql_set_option(MYSQL_OPT_USE_EMBEDDED_CONNECTION, NULL);
            }
        }
        if(ci.has("opt_use_remote_connection")) {
            if(ci.get("opt_use_remote_connection", 1)) {
                mysql_set_option(MYSQL_OPT_USE_REMOTE_CONNECTION, NULL);
            }
        }
        if(ci.has("opt_write_timeout")) {
            if(unsigned write_timeout = ci.get("opt_write_timeout", 0)) {
                mysql_set_option(MYSQL_OPT_WRITE_TIMEOUT, &write_timeout);
            }
        }
        std::string read_default_file = ci.get_copy("read_default_file", "");
        if(!read_default_file.empty()) {
            mysql_set_option(MYSQL_READ_DEFAULT_FILE, read_default_file.c_str());
        }
        std::string read_default_group = ci.get_copy("read_default_group", "");
        if(!read_default_group.empty()) {
            mysql_set_option(MYSQL_READ_DEFAULT_GROUP, read_default_group.c_str());
        }
        if(ci.has("report_data_truncation")) {
            if(unsigned report = ci.get("report_data_truncation", 1)) {
                my_bool value = report;
                mysql_set_option(MYSQL_REPORT_DATA_TRUNCATION, &value);
            }
        }
#if MYSQL_VERSION_ID >= 40101
        if(ci.has("secure_auth")) {
            if(unsigned secure = ci.get("secure_auth", 1)) {
                my_bool value = secure;
                mysql_set_option(MYSQL_SECURE_AUTH, &value);
            }
        }
#endif
        std::string set_charset_dir = ci.get_copy("set_charset_dir", "");
        if(!set_charset_dir.empty()) {
            mysql_set_option(MYSQL_SET_CHARSET_DIR, set_charset_dir.c_str());
        }
        std::string set_charset_name = ci.get_copy("set_charset_name", "");
        if(!set_charset_name.empty()) {
            mysql_set_option(MYSQL_SET_CHARSET_NAME, set_charset_name.c_str());
        }
        std::string shared_memory_base_name = ci.get_copy("shared_memory_base_name", "");
        if(!shared_memory_base_name.empty()) {
            mysql_set_option(MYSQL_SHARED_MEMORY_BASE_NAME, shared_memory_base_name.c_str());
        }

        if(!mysql_real_connect(conn_,phost,puser,ppassword,pdatabase,port,punix_socket,0)) {
            std::string err="unknown";
            try { err = mysql_error(conn_); }catch(...){}
            mysql_close(conn_);
            throw edba_myerror(err);
        }

        // prepare human readeable description
        int major;
        int minor;
        version(major, minor);
        char buf[256];
#ifdef _WIN32
        _snprintf_s(buf, 256, "MySQL version %d.%d", major, minor);
#else
        snprintf(buf, 256, "MySQL version %d.%d", major, minor);
#endif                
        description_ = buf;
    }
    ~connection()
    {
        mysql_close(conn_);
    }
    // API 

    void fast_exec(const string_ref& sql) 
    {
        if(mysql_real_query(conn_,sql.begin(),sql.size()))
            throw edba_myerror(mysql_error(conn_));

        // process each statement result

        int status;
        do 
        {
            MYSQL_RES* result = mysql_store_result(conn_);
            if (result) 
                mysql_free_result(result);

            if ((status = mysql_next_result(conn_)) > 0)
                throw edba_myerror(mysql_error(conn_));
        } 
        while(status == 0);
    }

    virtual void exec_batch_impl(const string_ref& q)
    {
        if(mysql_set_server_option(conn_, MYSQL_OPTION_MULTI_STATEMENTS_ON))
            throw edba_myerror(mysql_error(conn_));

        BOOST_SCOPE_EXIT((conn_))
        {
            mysql_set_server_option(conn_, MYSQL_OPTION_MULTI_STATEMENTS_OFF);
        } BOOST_SCOPE_EXIT_END

        fast_exec(q);
    }

    ///
    /// Start new isolated transaction. Would not be called
    /// withing other transaction on current connection.
    ///
    virtual void begin_impl() 
    {
        fast_exec("BEGIN");
    }
    ///
    /// Commit the transaction, you may assume that is called after begin()
    /// was called.
    ///
    virtual void commit_impl() 
    {
        fast_exec("COMMIT");
    }
    ///
    /// Rollback the transaction. MUST never throw!!!
    ///
    virtual void rollback_impl() 
    {
        try {
            fast_exec("ROLLBACK");
        }
        catch(...) {
        }
    }
    ///
    /// Create a prepared statement \a q. May throw if preparation had failed.
    /// Should never return null value.
    ///
    virtual backend::statement_ptr prepare_statement_impl(const string_ref& q)
    {
        return backend::statement_ptr(new prep::statement(q, conn_, sm_));
    }
    virtual backend::statement_ptr create_statement_impl(const string_ref& q)
    {
        return backend::statement_ptr(new unprep::statement(q, conn_, sm_));
    }
    ///
    /// Escape a string for inclusion in SQL query. May throw not_supported_by_backend() if not supported by backend.
    ///
    virtual std::string escape(std::string const &s) 
    {
        return escape(s.c_str(),s.c_str()+s.size());
    }
    ///
    /// Escape a string for inclusion in SQL query. May throw not_supported_by_backend() if not supported by backend.
    ///
    virtual std::string escape(char const *s)
    {
        return escape(s,s+strlen(s));
    }
    ///
    /// Escape a string for inclusion in SQL query. May throw not_supported_by_backend() if not supported by backend.
    ///
    virtual std::string escape(char const *b,char const *e) 
    {
        std::vector<char> buf(2*(e-b)+1);
        size_t len = mysql_real_escape_string(conn_,&buf.front(),b,e-b);
        std::string result;
        result.assign(&buf.front(),len);
        return result;
    }
    ///
    /// Get the name of the driver, for example sqlite3, odbc
    ///
    virtual const std::string& engine() 
    {
        return g_backend_and_engine;
    }
    ///
    /// Get the name of the SQL Server, for example sqlite3, mssql, oracle, differs from driver() when
    /// the backend supports multiple databases like odbc backend.
    ///
    virtual const std::string& backend()
    {
        return g_backend_and_engine;
    }
    virtual void version(int& major, int& minor)
    {
        unsigned long v = mysql_get_server_version(conn_);
        major = v/10000;
        minor = (v/100) % 100;
    }
    virtual const std::string& description()
    {
        return description_;
    }

    // API

private:
    ///
    /// Set a custom MYSQL option on the connection.
    ///
    void mysql_set_option(mysql_option option, const void* arg)
    {
        // char can be casted to void but not the other way, support older API
        if(mysql_options(conn_, option, reinterpret_cast<char const *>(arg))) {
            throw edba_error("edba::mysql failed to set option");
        }
    }

    MYSQL *conn_;
    std::string description_;
};

}} // edba, backend

extern "C" {

EDBA_DRIVER_API edba::backend::connection *edba_mysql_get_connection(const edba::conn_info& cs, edba::session_monitor* sm)
{
    return new edba::mysql_backend::connection(cs, sm);
}

}
