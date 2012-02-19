#define EDBA_DRIVER_SOURCE

#include <edba/backend.hpp>
#include <edba/errors.hpp>
#include <edba/utils.hpp>

#include <libpq-fe.h>
#include <libpq/libpq-fs.h>

#include <boost/algorithm/string/predicate.hpp>

#include <sstream>
#include <vector>
#include <limits>
#include <iomanip>

#include <stdlib.h>
#include <string.h>

namespace edba { namespace postgresql {	

const std::string g_backend("PgSQL");
const std::string g_engine("PgSQL");

long long atoll(const char* val) 
{
#ifdef _WIN32 
    return _strtoi64(val, 0, 10);
#else
    return ::atoll(val);
#endif
}

typedef enum {
    lo_type,
    bytea_type
} blob_type;

class pqerror : public edba_error {
public:
    pqerror(char const *msg) : edba_error(message(msg)) {}
    pqerror(PGresult *r,char const *msg) : edba_error(message(msg,r)) {}
    pqerror(PGconn *c,char const *msg) : edba_error(message(msg,c)) {}

    static std::string message(char const *msg)
    {
        return std::string("edba::posgresql: ") + msg;
    }
    static std::string message(char const *msg,PGresult *r)
    {
        std::string m="edba::posgresql: ";
        m+=msg;
        m+=": ";
        m+=PQresultErrorMessage(r);
        return m;
    }
    static std::string message(char const *msg,PGconn *c)
    {
        std::string m="edba::posgresql: ";
        m+=msg;
        m+=": ";
        m+=PQerrorMessage(c);
        return m;
    }
};

class result : public backend::result {
public:
    result(PGresult *res,PGconn *conn,blob_type b) :
      res_(res),
      conn_(conn),
      rows_(PQntuples(res)),
      cols_(PQnfields(res)),
      current_(-1),
      blob_(b)
    {
    }
    virtual ~result() 
    {
        PQclear(res_);
    }
    virtual next_row has_next()
    {
        if(current_ + 1 < rows_)
            return next_row_exists;
        else
            return last_row_reached; 
    }
    virtual bool next() 
    {
        ++current_;
        return current_ < rows_;
    }

    template<typename T>
    bool do_fetch(int col,T& v)
    {
        if(do_isnull(col))
            return false;
        
        parse_number(chptr_range(PQgetvalue(res_,current_,col), PQgetlength(res_,current_,col)), v);

        return true;
    }
    virtual bool fetch(int col,short &v)
    {
        return do_fetch(col,v);
    }
    virtual bool fetch(int col,unsigned short &v)
    {
        return do_fetch(col,v);
    }
    virtual bool fetch(int col,int &v)
    {
        return do_fetch(col,v);
    }
    virtual bool fetch(int col,unsigned &v)
    {
        return do_fetch(col,v);
    }
    virtual bool fetch(int col,long &v)
    {
        return do_fetch(col,v);
    }
    virtual bool fetch(int col,unsigned long &v)
    {
        return do_fetch(col,v);
    }
    virtual bool fetch(int col,long long &v)
    {
        return do_fetch(col,v);
    }
    virtual bool fetch(int col,unsigned long long &v)
    {
        return do_fetch(col,v);
    }
    virtual bool fetch(int col,float &v)
    {
        return do_fetch(col,v);
    }
    virtual bool fetch(int col,double &v)
    {
        return do_fetch(col,v);
    }
    virtual bool fetch(int col,long double &v)
    {
        return do_fetch(col,v);
    }
    virtual bool fetch(int col,std::string &v)
    {
        if(do_isnull(col))
            return false;
        v.assign(PQgetvalue(res_,current_,col),PQgetlength(res_,current_,col));
        return true;
    }
    virtual bool fetch(int col,std::ostream &v)
    {
        if(do_isnull(col))
            return false;
        if(blob_ == bytea_type) {
            unsigned char *val=(unsigned char*)PQgetvalue(res_,current_,col);
            size_t len = 0;
            unsigned char *buf=PQunescapeBytea(val,&len);
            if(!buf) {
                throw bad_value_cast();
            }
            try {
                v.write((char *)buf,len);
            }catch(...) {
                PQfreemem(buf);
                throw;
            }
            PQfreemem(buf);
        }
        else { // oid
            Oid id = 0;
            fetch(col,id);
            if(id==0) {
                throw pqerror("fetching large object failed, oid=0");
            }
            int fd = -1;
            try {
                fd = lo_open(conn_,id,INV_READ | INV_WRITE);
                if(fd < 0)
                    throw pqerror(conn_,"Failed opening large object for read");
                char buf[4096];
                for(;;) {
                    int n=lo_read(conn_,fd,buf,sizeof(buf));
                    if(n < 0)
                        throw pqerror(conn_,"Failed reading large object");
                    if(n>=0)
                        v.write(buf,n);
                    if(n < int(sizeof(buf)))
                        break;
                }
                int r = lo_close(conn_,fd);
                fd = -1;
                if(r < 0)
                    throw pqerror(conn_,"error on close of large object");
            }
            catch(...) {
                if(fd != -1)
                    lo_close(conn_,fd);
                throw;
            }
        }
        return true;
    }
    virtual bool fetch(int col,std::tm &v)
    {
        if(do_isnull(col))
            return false;
        v=parse_time(PQgetvalue(res_,current_,col));
        return true;
    }
    virtual bool is_null(int col)
    {
        return do_isnull(col);
    }
    virtual int cols() 
    {
        return cols_;
    }
    virtual unsigned long long rows() 
    {
        return unsigned long long(PQntuples(res_));
    }
    virtual int name_to_column(const chptr_range& n) 
    {
        return PQfnumber(res_, boost::copy_range<std::string>(n).c_str());
    }
    virtual std::string column_to_name(int pos)
    {
        char const *name = PQfname(res_,pos);
        if(!name)
            return std::string();
        return name;
    }
private:

    void check(int c)
    {
        if(c < 0 || c>= cols_)
            throw invalid_column();
    }
    bool do_isnull(int col)
    {
        check(col);
        return PQgetisnull(res_,current_,col) ? true : false;
    }

    PGresult *res_;
    PGconn *conn_;
    int rows_;
    int cols_;
    int current_;
    blob_type blob_;
};

class statement : public backend::statement {
public:

    typedef enum {
        null_param,
        text_param,
        binary_param
    } param_type;

    statement(PGconn *conn, const chptr_range& src_query,blob_type b,unsigned long long prepared_id, session_monitor* sm) :
        backend::statement(sm, src_query),
        res_(0),
        conn_(conn),
        params_(0),
        blob_(b)
    {
        std::ostringstream ss;
        ss.imbue(std::locale::classic());

        bool inside_string=false;
        for(unsigned i=0;i<(unsigned)src_query.size();i++) {
            char c=src_query[i];
            if(c=='\'') {
                inside_string = !inside_string;
            }
            if(!inside_string && c=='?') {
                query_+='$';
                params_++;
                ss<<params_;
                query_+=ss.str();
                ss.str("");
            }
            else {
                query_+=c;
            }
        }
        reset();

        if(prepared_id > 0) 
        {
            ss.str("");
            ss<<"edba_psqlstmt_" << prepared_id;
            prepared_id_ = ss.str();

            PGresult *r=PQprepare(conn_,prepared_id_.c_str(),query_.c_str(),0,0);
            try {
                if(!r) {
                    throw pqerror("Failed to create prepared statement object!");
                }
                if(PQresultStatus(r)!=PGRES_COMMAND_OK)
                    throw pqerror(r,"statement preparation failed");
            }
            catch(...) {
                if(r) PQclear(r);
                throw;
            }
            PQclear(r);
        }
    }
    virtual ~statement()
    {
        try {
            if(res_) {
                PQclear(res_);
                res_ = 0;
            }
            if(!prepared_id_.empty()) {
                std::string stmt = "DEALLOCATE " + prepared_id_;
                res_ = PQexec(conn_,stmt.c_str());
                if(res_)  {
                    PQclear(res_);
                    res_ = 0;
                }
            }
        }
        catch(...) 
        {
        }
    }
    virtual void reset_impl()
    {
        if(res_) {
            PQclear(res_);
            res_ = 0;
        }
        std::vector<std::string> vals(params_);
        std::vector<size_t> lengths(params_,0);
        std::vector<char const *> pvals(params_,0);
        std::vector<param_type> flags(params_,null_param);
        params_values_.swap(vals);
        params_pvalues_.swap(pvals);
        params_plengths_.swap(lengths);
        params_set_.swap(flags);
    }
    virtual void bind_impl(int col, const chptr_range& rng)
    {
        check(col);
        params_pvalues_[col-1] = rng.begin();
        params_plengths_[col-1] = rng.size();
        params_set_[col-1]=text_param;
    }
    virtual void bind_impl(int col,std::tm const &v) 
    {
        check(col);
        params_values_[col-1]=format_time(v);
        params_set_[col-1]=text_param;
    }
    virtual void bind_impl(int col,std::istream &in)
    {
        check(col);
        if(blob_ == bytea_type) {
            std::ostringstream ss;
            ss << in.rdbuf();
            params_values_[col-1]=ss.str();
            params_set_[col-1]=binary_param;
        }
        else {
            Oid id = 0;
            int fd = -1;
            try {
                id = lo_creat(conn_, INV_READ|INV_WRITE);
                if(id == 0)
                    throw pqerror(conn_,"failed to create large object");
                fd = lo_open(conn_,id,INV_READ | INV_WRITE);
                if(fd < 0)
                    throw pqerror(conn_,"failed to open large object for writing");
                char buf[4096];
                for(;;) {
                    in.read(buf,sizeof(buf));
                    std::streamsize bytes_read = in.gcount();
                    if(bytes_read > 0) {
                        int n = lo_write(conn_,fd,buf,(size_t)bytes_read);
                        if(n < 0) {
                            throw pqerror(conn_,"failed writing to large object");
                        }
                    }
                    if(bytes_read < int(sizeof(buf)))
                        break;
                }
                int r = lo_close(conn_,fd);
                fd=-1;
                if(r < 0)
                    throw pqerror(conn_,"error closing large object after write");
                bind(col,id);
            }
            catch(...) {
                if(fd<-1)
                    lo_close(conn_,fd);
                if(id!=0)
                    lo_unlink(conn_,id);
                throw;
            }
        }
    }

    template<typename T>
    void do_bind(int col,T v)
    {
        check(col);
        std::ostringstream ss;
        ss.imbue(std::locale::classic());
        if(!std::numeric_limits<T>::is_integer)
            ss << std::setprecision(std::numeric_limits<T>::digits10+1);
        ss << v;
        params_values_[col-1]=ss.str();
        params_set_[col-1]=text_param;
    }

    virtual void bind_impl(int col,int v)
    {
        do_bind(col,v);
    }
    virtual void bind_impl(int col,unsigned v)
    {
        do_bind(col,v);
    }
    virtual void bind_impl(int col,long v)
    {
        do_bind(col,v);
    }
    virtual void bind_impl(int col,unsigned long v)
    {
        do_bind(col,v);
    }
    virtual void bind_impl(int col,long long v)
    {
        do_bind(col,v);
    }
    virtual void bind_impl(int col,unsigned long long v)
    {
        do_bind(col,v);
    }
    virtual void bind_impl(int col,double v)
    {
        do_bind(col,v);
    }
    virtual void bind_impl(int col,long double v)
    {
        do_bind(col,v);
    }
    virtual void bind_null_impl(int col)
    {
        check(col);
        params_set_[col-1]=null_param;
        std::string tmp;
        params_values_[col-1].swap(tmp);
    }

    void real_query()
    {
        char const * const *pvalues = 0;
        int *plengths = 0;
        int *pformats = 0;
        std::vector<char const *> values;
        std::vector<int> lengths;
        std::vector<int> formats;
        if(params_>0) {
            values.resize(params_,0);
            lengths.resize(params_,0);
            formats.resize(params_,0);
            for(unsigned i=0;i<params_;i++) {
                if(params_set_[i]!=null_param) {
                    if(params_pvalues_[i]!=0) {
                        values[i]=params_pvalues_[i];
                        lengths[i]=params_plengths_[i];
                    }
                    else {
                        values[i]=params_values_[i].c_str();
                        lengths[i]=params_values_[i].size();
                    }
                    if(params_set_[i]==binary_param) {
                        formats[i]=1;
                    }
                }
            }
            pvalues=&values.front();
            plengths=&lengths.front();
            pformats=&formats.front();
        }
        if(res_) {
            PQclear(res_);
            res_ = 0;
        }
        if(prepared_id_.empty()) {
            res_ = PQexecParams(
                conn_,
                query_.c_str(),
                params_,
                0, // param types
                pvalues,
                plengths,
                pformats, // format - text
                0 // result format - text
                );
        }
        else {
            res_ = PQexecPrepared(
                conn_,
                prepared_id_.c_str(),
                params_,
                pvalues,
                plengths,
                pformats, // format - text
                0 // result format - text
                );
        }
    }

    virtual boost::intrusive_ptr<backend::result> query_impl() 
    {
        real_query();
        switch(PQresultStatus(res_))
        {
        case PGRES_TUPLES_OK:
        {
            boost::intrusive_ptr<result> ptr(new result(res_,conn_,blob_));
            res_ = 0;
            return ptr;
        }
    break;
        case PGRES_COMMAND_OK:
            throw pqerror("Statement used instread of query");
            break;
        default:
            throw pqerror(res_,"query execution failed ");
        }
    }
    virtual void exec_impl() 
    {
        real_query();
        switch(PQresultStatus(res_))
        {
        case PGRES_TUPLES_OK:
            throw pqerror("Query used instread of statement");
            break;
        case PGRES_COMMAND_OK:
            break;
        default:
            throw pqerror(res_,"statement execution failed ");
        }
    }
    virtual long long sequence_last(std::string const &sequence)
    {
        PGresult *res = 0;
        long long rowid;
        try {
            if (sequence.empty()) 
                res = PQexec( conn_, "SELECT lastval()" );
            else 
            {
                char const * const param_ptr = sequence.c_str();
                res = PQexecParams(	conn_,
                    "SELECT currval($1)",
                    1, // 1 param
                    0, // types
                    &param_ptr, // param values
                    0, // lengths
                    0, // formats
                    0 // string format
                    );
            }

            if(PQresultStatus(res) != PGRES_TUPLES_OK) {
                throw pqerror(res,"failed to fetch last sequence id");
            }
            char const *val = PQgetvalue(res,0,0);
            if(!val || *val==0)
                throw pqerror("Failed to get value for sequecne id");
#ifdef _WIN32
            rowid = _strtoi64(val, 0, 10);
#else
            rowid = atoll(val);
#endif

        }
        catch(...) {
            if(res) PQclear(res);
            throw;
        }
        PQclear(res);
        return rowid;
    }
    virtual unsigned long long affected() 
    {
        if(res_) {
            char const *s=PQcmdTuples(res_);
            if(!s || !*s)
                return 0;
            return atoll(s);
        }
        return 0;
    }
 
private:
    void check(int col)
    {
        if(col < 1 || col > int(params_))
            throw invalid_placeholder();
    }
    PGresult *res_;
    PGconn *conn_;

    std::string query_;
    unsigned params_;
    std::vector<std::string> params_values_;
    std::vector<char const *> params_pvalues_;
    std::vector<size_t> params_plengths_;
    std::vector<param_type> params_set_;
    std::string prepared_id_;
    blob_type blob_;
};

class connection : public backend::connection {
public:
    void do_simple_exec(char const *s)
    {
        PGresult *r=PQexec(conn_,s);
        PQclear(r);
    }
    virtual void begin_impl()
    {
        do_simple_exec("begin");
    }
    virtual void commit_impl() 
    {
        do_simple_exec("commit");
    }
    virtual void rollback_impl()
    {
        try {
            do_simple_exec("rollback");
        }
        catch(...) {}
    }
    virtual boost::intrusive_ptr<backend::statement> prepare_statement(const chptr_range& q)
    {
        return boost::intrusive_ptr<backend::statement>(new statement(conn_,q,blob_,++prepared_id_, sm_));
    }
    virtual boost::intrusive_ptr<backend::statement> create_statement(const chptr_range& q)
    {
        return boost::intrusive_ptr<backend::statement>(new statement(conn_,q,blob_,0, sm_));
    }
    virtual void exec_batch_impl(const chptr_range& q)
    {
        if (expand_conditionals_) 
            do_simple_exec(q.begin());
        else
        {
            // copy whole string to make it null terminated :(
            std::string tmp(q.begin(), q.end());
            do_simple_exec(tmp.c_str());
        }
    }
    std::string do_escape(char const *b,size_t length)
    {
        std::vector<char> buf(2*length+1);
        size_t len = PQescapeStringConn(conn_,&buf.front(),b,length,0);
        return std::string(&buf.front(),len);
    }
    virtual std::string escape(std::string const &s)
    {
        return do_escape(s.c_str(),s.size());
    }
    virtual std::string escape(char const *s)
    {
        return do_escape(s,strlen(s));
    }
    virtual std::string escape(char const *b,char const *e) 
    {
        return do_escape(b,e-b);
    }

    connection(conn_info const &ci, session_monitor* sm) :
        backend::connection(ci, sm),
        conn_(0),
        prepared_id_(0)
    {
        std::string pq = ci.pgsql_conn_string();
        chptr_range blob = ci.get("@blob","lo");

        if(boost::algorithm::iequals(blob, "bytea"))
            blob_ = bytea_type;
        else if(boost::algorithm::iequals(blob, "lo"))
            blob_ = lo_type;
        else 
            throw pqerror("@blob property should be either lo or bytea");

        conn_ = 0;
        try {
            conn_ = PQconnectdb(pq.c_str());
            if(!conn_)
                throw pqerror("failed to create connection object");
            if(PQstatus(conn_)!=CONNECTION_OK)
                throw pqerror(conn_, "failed to connect ");
        }
        catch(...) {
            if(conn_) {
                PQfinish(conn_);
                conn_ = 0;
            }

            throw;
        }

        // prepare human readeable description
        int major;
        int minor;
        version(major, minor);
        char buf[256];
#ifdef _WIN32
        _snprintf_s(buf, 256, "PostgreSQL version %d.%d, user is '%s'", major, minor, PQuser(conn_));
#else
        snprintf(buf, 256, "PostgreSQL version %d.%d, user is '%s'", major, minor, PQuser(conn_));
#endif                
        description_ = buf;

    }
    virtual ~connection()
    {
        PQfinish(conn_);
    }
    virtual const std::string& backend()
    {
        return g_backend;
    }
    virtual const std::string& engine()
    {
        return g_engine;
    }
    virtual void version(int& major, int& minor)
    {
        int full_ver = PQserverVersion(conn_);
        major = full_ver / 10000;
        minor = (full_ver / 100) % 100;
    }
    virtual const std::string& description()
    {
        return description_;
    }
private:
    PGconn *conn_;
    unsigned long long prepared_id_;
    blob_type blob_;
    std::string description_;
};

}} // namespace edba, backend


extern "C" {
    EDBA_DRIVER_API edba::backend::connection *edba_postgres_get_connection(const edba::conn_info& cs, edba::session_monitor* sm)
    {
        return new edba::postgresql::connection(cs, sm);
    }
}
