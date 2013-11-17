#include <edba/backend/bind_by_name_helper.hpp>
#include <edba/detail/utils.hpp>

#include <edba/errors.hpp>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/scope_exit.hpp>
#include <boost/foreach.hpp>

#include <sstream>
#include <vector>
#include <limits>
#include <iomanip>

#include <libpq-fe.h>
#include <libpq/libpq-fs.h>

#include <stdlib.h>
#include <string.h>

namespace edba { namespace backend { namespace postgres { namespace {

const std::string g_backend("PgSQL");
const std::string g_engine("PgSQL");

const int BYTEA_IDENTIFIER_TYPE = 17;
const int OID_IDENTIFIER_TYPE = 26;

typedef enum {
    lo_type,
    bytea_type
} blob_type;

/// Data owned by connection object by also required by statement object
struct common_data
{
    common_data() : conn_(0), inside_transaction_(false), blob_(bytea_type) {}

    PGconn* conn_;
    bool inside_transaction_;
    blob_type blob_;
};

void emptyNoticeProcessor(void *, const char *)
{
}

class pqerror : public edba_error
{
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

class result : public backend::result, public boost::static_visitor<>
{
public:
    result(PGresult *res, PGconn *conn) :
      res_(res),
      conn_(conn),
      rows_(PQntuples(res)),
      cols_(PQnfields(res)),
      current_(-1)
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

    virtual bool fetch(int col, const fetch_types_variant& v)
    {
        if(do_isnull(col))
            return false;

        fetch_col_ = col;

        v.apply_visitor(*this);
        return true;
    }

    template<typename T>
    void operator()(T* v, typename boost::enable_if< boost::is_arithmetic<T> >::type* = 0)
    {
        parse_number(string_ref(PQgetvalue(res_, current_, fetch_col_), PQgetlength(res_, current_, fetch_col_)), *v);
    }

    void operator()(std::string* v)
    {
        v->assign(PQgetvalue(res_, current_, fetch_col_),PQgetlength(res_, current_, fetch_col_));
    }

    void operator()(std::ostream* v)
    {
        switch(PQftype(res_, fetch_col_))
        {
            case BYTEA_IDENTIFIER_TYPE: {
                unsigned char *val = (unsigned char*)PQgetvalue(res_, current_, fetch_col_);
                size_t len = 0;

                unsigned char *buf = PQunescapeBytea(val, &len);
                if(!buf)
                    throw bad_value_cast();

                BOOST_SCOPE_EXIT((buf)) {
                    PQfreemem(buf);
                } BOOST_SCOPE_EXIT_END

                v->write((char *)buf,len);
                break;
            }
            case OID_IDENTIFIER_TYPE: {
                Oid id = 0;
                this->operator()(&id);

                if(id == 0)
                    throw pqerror("fetching large object failed, oid=0");

                int fd = lo_open(conn_, id, INV_READ | INV_WRITE);

                if(fd < 0)
                    throw pqerror(conn_, "Failed opening large object for read");

                BOOST_SCOPE_EXIT((conn_)(fd)) {
                    lo_close(conn_, fd);
                } BOOST_SCOPE_EXIT_END

                char buf[4096];
                for(;;)
                {
                    int n = lo_read(conn_, fd, buf, sizeof(buf));
                    if(n < 0)
                        throw pqerror(conn_, "Failed reading large object");

                    if(n >= 0)
                        v->write(buf,n);

                    if(n < int(sizeof(buf)))
                        break;
                }

                break;
            }
            default: {
                v->write(PQgetvalue(res_, current_, fetch_col_), PQgetlength(res_, current_, fetch_col_));
            }
        }
    }

    void operator()(std::tm* v)
    {
        *v = parse_time(PQgetvalue(res_, current_, fetch_col_));
    }

    virtual bool is_null(int col)
    {
        return do_isnull(col);
    }

    virtual int cols()
    {
        return cols_;
    }

    virtual boost::uint64_t rows()
    {
        return boost::uint64_t(PQntuples(res_));
    }

    virtual int name_to_column(const string_ref& n)
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
            throw invalid_column(c);
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
    int fetch_col_;
};

class statement : public backend::bind_by_name_helper, public boost::static_visitor<>
{
public:

    typedef enum {
        null_param,
        text_param,
        binary_param
    } param_type;

    static void do_simple_exec(PGconn* conn, char const *s)
    {
        PGresult* r = PQexec(conn, s);

        BOOST_SCOPE_EXIT((r))
        {
            PQclear(r);
        } BOOST_SCOPE_EXIT_END

        switch(PQresultStatus(r))
        {
        case PGRES_COMMAND_OK:
        case PGRES_EMPTY_QUERY:
        case PGRES_TUPLES_OK:
        case PGRES_COPY_OUT:
        case PGRES_COPY_IN:
        case PGRES_NONFATAL_ERROR:
            break;
        default:
            throw pqerror(r, "PQexec failed");
        }
    }

    statement(const common_data* data, const string_ref& src_query, unsigned long long prepared_id, session_stat* stat)
      : backend::bind_by_name_helper(stat, src_query, backend::postgresql_style_marker())
      , data_(data)
      , res_(0)
      , params_values_(bindings_count())
      , params_pvalues_(bindings_count(), 0)
      , params_plengths_(bindings_count(), 0)
      , params_set_(bindings_count(), null_param)
    {
        std::ostringstream ss;
        ss.imbue(std::locale::classic());

        if(prepared_id > 0)
        {
            ss << "edba_psqlstmt_" << prepared_id;
            prepared_id_ = ss.str();

            PGresult* r = PQprepare(data_->conn_, prepared_id_.c_str(), patched_query().c_str(), 0, 0);

            if(!r)
                throw pqerror("Failed to create prepared statement object!");

            BOOST_SCOPE_EXIT((r))
            {
                PQclear(r);
            } BOOST_SCOPE_EXIT_END

            if(PQresultStatus(r) != PGRES_COMMAND_OK)
                throw pqerror(r,"statement preparation failed");
        }
    }

    virtual ~statement()
    {
        try
        {
            if(res_)
            {
                PQclear(res_);
                res_ = 0;
            }

            if(!prepared_id_.empty())
            {
                std::string stmt = "DEALLOCATE " + prepared_id_;
                res_ = PQexec(data_->conn_, stmt.c_str());

                if(res_)
                {
                    PQclear(res_);
                    res_ = 0;
                }
            }
        }
        catch(...)
        {
        }
    }

    virtual void reset_bindings_impl()
    {
        if(res_)
        {
            PQclear(res_);
            res_ = 0;
        }

        params_values_.resize(bindings_count());
        BOOST_FOREACH(std::string& s, params_values_)
            s.clear();

        params_pvalues_.assign(bindings_count(), 0);
        params_plengths_.assign(bindings_count(), 0);
        params_set_.assign(bindings_count(), null_param);
    }

    virtual void bind_impl(int col, bind_types_variant const& v)
    {
        check(col);
        bind_col_ = col;

        v.apply_visitor(*this);
    }

    template<typename T>
    void operator()(T v, typename boost::enable_if< boost::is_arithmetic<T> >::type* = 0)
    {
        std::ostringstream ss;
        ss.imbue(std::locale::classic());
        if(!std::numeric_limits<T>::is_integer)
            ss << std::setprecision(std::numeric_limits<T>::digits10+1);
        ss << v;
        params_values_[bind_col_ - 1] = ss.str();
        params_set_[bind_col_ - 1] = text_param;
    }

    void operator()(const string_ref& v)
    {
        params_pvalues_[bind_col_ - 1] = v.begin();
        params_plengths_[bind_col_ - 1] = v.size();
        params_set_[bind_col_ - 1] = text_param;
    }

    void operator()(const std::tm& v)
    {
        params_values_[bind_col_ - 1] = format_time(v);
        params_set_[bind_col_ - 1] = text_param;
    }

    void operator()(std::istream* in)
    {
        if(data_->blob_ == bytea_type)
        {
            std::ostringstream ss;
            ss << in->rdbuf();
            params_values_[bind_col_ - 1] = ss.str();
            params_set_[bind_col_ - 1] = binary_param;
        }
        else
        {
            Oid oid = InvalidOid;

            // All lob operations should be inside transaction
            // http://web.archiveorange.com/archive/v/KRdh2pENAWi9JrZH1bke
            if (!data_->inside_transaction_)
                do_simple_exec(data_->conn_, "begin");

            BOOST_SCOPE_EXIT((data_))
            {
                if (!data_->inside_transaction_)
                {
                    const char* query = std::uncaught_exception() ? "rollback" : "commit";
                    try { do_simple_exec(data_->conn_, query); }
                    catch(...) { }
                }
            } BOOST_SCOPE_EXIT_END

            try
            {
                oid = lo_creat(data_->conn_, INV_READ | INV_WRITE);
                if(InvalidOid == oid)
                    throw pqerror(data_->conn_, "failed to create large object");

                int fd = lo_open(data_->conn_, oid, INV_WRITE);
                if(fd < 0)
                    throw pqerror(data_->conn_, "failed to open large object for writing");

                BOOST_SCOPE_EXIT((data_)(fd))
                {
                    lo_close(data_->conn_, fd);
                } BOOST_SCOPE_EXIT_END

                char buf[4096];
                for(;;)
                {
                    in->read(buf, sizeof(buf));
                    std::streamsize bytes_read = in->gcount();
                    if(bytes_read > 0)
                    {
                        int n = lo_write(data_->conn_, fd, buf, (size_t)bytes_read);
                        if(n < 0)
                            throw pqerror(data_->conn_, "failed writing to large object");
                    }
                    if(bytes_read < int(sizeof(buf)))
                        break;
                }

                bind(bind_col_, oid);
            }
            catch(...) {
                if(oid != InvalidOid)
                    lo_unlink(data_->conn_, oid);
                throw;
            }
        }
    }

    void operator()(null_type)
    {
        params_set_[bind_col_ - 1] = null_param;
        std::string tmp;
        params_values_[bind_col_ - 1].swap(tmp);
    }

    void real_query()
    {
        char const * const *pvalues = 0;
        int *plengths = 0;
        int *pformats = 0;
        std::vector<char const *> values;
        std::vector<int> lengths;
        std::vector<int> formats;
        if(bindings_count() > 0)
        {
            values.resize(bindings_count(),0);
            lengths.resize(bindings_count(),0);
            formats.resize(bindings_count(),0);
            for(unsigned i=0; i<bindings_count(); i++)
            {
                if(params_set_[i]!=null_param)
                {
                    if(params_pvalues_[i]!=0)
                    {
                        values[i]=params_pvalues_[i];
                        lengths[i]=params_plengths_[i];
                    }
                    else
                    {
                        values[i]=params_values_[i].c_str();
                        lengths[i]=params_values_[i].size();
                    }

                    if(params_set_[i]==binary_param)
                        formats[i]=1;
                }
            }
            pvalues=&values.front();
            plengths=&lengths.front();
            pformats=&formats.front();
        }

        if(res_)
        {
            PQclear(res_);
            res_ = 0;
        }

        if(prepared_id_.empty()) {
            res_ = PQexecParams(
                data_->conn_,
                patched_query().c_str(),
                bindings_count(),
                0, // param types
                pvalues,
                plengths,
                pformats, // format - text
                0 // result format - text
                );
        }
        else {
            res_ = PQexecPrepared(
                data_->conn_,
                prepared_id_.c_str(),
                bindings_count(),
                pvalues,
                plengths,
                pformats, // format - text
                0 // result format - text
                );
        }
    }

    virtual backend::result_ptr query_impl()
    {
        real_query();
        switch(PQresultStatus(res_))
        {
        case PGRES_TUPLES_OK:
        {
            boost::intrusive_ptr<result> ptr(new result(res_, data_->conn_));
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
            throw pqerror("Query used instead of statement");
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
                res = PQexec(data_->conn_, "SELECT lastval()");
            else
            {
                char const * const param_ptr = sequence.c_str();
                res = PQexecParams(data_->conn_,
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

            rowid = atoll(val);
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
        if(res_)
        {
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
        if(col < 1 || col > int(bindings_count()))
            throw invalid_placeholder();
    }

    const common_data* data_;
    PGresult *res_;
    std::string prepared_id_;

    std::vector<std::string> params_values_;
    std::vector<char const *> params_pvalues_;
    std::vector<size_t> params_plengths_;
    std::vector<param_type> params_set_;
    int bind_col_;
};

class connection : public backend::connection, private common_data
{
public:
    connection(conn_info const &ci, session_monitor* sm) :
        backend::connection(ci, sm),
        prepared_id_(0)
    {
        std::string pq = ci.pgsql_conn_string();
        string_ref blob = ci.get("@blob","bytea");

        if(boost::algorithm::iequals(blob, "bytea"))
            blob_ = bytea_type;
        else if(boost::algorithm::iequals(blob, "lo"))
            blob_ = lo_type;
        else
            throw pqerror("@blob property should be either lo or bytea");

        try 
        {
            conn_ = PQconnectdb(pq.c_str());
            if(!conn_)
                throw pqerror("failed to create connection object");
            if(PQstatus(conn_)!=CONNECTION_OK)
                throw pqerror(conn_, "failed to connect ");
        }
        catch(...) 
        {
            if(conn_) {
                PQfinish(conn_);
                conn_ = 0;
            }

            throw;
        }

        // Get rid of spam in stderr.
        // TODO: Probably it should be forwarded to session monitor
        PQsetNoticeProcessor(conn_, &emptyNoticeProcessor, 0);

        // prepare human readable description
        int major;
        int minor;
        version(major, minor);
        char buf[256];
        EDBA_SNPRINTF(buf, 256, "PostgreSQL version %d.%d, user is '%s'", major, minor, PQuser(conn_));
        description_ = buf;
    }

    virtual ~connection()
    {
        PQfinish(conn_);
    }

    virtual void begin_impl()
    {
        statement::do_simple_exec(conn_, "begin");
        inside_transaction_ = true;
    }
    virtual void commit_impl()
    {
        statement::do_simple_exec(conn_, "commit");
        inside_transaction_ = false;
    }
    virtual void rollback_impl()
    {
        try {
            statement::do_simple_exec(conn_, "rollback");
        }
        catch(...) {}
        inside_transaction_ = false;
    }
    virtual backend::statement_ptr prepare_statement_impl(const string_ref& q)
    {
        return backend::statement_ptr(new statement(this,q,++prepared_id_, &stat_));
    }
    virtual backend::statement_ptr create_statement_impl(const string_ref& q)
    {
        return backend::statement_ptr(new statement(this,q,0, &stat_));
    }
    virtual void exec_batch_impl(const string_ref& q)
    {
        if (expand_conditionals_)
            statement::do_simple_exec(conn_, q.begin());
        else
        {
            // copy whole string to make it null terminated :(
            std::string tmp(q.begin(), q.end());
            statement::do_simple_exec(conn_, tmp.c_str());
        }
    }

    virtual std::string escape(const string_ref& s)
    {
        std::vector<char> buf(2 * s.size() + 1);
        size_t len = PQescapeStringConn(conn_, &buf.front(), s.begin(), s.size(), 0);
        return std::string(&buf.front(), len);
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
    unsigned long long prepared_id_;
    std::string description_;
};

}}}} // namespace edba, backend, postgres, anonymous


extern "C" {
    EDBA_DRIVER_API edba::backend::connection *edba_postgresql_get_connection(const edba::conn_info& cs, edba::session_monitor* sm)
    {
        return new edba::backend::postgres::connection(cs, sm);
    }
}
