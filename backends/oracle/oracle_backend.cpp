#include <edba/backend/implementation_base.hpp>

#include <edba/errors.hpp>

#include <oci.h>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/find_iterator.hpp>
#include <boost/typeof/typeof.hpp>
#include <boost/scoped_array.hpp>
#include <boost/type_traits/is_signed.hpp>
#include <boost/type_traits/is_unsigned.hpp>
#include <boost/type_traits/is_floating_point.hpp>
#include <boost/move/move.hpp>
#include <boost/container/vector.hpp>

#include <boost/foreach.hpp>

#include <sstream>
#include <vector>

namespace {
  
#if defined( _WIN32 )
  
struct oracle_driver_initializer_t 
{
    oracle_driver_initializer_t() 
    {
        _putenv( "NLS_LANG=.AL32UTF8" );
    }

    ~oracle_driver_initializer_t() 
    {
        _putenv( "NLS_LANG=" );
    }
} oracle_driver_initializer;
  
#else 
  
struct oracle_driver_initializer_t 
{
    oracle_driver_initializer_t() 
    {
        setenv( "NLS_LANG", ".AL32UTF8", 1 );
    }

    ~oracle_driver_initializer_t() 
    {
        unsetenv( "NLS_LANG" );
    }
} oracle_driver_initializer;
  
#endif
  
}

namespace edba { namespace oracle_backend {

static std::string g_backend_name("oracle");
static std::string g_engine_name("oracle");

typedef sword (*deallocator_type)( void *hndlp, ub4 type );
  
template <class T, ub4 HandleType, deallocator_type Deallocator = &OCIHandleFree> 
class oci_handle 
{
    struct proxy;

    BOOST_MOVABLE_BUT_NOT_COPYABLE(oci_handle)

public:
    oci_handle() : handle_(0) {}
    explicit oci_handle(T* handle) : handle_(handle) {}

    // move constructor
    oci_handle(BOOST_RV_REF(oci_handle) x) : handle_(x.handle_)
    {
        x.handle_ = 0;
    }

    oci_handle& operator=(BOOST_RV_REF(oci_handle) x) 
    {
        handle_ = x.handle_;
        x.handle_ = 0;
        return *this;
    }

    ~oci_handle() 
    {
        if(handle_)
            Deallocator( (dvoid *)handle_, HandleType );
    }

    void reset(T* handle = 0)
    {
        if(handle_)
            Deallocator( (dvoid *)handle_, HandleType );

        handle_ = handle;
    }

    T* get() const 
    {
        return handle_;
    }

    proxy ptr() 
    {
        return proxy(this);
    }

private:
    struct proxy {
        proxy(oci_handle* h) : h_(h), val_(h->get()) {}

        ~proxy() 
        {
            if (h_->get() != val_) 
                h_->reset(val_);
        }

        operator T**() 
        {
            return &val_;
        }

        operator dvoid**()
        {
            return (dvoid**)&val_;
        }

    private:
        oci_handle* h_;
        T* val_;
    };

    T* handle_;
};

typedef oci_handle<OCIEnv   , OCI_HTYPE_ENV   > oci_handle_env;
typedef oci_handle<OCIError , OCI_HTYPE_ERROR > oci_handle_error;
typedef oci_handle<OCISvcCtx, OCI_HTYPE_SVCCTX> oci_handle_service_context;
typedef oci_handle<OCIStmt  , OCI_HTYPE_STMT>   oci_handle_statement;
typedef oci_handle<OCIDefine, OCI_HTYPE_DEFINE> oci_handle_define;
typedef oci_handle<OCIDateTime, OCI_DTYPE_TIMESTAMP, &OCIDescriptorFree> oci_descriptor_datetime;
typedef oci_handle<OCIInterval, OCI_DTYPE_INTERVAL_DS, &OCIDescriptorFree> oci_descriptor_interval_ds;
typedef oci_handle<OCILobLocator, OCI_DTYPE_LOB, &OCIDescriptorFree> oci_lob;


struct error_checker
{
    error_checker() : errhp_(0) {}
    error_checker(OCIError* errhp) : errhp_(errhp) {}

    sword operator=(sword code)
    {
        if( code >= 0 ) 
            return code;

        std::ostringstream error_message;
        error_message << "oracle: ";
    
        if( OCI_ERROR == code && errhp_ )
        {
            char errbuf[1024];

            OCIErrorGet(errhp_, 1, 0, &code, (text*)errbuf, (ub4) sizeof(errbuf), OCI_HTYPE_ERROR);

            error_message << errbuf;
        }
        else if( OCI_INVALID_HANDLE == code )
            error_message << "invalid handle provided to OCI(probably internal backend bug)";
        else
            error_message << "unknown error ";
    
        error_message << "(error " << code << ')';

        throw edba_error(error_message.str());

    }

    OCIError* errhp_;
};

struct column
{
    std::string name_;
    ub2 type_;
    oci_handle_define define_;

    std::vector<char> data_;
    oci_lob lob_;

    sb2 col_fetch_ind_;
    ub2	col_fetch_size_;
    ub2	col_fetch_rcode_;

    column() : col_fetch_ind_(0), col_fetch_size_(0), col_fetch_rcode_(0) {}

    void init(OCIEnv* envhp, OCIStmt* stmtp, OCIError* errp, ub4 idx)
    {
        error_checker ec(errp);
        oci_handle< OCIParam, OCI_DTYPE_PARAM, &OCIDescriptorFree > parm;
        ++idx;

        ec = OCIParamGet(stmtp, OCI_HTYPE_STMT, errp, parm.ptr(), idx);
      
        // Get column name and remember it
        text* name = 0;
        ub4 len = 0;
      
        ec = OCIAttrGet(parm.get(), OCI_DTYPE_PARAM, &name, &len, OCI_ATTR_NAME, errp);
        name_.assign((const char*)name, len);

        // Get column max length
        ub4 col_length = 0;
        ec = OCIAttrGet(parm.get(), OCI_DTYPE_PARAM, &col_length, 0, OCI_ATTR_DATA_SIZE, errp);

        // Get column type
        ec = OCIAttrGet(parm.get(), OCI_DTYPE_PARAM, &type_, 0, OCI_ATTR_DATA_TYPE, errp);
        if (SQLT_NUM == type_)
        {
            type_ = SQLT_VNU;
        }
        else if (SQLT_TIMESTAMP     == type_ ||
                 SQLT_TIMESTAMP_TZ  == type_ ||
                 SQLT_TIMESTAMP_LTZ == type_ ||
                 SQLT_TIME          == type_ ||
                 SQLT_TIME_TZ       == type_ ||
                 SQLT_DATE          == type_)
        {
            type_ = SQLT_DAT;
        }
        else if (SQLT_LVC  == type_ ||
                 SQLT_LVB  == type_)
        {
            type_ = SQLT_STR;
        }
        
        if (SQLT_CLOB == type_ ||
            SQLT_BLOB == type_ )
        {
            ec = OCIDescriptorAlloc(envhp, lob_.ptr(), OCI_DTYPE_LOB, 0, 0);

            ec = OCIDefineByPos(
                stmtp, define_.ptr(), errp, 
                idx, 
                &lob_, sizeof(lob_), type_,
                &col_fetch_ind_, &col_fetch_size_, &col_fetch_rcode_, 
                OCI_DEFAULT 
                );
        }
        else
        {
            // Allocate data, and define output value
            data_.resize(eval_alloc_size(col_length));

            ec = OCIDefineByPos(
                stmtp, define_.ptr(), errp, 
                idx, 
                &data_[0], data_.size(), type_,
                &col_fetch_ind_, &col_fetch_size_, &col_fetch_rcode_, 
                OCI_DEFAULT 
                );
        }
    }

    bool is_null() const
    {
        return -1 == col_fetch_ind_;
    }

    bool is_lob() const
    {
        return !!lob_.get();
    }

private:
    size_t eval_alloc_size(ub4 col_length)
    {
        return col_length;
    }    
};

class result : public backend::result, public boost::static_visitor<>
{
public:
    result(OCIEnv* envhp, OCISvcCtx* svchp, OCIError* errhp, OCIStmt* stmtp)
      : envhp_(envhp)
      , svchp_(svchp)
      , stmtp_(stmtp)
      , throw_on_error_(errhp)
      , just_initialized_(true)
    {   
        // Get number of columns in result
        throw_on_error_ =  OCIAttrGet( 
            stmtp_
          , OCI_HTYPE_STMT
          , &columns_size_
          , 0
          , OCI_ATTR_PARAM_COUNT
          , errhp
          );

        // Fill columns array. Columns will allocate buffers and 
        // create defines for output parameter
        columns_.reset(new column[columns_size_]);
        for (ub4 i = 0; i < columns_size_; ++i)
            columns_[i].init(envhp, stmtp, errhp, i);
    }

    virtual next_row has_next()
    {
        return next_row_unknown;
    }
    virtual bool next() 
    {
        sword status;
        if (just_initialized_)
        {
            status = throw_on_error_ = OCIStmtFetch2(
                stmtp_, throw_on_error_.errhp_, 1, OCI_FETCH_FIRST, 0, OCI_DEFAULT
              );
        }
        else
        {
            status = throw_on_error_ = OCIStmtFetch2(
                stmtp_, throw_on_error_.errhp_, 1, OCI_FETCH_NEXT, 1, OCI_DEFAULT
              );
        }

        just_initialized_ = false;

        if (status == OCI_SUCCESS_WITH_INFO) 
        {
            // we must check all columns and find out where buffer is not enought to fit result
            for (size_t i = 0; i < columns_size_; ++i)
            {
                column& c = columns_[i];
                ub2 code = c.col_fetch_rcode_;
            }
        }

        return status != OCI_NO_DATA;
    }

    bool fetch(int col, const fetch_types_variant& v)
    {
        if (is_null(col)) 
            return false;
        
        fetch_col_ = col;

        v.apply_visitor(*this);
        return true;
    }

    template<typename T>
    void operator()(T* v, typename boost::enable_if< boost::is_integral<T> >::type* = 0)
    {
    }

    template<typename T>
    void operator()(T* v, typename boost::enable_if< boost::is_floating_point<T> >::type* = 0)
    {
        switch(columns_[fetch_col_].type_)
        {
            case SQLT_VNU: 
                convert_number_to_type(&columns_[fetch_col_].data_[0], SQLT_FLT, v, sizeof(v));
                break;
        }
    }

    void operator()(std::string* v)
    {
        column& c = columns_[fetch_col_];

        // TODO: Read by portion 

        if (c.is_lob())
        {
            ub4 lob_len = 0;
            throw_on_error_ = OCILobGetLength(svchp_, throw_on_error_.errhp_, c.lob_.get(), &lob_len);

            if (!lob_len) 
            {
                v->clear();
                return;
            }

            v->resize(lob_len);
            throw_on_error_ = OCILobRead(
                svchp_, throw_on_error_.errhp_, c.lob_.get(), &lob_len, 
                1,
                &v->front(), v->size(),
                0, 0,
                0, 0
              );
        }
        else 
            v->assign(&c.data_[0], c.col_fetch_size_);
    }

    void operator()(std::ostream* v)
    {
        column& c = columns_[fetch_col_];

        if (c.is_lob())
        {
            ub4 lob_len = 0;
            throw_on_error_ = OCILobGetLength(svchp_, throw_on_error_.errhp_, c.lob_.get(), &lob_len);

            if (!lob_len) 
                return;

            // TODO: Read by portion 
            std::vector<char> tmp(lob_len);

            throw_on_error_ = OCILobRead(
                svchp_, throw_on_error_.errhp_, c.lob_.get(), &lob_len, 
                1,
                &tmp[0], tmp.size(),
                0, 0,
                0, 0
              );

            v->write(&tmp[0], tmp.size());
        }
        else 
            v->write(&columns_[fetch_col_].data_[0], columns_[fetch_col_].col_fetch_size_);
    }

    void operator()(std::tm* v)
    {
        if (SQLT_DAT != columns_[fetch_col_].type_)
            throw invalid_placeholder();

        oci_descriptor_datetime dt;
        throw_on_error_ = OCIDescriptorAlloc(envhp_, dt.ptr(), OCI_DTYPE_TIMESTAMP, 0, 0);

        oci_descriptor_interval_ds iv;
        throw_on_error_ = OCIDescriptorAlloc(envhp_, iv.ptr(), OCI_DTYPE_INTERVAL_DS, 0, 0);

        ub4 data_len = columns_[fetch_col_].col_fetch_size_;
        throw_on_error_ = OCIDateTimeFromArray(
            envhp_, throw_on_error_.errhp_, 
            (ub1*)&columns_[fetch_col_].data_[0], data_len, SQLT_TIMESTAMP,
            dt.get(), iv.get(), 0
          );

        sb2 year;
        ub1 month;
        ub1 day;
        ub1 hour;
        ub1 min;
        ub1 sec;
        ub4 sec_frac;

        throw_on_error_ = OCIDateTimeGetDate(
            envhp_, throw_on_error_.errhp_, dt.get(), &year, &month, &day
          );

        throw_on_error_ = OCIDateTimeGetTime(
            envhp_, throw_on_error_.errhp_, dt.get(), &hour, &min, &sec, &sec_frac
          );

	    v->tm_year  = year - 1900;
	    v->tm_mon   = month - 1;
        v->tm_mday  = day;
        v->tm_hour  = hour;
        v->tm_min   = min;
	    v->tm_sec   = static_cast<int>(sec);
	    v->tm_isdst = -1;

	    if (mktime(v) == -1)
		    throw bad_value_cast();
    }

    //virtual bool fetch(int col,short &v) 
    //{
    //    return fetch_numeric(col, &v, sizeof(v), SQLT_INT);
    //}
    //virtual bool fetch(int col,unsigned short &v)
    //{
    //    return fetch_numeric(col, &v, sizeof(v), SQLT_UIN);
    //}
    //virtual bool fetch(int col,int &v)
    //{
    //    return fetch_numeric(col, &v, sizeof(v), SQLT_INT);
    //}
    //virtual bool fetch(int col,unsigned &v)
    //{
    //    return fetch_numeric(col, &v, sizeof(v), SQLT_UIN);
    //}
    //virtual bool fetch(int col,long &v)
    //{
    //    int tmp;
    //    bool ret = fetch_numeric(col, &tmp, sizeof(tmp), SQLT_INT);
    //    if(ret) 
    //        v = tmp;
    //    return ret;
    //}
    //virtual bool fetch(int col,unsigned long &v)
    //{
    //    unsigned int tmp;
    //    bool ret = fetch_numeric(col, &tmp, sizeof(tmp), SQLT_UIN);
    //    if(ret) 
    //        v = tmp;
    //    return ret;
    //}
    //virtual bool fetch(int col,long long &v)
    //{
    //    int tmp;
    //    bool ret = fetch_numeric(col, &tmp, sizeof(tmp), SQLT_INT);
    //    if(ret) 
    //        v = tmp;
    //    return ret;
    //}
    //virtual bool fetch(int col,unsigned long long &v)
    //{
    //    unsigned int tmp;
    //    bool ret = fetch_numeric(col, &tmp, sizeof(tmp), SQLT_UIN);
    //    if(ret) 
    //        v = tmp;
    //    return ret;
    //}
    //virtual bool fetch(int col,float &v) 
    //{
    //    return fetch_numeric(col, &v, sizeof(v), SQLT_FLT);
    //}
    //virtual bool fetch(int col,double &v)
    //{
    //    return fetch_numeric(col, &v, sizeof(v), SQLT_FLT);
    //}
    //virtual bool fetch(int col,long double &v)
    //{
    //    return fetch_numeric(col, &v, sizeof(v), SQLT_FLT);
    //}
    virtual bool is_null(int col)
    {
        return -1 == columns_[col].col_fetch_ind_;
    }

    virtual int cols() 
    {
        return columns_size_;
    }

    virtual unsigned long long rows() 
    {
        throw_on_error_ = OCIStmtFetch2(
            stmtp_, throw_on_error_.errhp_, 1, OCI_FETCH_LAST, 0, OCI_DEFAULT
          );

        just_initialized_ = true;

        ub4 rows_count;
        throw_on_error_ =  OCIAttrGet( 
            stmtp_, OCI_HTYPE_STMT, &rows_count, 0, OCI_ATTR_ROW_COUNT, throw_on_error_.errhp_
          );

        return rows_count;
    }
    virtual int name_to_column(const string_ref& n)
    {
        for(size_t i = 0; i < columns_size_; ++i)
        {
            if (boost::algorithm::iequals(n, columns_[i].name_))
                return int(i);
        }

        return -1;
    }
    virtual std::string column_to_name(int col)
    {
        if (col < 0 || (unsigned)col >= columns_size_)
            throw invalid_column();

        return columns_[col].name_;
    }

private:
    void convert_number_to_type(const void* number, ub2 type, void* out, size_t out_len)
    {
        const OCINumber* n = reinterpret_cast<const OCINumber*>(number);

        switch(type)
        {
            case SQLT_INT:
                throw_on_error_ = OCINumberToInt(
                    throw_on_error_.errhp_, n, out_len, OCI_NUMBER_SIGNED, out
                  );
                break;  
            case SQLT_UIN:
                throw_on_error_ = OCINumberToInt(
                    throw_on_error_.errhp_, n, out_len, OCI_NUMBER_UNSIGNED, out
                  );
                break;
            case SQLT_FLT:
                throw_on_error_ = OCINumberToReal(
                    throw_on_error_.errhp_, n, out_len, out
                  );
                break;
            default:
                throw edba_error("unsupported conversion for oracle NUMBER type");
        }
    }

    OCIEnv*           envhp_;
    OCISvcCtx*        svchp_;
    OCIStmt*          stmtp_;

    error_checker throw_on_error_;         //!< Error checker
    boost::scoped_array<column> columns_;  //!< Columns and defines
    ub4 columns_size_;                     //!< Number of columns in result
    bool just_initialized_;                //!< True before first fetch attempt
    int fetch_col_;
};

class statement : public backend::statement, public boost::static_visitor<>
{
public:
    statement(const string_ref& query, OCIEnv* envhp, OCIError* errhp, OCISvcCtx* svchp, session_monitor* sm)
      : backend::statement(sm)
      , envhp_(envhp)
      , svchp_(svchp)
      , throw_on_error_(errhp)
      , query_(query.begin(), query.end())
    {
        throw_on_error_ = OCIStmtPrepare2(
            svchp_
          , stmtp_.ptr()
          , throw_on_error_.errhp_
          , (const OraText*)query.begin()
          , (ub4) query.size()
          , 0
          , 0
          , OCI_NTV_SYNTAX, OCI_DEFAULT
          );

        stmt_type_ = stmt_attr_get<ub2>(OCI_ATTR_STMT_TYPE);
    }

    virtual void bind_impl(int col, bind_types_variant const& v)
    {
        v.apply_visitor(*this);
        bind_bounds_.back().col_ = col;
    }

    virtual void bind_impl(const string_ref& name, bind_types_variant const& v)
    {
        v.apply_visitor(*this);
        bind_bounds_.back().name_.assign(name.begin(), name.end());
    }

    virtual void bindings_reset_impl()
    {
        bind_buf_.clear();
        dt_holder_.clear();
    }
    
    template<typename T>
    void operator()(T v, typename boost::enable_if< boost::is_signed<T> >::type* = 0)
    {
        do_bind(&v, sizeof(v), SQLT_INT);
    }

    template<typename T>
    void operator()(T v, typename boost::enable_if< boost::is_unsigned<T> >::type* = 0)
    {
        do_bind(&v, sizeof(v), SQLT_UIN);
    }

    template<typename T>
    void operator()(T v, typename boost::enable_if< boost::is_floating_point<T> > ::type* = 0)
    {
        do_bind(&v, sizeof(v), SQLT_FLT);
    }

    void operator()(const string_ref& v)
    {
        do_bind(v.begin(), v.size(), SQLT_STR);
    }

    void operator()(const std::tm& v)
    {
        oci_descriptor_datetime dt;
        throw_on_error_ = OCIDescriptorAlloc(envhp_, dt.ptr(), OCI_DTYPE_TIMESTAMP, 0, 0);
        dt_holder_.push_back(boost::move(dt));

        throw_on_error_ = OCIDateTimeConstruct(
            envhp_
          , throw_on_error_.errhp_
          , dt_holder_.back().get()
          , v.tm_year + 1900
          , v.tm_mon + 1
          , v.tm_mday
          , v.tm_hour
          , v.tm_min
          , v.tm_sec
          , 0
          , 0, 0
          );

        do_bind(&dt_holder_.back(), sizeof(OCIDateTime*), SQLT_TIMESTAMP);
    }

    void operator()(std::istream* v) 
    {
        std::ostringstream ss;
        ss << v->rdbuf();
        size_t len = ss.str().length();
        do_bind(ss.str().c_str(), len, SQLT_CHR);
    }

    void operator()(null_type)
    {
        do_bind(0, 0, SQLT_STR);
    }

    virtual const std::string& patched_query() const
    {
        return query_;
    }

    virtual backend::result_ptr query_impl()
    {
        if (stmt_type_ != OCI_STMT_SELECT) 
            throw edba_error(std::string("oracle: query from not query statement"));

        bind_unbinded_params();

        throw_on_error_ = OCIStmtExecute(svchp_, stmtp_.get(), throw_on_error_.errhp_, 0, 0, 0, 0, OCI_STMT_SCROLLABLE_READONLY);
        
        return backend::result_ptr(new result(envhp_, svchp_, throw_on_error_.errhp_, stmtp_.get()));
    }
    virtual long long sequence_last(std::string const &/*name*/)
    {
        OCIRowid *rowid;
        throw_on_error_ = OCIDescriptorAlloc (envhp_, (dvoid **) &rowid, OCI_DTYPE_ROWID, 0, 0);
        throw_on_error_ = OCIAttrGet (stmtp_.get(), OCI_HTYPE_STMT, rowid, 0, OCI_ATTR_ROWID, throw_on_error_.errhp_);

        // TODO: Implement it

        return 0;
    }
    virtual void exec_impl()
    {
        if (OCI_STMT_SELECT == stmt_type_) 
            throw edba_error(std::string("oracle: exec of query statement"));

        bind_unbinded_params();

        throw_on_error_ = OCIStmtExecute(svchp_, stmtp_.get(), throw_on_error_.errhp_, 1, 0, 0, 0, OCI_DEFAULT);
    }
    virtual unsigned long long affected()
    {
        return stmt_attr_get<ub4>(OCI_ATTR_ROW_COUNT);
    }

private:    
    void do_bind(const void *v, size_t len, ub2 type) 
    {        
        // remember data to bind it later
        bind_bound b;
        b.idx_ = bind_buf_.size();
        b.size_ = len;
        b.type_ = type;
        bind_bounds_.push_back(b);

        bind_buf_.insert(bind_buf_.end(), (const char*)v, (const char*)v + len);
    }

    template<typename AttrType>
    AttrType stmt_attr_get(ub4 attr)
    {
        AttrType result;
        throw_on_error_ = OCIAttrGet(
            stmtp_.get()
          , OCI_HTYPE_STMT
          , &result
          , 0
          , attr
          , throw_on_error_.errhp_
          );

        return result;
    }

    void stmt_bind(int col, ub2 type, void* ptr, size_t len, void* indicator = 0)
    {
        OCIBind* tmp_binding = 0;

        throw_on_error_ = OCIBindByPos(
            stmtp_.get()
          , &tmp_binding
          , throw_on_error_.errhp_
          , (ub4) col
          , ptr
          , (sb4) len
          , type
          , indicator
          , 0, 0, 0, 0, OCI_DEFAULT
          );
    }

    void bind_unbinded_params()
    {
        for (size_t i = 0; i<bind_bounds_.size(); ++i)
        {
            int oci_ind_null = OCI_IND_NULL;
            void* poci_ind_null = !bind_bounds_[i].size_ ? &oci_ind_null : 0;

            stmt_bind(
                i + 1
              , bind_bounds_[i].type_
              , &bind_buf_[bind_bounds_[i].idx_]
              , bind_bounds_[i].size_
              , poci_ind_null
              );
        }
    }

    OCIEnv* envhp_;
    OCISvcCtx* svchp_;
    oci_handle_statement stmtp_;
    error_checker throw_on_error_;
    std::string query_;
    ub2 stmt_type_;

    struct bind_bound
    {
        bind_bound() : col_(0), idx_(0), size_(0), type_(SQLT_NON) {}

        std::string name_;
        int col_;
        size_t idx_;
        size_t size_;
        ub2 type_;
    };

    std::vector<char> bind_buf_;           //!< Bindings serialized in one buffer
    std::vector<bind_bound> bind_bounds_;  //!< Bindings bounds from bind_buf_
    boost::container::vector<oci_descriptor_datetime> dt_holder_;
    int bind_col_;
};

class connection : public backend::connection 
{
public:
    connection(const conn_info& ci, session_monitor* si) : backend::connection(ci, si)
    {
        string_ref username = ci.get("User");
        string_ref password = ci.get("Password"); 
        string_ref conn_string = ci.get("ConnectionString");
        
        throw_on_error_ = OCIEnvCreate(envhp_.ptr(), OCI_THREADED, 0, 0, 0, 0, 0, 0);
        throw_on_error_ = OCIHandleAlloc(envhp_.get(), errhp_.ptr(), OCI_HTYPE_ERROR, 0, 0);

        throw_on_error_.errhp_ = errhp_.get();
        
        throw_on_error_ = OCILogon(
            envhp_.get()
          , errhp_.get()
          , svcp_.ptr()
          , (const OraText*)username.begin(), (ub4)username.size()
          , (const OraText*)password.begin(), (ub4)password.size()
          , (const OraText*)conn_string.begin(), (ub4)conn_string.size()
          );
        
        char buf[256];
        ub4 version;

        throw_on_error_ = OCIServerRelease(svcp_.get(), errhp_.get(), (text *) buf, (ub4) sizeof(buf), OCI_HTYPE_SVCCTX, &version);

        description_ = buf;
        ver_major_ = version >> 24;
        ver_minor_ = (version << 8) >> 28;
    }

    virtual void begin_impl()
    {
        // throw_on_error_ = OCITransStart(svcp_.get(), errhp_.get(), 0, OCI_TRANS_NEW);
    }

    virtual void commit_impl() 
    {
        throw_on_error_ = OCITransCommit(svcp_.get(), errhp_.get(), OCI_DEFAULT);
    }

    virtual void rollback_impl()
    {
        OCITransRollback(svcp_.get(), errhp_.get(), OCI_DEFAULT); // do not throw exception on error
    }

    virtual backend::statement_ptr prepare_statement_impl(const string_ref& q)
    {
        return backend::statement_ptr(new statement(q, envhp_.get(), errhp_.get(), svcp_.get(), sm_));
    }

    virtual backend::statement_ptr create_statement_impl(const string_ref& q)
    {
        return prepare_statement(q);
    }

    virtual void exec_batch_impl(const string_ref& q)
    {
        using namespace boost::algorithm;

        BOOST_AUTO(spl_iter, (make_split_iterator(q, first_finder(";"))));
        BOOST_TYPEOF(spl_iter) spl_iter_end;

        for (;spl_iter != spl_iter_end; ++spl_iter)
        {
            BOOST_AUTO(query, *spl_iter);

            trim(query);
            if (query.empty()) 
                continue;

            create_statement(*spl_iter)->run_exec();    
        }
        
        commit();
    }

    virtual std::string escape(const string_ref& s)
    {
        std::string result;
        result.reserve(s.size()*2);
        BOOST_FOREACH(char c, s)
        {
            if (c=='\'') 
                result+="''";
            else if (c=='\"') 
                result+="\"\"";
            else 
                result+=c;
        }
        return result;
    }

    virtual const std::string& backend()
    {
        return g_backend_name;
    }

    virtual const std::string& engine()
    {
        return g_engine_name;
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

private:
    oci_handle_env             envhp_;
    oci_handle_error           errhp_;
    oci_handle_service_context svcp_;

    error_checker throw_on_error_;
    sword lasterror_;
    std::string description_;
    int ver_major_;
    int ver_minor_;
};

}} // oracle_backend, edba

extern "C" {
    EDBA_DRIVER_API edba::backend::connection *edba_oracle_get_connection(const edba::conn_info& ci, edba::session_monitor* sm)
    {
        return new edba::oracle_backend::connection(ci, sm);
    }
}
