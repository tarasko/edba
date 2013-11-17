#include <edba/backend/implementation_base.hpp>
#include <edba/detail/handle.hpp>
#include <edba/errors.hpp>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/find_iterator.hpp>
#include <boost/foreach.hpp>
#include <boost/typeof/typeof.hpp>
#include <boost/scoped_array.hpp>
#include <boost/type_traits/is_signed.hpp>
#include <boost/type_traits/is_unsigned.hpp>
#include <boost/type_traits/is_floating_point.hpp>
#include <boost/move/move.hpp>
#include <boost/container/vector.hpp>
#include <boost/mpl/switch.hpp>

#include <boost/foreach.hpp>

#include <sstream>
#include <vector>

#include <oci.h>

namespace edba { namespace backend { namespace oracle { namespace {

const std::string g_backend_name("oracle");
const std::string g_engine_name("oracle");
const int g_utf8_charset_id = 871;

struct handle_deallocator
{
    static void free(void* h, ub4 type)
    {
        OCIHandleFree(h, type);
    }
};

struct descriptor_deallocator
{
    static void free(void* h, ub4 type)
    {
        OCIDescriptorFree(h, type);
    }
};

typedef detail::handle<OCIEnv*,         ub4, OCI_HTYPE_ENV,         handle_deallocator>     oci_handle_env;
typedef detail::handle<OCIError*,       ub4, OCI_HTYPE_ERROR,       handle_deallocator>     oci_handle_error;
typedef detail::handle<OCISvcCtx*,      ub4, OCI_HTYPE_SVCCTX,      handle_deallocator>     oci_handle_service_context;
typedef detail::handle<OCIStmt*,        ub4, OCI_HTYPE_STMT,        handle_deallocator>     oci_handle_statement;
typedef detail::handle<OCIDefine*,      ub4, OCI_HTYPE_DEFINE,      handle_deallocator>     oci_handle_define;

typedef detail::handle<OCIDateTime*,    ub4, OCI_DTYPE_TIMESTAMP,   descriptor_deallocator> oci_desc_datetime;
typedef detail::handle<OCIInterval*,    ub4, OCI_DTYPE_INTERVAL_DS, descriptor_deallocator> oci_desc_interval_ds;
typedef detail::handle<OCIParam*,       ub4, OCI_DTYPE_PARAM,       descriptor_deallocator> oci_desc_param;
typedef detail::handle<OCILobLocator*,  ub4, OCI_DTYPE_LOB,         descriptor_deallocator> oci_desc_lob;

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

// Owned by connection object, used by statement and result objects too
struct common_data
{
    common_data() : inside_trans_(false) {}

    oci_handle_env             envhp_;
    oci_handle_error           errhp_;
    oci_handle_service_context svcp_;
    bool                       inside_trans_;
};

struct column
{
    std::string name_;
    ub2 type_;
    oci_handle_define define_;

    std::vector<char> data_;
    oci_desc_lob lob_;

    sb2 col_fetch_ind_;
    ub2	col_fetch_size_;
    ub2	col_fetch_rcode_;

    column() : col_fetch_ind_(0), col_fetch_size_(0), col_fetch_rcode_(0) {}

    void init(OCIEnv* envhp, OCIStmt* stmtp, OCIError* errp, ub4 idx)
    {
        error_checker ec(errp);
        oci_desc_param parm;
        ++idx;

        ec = OCIParamGet(stmtp, OCI_HTYPE_STMT, errp, parm.ptr().as_void(), idx);
      
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
            ec = OCIDescriptorAlloc(envhp, lob_.ptr().as_void(), OCI_DTYPE_LOB, 0, 0);

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
    typedef boost::integral_constant<int, SQLT_FLT> sqlt_flt_tag;
    typedef boost::integral_constant<int, SQLT_INT> sqlt_int_tag;
    typedef boost::integral_constant<int, SQLT_UIN> sqlt_uin_tag;

public:
    result(OCIEnv* envhp, OCISvcCtx* svchp, OCIError* errhp, OCIStmt* stmtp)
      : envhp_(envhp)
      , svchp_(svchp)
      , stmtp_(stmtp)
      , throw_on_error_(errhp)
      , total_rows_(-1)
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

        // Evaluate total number of rows
        throw_on_error_ = OCIStmtFetch2(
            stmtp_, throw_on_error_.errhp_, 1, OCI_FETCH_LAST, 0, OCI_DEFAULT
          );

        ub4 rows_count;
        throw_on_error_ =  OCIAttrGet( 
            stmtp_, OCI_HTYPE_STMT, &rows_count, 0, OCI_ATTR_ROW_COUNT, throw_on_error_.errhp_
          );

        total_rows_ = rows_count;
    }

    virtual next_row has_next()
    {
        if (total_rows_ == (unsigned long long)-1)
            return next_row_unknown;

        ub4 rows_processed;
        throw_on_error_ =  OCIAttrGet( 
            stmtp_, OCI_HTYPE_STMT, &rows_processed, 0, OCI_ATTR_ROW_COUNT, throw_on_error_.errhp_
          );

        return rows_processed == total_rows_ ? last_row_reached : next_row_exists;
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
    void operator()(T* v, typename boost::enable_if< boost::is_arithmetic<T> >::type* = 0)
    {
        using namespace boost::mpl;
        using namespace boost;

        typedef typename if_<
            is_floating_point<T>
          , sqlt_flt_tag
          , typename if_<
                is_signed<T>
              , sqlt_int_tag
              , sqlt_uin_tag
              >::type  
          >::type type_tag;

        switch(columns_[fetch_col_].type_)
        {
        case SQLT_VNU:
            convert_number_to_type(&columns_[fetch_col_].data_[0], type_tag(), v, sizeof(T));
            break;
        default:
            throw edba_error("unsupported conversion");
        }
    }

    void operator()(std::string* v)
    {
        column& c = columns_[fetch_col_];

        if (c.is_lob())
        {
            oraub8 lob_len_max = 0;
            throw_on_error_ = OCILobGetLength2(svchp_, throw_on_error_.errhp_, c.lob_.get(), &lob_len_max);

            if (!lob_len_max) 
            {
                v->clear();
                return;
            }

            oraub8 lob_len_char = lob_len_max;
            oraub8 lob_len_bytes = lob_len_max;

            if (c.type_ == SQLT_CLOB)
                lob_len_bytes *= 4;

            c.data_.resize((size_t)lob_len_bytes);

            throw_on_error_ = OCILobRead2(
                svchp_
              , throw_on_error_.errhp_
              , c.lob_.get()
              , &lob_len_bytes
              , &lob_len_char
              , 1
              , &c.data_[0]
              , c.data_.size()
              , OCI_ONE_PIECE
              , 0, 0, 0, 0
              );

              v->assign(c.data_.begin(), c.data_.begin() + (size_t)lob_len_bytes);
        }
        else 
            v->assign(&c.data_[0], c.col_fetch_size_);
    }

    void operator()(std::ostream* v)
    {
        column& c = columns_[fetch_col_];

        if (c.is_lob())
        {
            // TODO: Read by chunk size        
            oraub8 lob_len_max = 0;
            throw_on_error_ = OCILobGetLength2(svchp_, throw_on_error_.errhp_, c.lob_.get(), &lob_len_max);

            if (!lob_len_max) 
                return;

            oraub8 lob_len_char = lob_len_max;
            oraub8 lob_len_bytes = lob_len_max;

            if (c.type_ == SQLT_CLOB)
                lob_len_max *= 4;

            // TODO: Read by portion 
            c.data_.resize((size_t)lob_len_max);

            throw_on_error_ = OCILobRead2(
                svchp_
              , throw_on_error_.errhp_
              , c.lob_.get()
              , &lob_len_bytes
              , &lob_len_char
              , 1
              , &c.data_[0]
              , c.data_.size()
              , OCI_ONE_PIECE
              , 0, 0, 0, 0
              );

              v->write(&c.data_[0], lob_len_bytes);
        }
        else 
            v->write(&columns_[fetch_col_].data_[0], columns_[fetch_col_].col_fetch_size_);
    }

    void operator()(std::tm* v)
    {
        if (SQLT_DAT != columns_[fetch_col_].type_)
            throw invalid_placeholder();

        oci_desc_datetime dt;
        throw_on_error_ = OCIDescriptorAlloc(envhp_, dt.ptr().as_void(), OCI_DTYPE_TIMESTAMP, 0, 0);

        oci_desc_interval_ds iv;
        throw_on_error_ = OCIDescriptorAlloc(envhp_, iv.ptr().as_void(), OCI_DTYPE_INTERVAL_DS, 0, 0);

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
        return total_rows_;
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
    void convert_number_to_type(const void* number, sqlt_flt_tag, void* out, size_t out_len)
    {
        const OCINumber* n = reinterpret_cast<const OCINumber*>(number);
        throw_on_error_ = OCINumberToReal(throw_on_error_.errhp_, n, out_len, out);
    }

    void convert_number_to_type(const void* number, sqlt_int_tag, void* out, size_t out_len)
    {
        const OCINumber* n = reinterpret_cast<const OCINumber*>(number);
        throw_on_error_ = OCINumberToInt(throw_on_error_.errhp_, n, out_len, OCI_NUMBER_SIGNED, out);
    }

    void convert_number_to_type(const void* number, sqlt_uin_tag, void* out, size_t out_len)
    {
        const OCINumber* n = reinterpret_cast<const OCINumber*>(number);
        throw_on_error_ = OCINumberToInt(throw_on_error_.errhp_, n, out_len, OCI_NUMBER_UNSIGNED, out);
    }

    OCIEnv*           envhp_;
    OCISvcCtx*        svchp_;
    OCIStmt*          stmtp_;

    error_checker throw_on_error_;         //!< Error checker
    boost::scoped_array<column> columns_;  //!< Columns and defines
    ub4 columns_size_;                     //!< Number of columns in result
    unsigned long long total_rows_;        //!< Total number of rows
    bool just_initialized_;                //!< True before first next call
    int fetch_col_;
};

class statement : public backend::statement, public boost::static_visitor<>
{
public:
    statement(const string_ref& query, const common_data* cd, session_stat* stat)
      : backend::statement(stat)
      , cd_(cd)
      , throw_on_error_(cd->errhp_.get())
      , query_(query.begin(), query.end())
    {
        throw_on_error_ = OCIStmtPrepare2(
            cd_->svcp_.get()
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
        bind_bounds_.clear();
        bind_buf_.clear();
        dt_holder_.clear();
        lob_holder_.clear();
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
        do_bind(v.begin(), v.size(), SQLT_CHR);
    }

    void operator()(const std::tm& v)
    {
        oci_desc_datetime dt;
        throw_on_error_ = OCIDescriptorAlloc(cd_->envhp_.get(), dt.ptr().as_void(), OCI_DTYPE_TIMESTAMP, 0, 0);
        dt_holder_.push_back(boost::move(dt));

        throw_on_error_ = OCIDateTimeConstruct(
            cd_->envhp_.get()
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
        oci_desc_lob lob;
        throw_on_error_ = OCIDescriptorAlloc(cd_->envhp_.get(), lob.ptr().as_void(), OCI_DTYPE_LOB, 0, 0);
        throw_on_error_ = OCILobCreateTemporary(cd_->svcp_.get(), throw_on_error_.errhp_, lob.get(), OCI_DEFAULT, OCI_DEFAULT, OCI_TEMP_BLOB, FALSE, OCI_DURATION_STATEMENT);
        
        ub4 chunk_size;
        throw_on_error_ = OCILobGetChunkSize(cd_->svcp_.get(), throw_on_error_.errhp_, lob.get(), &chunk_size);

        std::vector<char> buf(chunk_size);
        for(;;) 
        {
            v->read(&buf[0], buf.size());
            oraub8 bytes_read = static_cast<oraub8>(v->gcount());
            if(bytes_read > 0)
                throw_on_error_ = OCILobWriteAppend2(cd_->svcp_.get(), throw_on_error_.errhp_, lob.get(), &bytes_read, 0, &buf[0], bytes_read, OCI_ONE_PIECE, 0, 0, OCI_DEFAULT, SQLCS_IMPLICIT);
            
            if(bytes_read < buf.size())
                break;
        }

        lob_holder_.push_back(boost::move(lob));

        do_bind(&lob_holder_.back(), sizeof(OCILobLocator*), SQLT_BLOB);
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

        throw_on_error_ = OCIStmtExecute(cd_->svcp_.get(), stmtp_.get(), throw_on_error_.errhp_, 0, 0, 0, 0, OCI_STMT_SCROLLABLE_READONLY);
        
        // emulate auto-commit when we are not inside transaction
        if (!cd_->inside_trans_)
            throw_on_error_ = OCITransCommit(cd_->svcp_.get(), throw_on_error_.errhp_, OCI_DEFAULT);

        return backend::result_ptr(new result(cd_->envhp_.get(), cd_->svcp_.get(), throw_on_error_.errhp_, stmtp_.get()));
    }

    virtual long long sequence_last(std::string const &name)
    {
        std::string query("select ");
        query += name;
        query += ".currval from dual";

        statement st(query, cd_, stat_.parent_stat());
        backend::result_ptr res = st.run_query();
        res->next();
        long long id;
        res->fetch(0, &id); 

        return id;
    }

    virtual void exec_impl()
    {
        if (OCI_STMT_SELECT == stmt_type_) 
            throw edba_error(std::string("oracle: exec of query statement"));

        bind_unbinded_params();

        throw_on_error_ = OCIStmtExecute(cd_->svcp_.get(), stmtp_.get(), throw_on_error_.errhp_, 1, 0, 0, 0, OCI_DEFAULT);

        // emulate auto-commit when we are not inside transaction
        if (!cd_->inside_trans_)
            throw_on_error_ = OCITransCommit(cd_->svcp_.get(), throw_on_error_.errhp_, OCI_DEFAULT);
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

        // Strings must be null-terminated
        if (len && SQLT_STR == type)
        {
            bind_buf_.push_back(0);
            bind_bounds_.back().size_ += 1;
        }
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

    void bind_unbinded_params()
    {
        BOOST_FOREACH(const bind_bound& bound, bind_bounds_)
        {
            int oci_ind_null;
            void* poci_ind_null;
            void* buf;

            if (bound.size_)
            {
                poci_ind_null = 0;
                buf = &bind_buf_[bound.idx_];
            }
            else
            {
                oci_ind_null = OCI_IND_NULL;
                poci_ind_null = &oci_ind_null;
                buf = 0;
            }

            OCIBind* tmp_binding = 0;

            if (bound.name_.empty())
            {
                throw_on_error_ = OCIBindByPos(
                    stmtp_.get()
                  , &tmp_binding
                  , throw_on_error_.errhp_
                  , (ub4) bound.col_
                  , buf
                  , (sb4) bound.size_
                  , bound.type_
                  , poci_ind_null
                  , 0, 0, 0, 0, OCI_DEFAULT
                  );
            }
            else
            {
                throw_on_error_ = OCIBindByName(
                    stmtp_.get()
                  , &tmp_binding
                  , throw_on_error_.errhp_
                  , (const OraText*)&bound.name_[0]
                  , bound.name_.size()
                  , buf
                  , (sb4) bound.size_
                  , bound.type_
                  , poci_ind_null
                  , 0, 0, 0, 0, OCI_DEFAULT
                  );
            }
        }
    }

    const common_data* cd_;
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
    boost::container::vector<oci_desc_datetime> dt_holder_;
    boost::container::vector<oci_desc_lob> lob_holder_;
    int bind_col_;
};

class connection : public backend::connection, private common_data
{
public:
    connection(const conn_info& ci, session_monitor* si) : backend::connection(ci, si)
    {
        string_ref username = ci.get("User");
        string_ref password = ci.get("Password"); 
        string_ref conn_string = ci.get("ConnectionString");
        
        throw_on_error_ = OCIEnvNlsCreate(envhp_.ptr(), OCI_THREADED, 0, 0, 0, 0, 0, 0, g_utf8_charset_id, g_utf8_charset_id);
        throw_on_error_ = OCIHandleAlloc(envhp_.get(), errhp_.ptr().as_void(), OCI_HTYPE_ERROR, 0, 0);

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
        // OCI works always in manual commit mode
        // so we need to emulate auto-commit after each statement not wrapped by transaction,
        // and stop emulating when begin_impl was explicitly called

        if (inside_trans_)
            throw edba_error("nested transactions are not supported by oracle backend");
        inside_trans_ = true;
    }

    virtual void commit_impl() 
    {
        throw_on_error_ = OCITransCommit(svcp_.get(), errhp_.get(), OCI_DEFAULT);
        inside_trans_ = false;
    }

    virtual void rollback_impl()
    {
        OCITransRollback(svcp_.get(), errhp_.get(), OCI_DEFAULT); // do not throw exception on error
        inside_trans_ = false;
    }

    virtual backend::statement_ptr prepare_statement_impl(const string_ref& q)
    {
        return backend::statement_ptr(new statement(q, this, &stat_));
    }

    virtual backend::statement_ptr create_statement_impl(const string_ref& q)
    {
        return prepare_statement_impl(q);
    }

    virtual void exec_batch_impl(const string_ref& q)
    {
        using namespace boost::algorithm;

        BOOST_AUTO(spl_iter, (make_split_iterator(q, first_finder(";"))));
        BOOST_TYPEOF(spl_iter) spl_iter_end;

        BOOST_FOREACH(string_ref item, (boost::make_iterator_range(spl_iter, spl_iter_end)))
        {
            trim(item);
            if (item.empty()) 
                continue;

            prepare_statement_impl(item)->run_exec();    
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
    error_checker throw_on_error_;
    sword lasterror_;
    std::string description_;
    int ver_major_;
    int ver_minor_;
};

}}}} // edba, backend, oracle, anonymous

extern "C" 
{
    EDBA_DRIVER_API edba::backend::connection *edba_oracle_get_connection(const edba::conn_info& ci, edba::session_monitor* sm)
    {
        return new edba::backend::oracle::connection(ci, sm);
    }
}
