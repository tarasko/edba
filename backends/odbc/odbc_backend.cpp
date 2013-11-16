#include <edba/backend/bind_by_name_helper.hpp>
#include <edba/detail/utils.hpp>
#include <edba/detail/handle.hpp>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/find_iterator.hpp>
#include <boost/locale.hpp>
#include <boost/typeof/typeof.hpp>
#include <boost/static_assert.hpp>
#include <boost/make_shared.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/mpl/map.hpp>
#include <boost/mpl/int.hpp>
#include <boost/mpl/at.hpp>
#include <boost/type_traits/is_arithmetic.hpp>
#include <boost/format.hpp>
#include <boost/multi_index_container.hpp>
#include <boost/multi_index/random_access_index.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/member.hpp>

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
using namespace boost::locale::conv;
using namespace boost::locale;
using namespace std;
using namespace boost::multi_index;

namespace boost {
    namespace locale {
        namespace conv {

            ///
            /// Extend original utf_to_utf to support Output iterator instead of forming new basic_string
            template<typename CharOut,typename InputIter,typename OutputIter>
            void
            utf_to_utf(InputIter begin, InputIter end, OutputIter output, method_type how = default_method)
            {
                typedef typename std::iterator_traits<InputIter>::value_type CharIn;

                utf::code_point c;
                while(begin!=end) {
                    c=utf::utf_traits<CharIn>::template decode<InputIter>(begin,end);
                    if(c==utf::illegal || c==utf::incomplete) {
                        if(how==stop)
                            throw conversion_error();
                    }
                    else {
                        utf::utf_traits<CharOut>::template encode<OutputIter>(c,output);
                    }
                }
            }
        }
    }
}

namespace edba {

struct column_info
{
    string name_;       // name
    SQLSMALLINT type_;  // type
};

string_ref to_string_ref(const column_info& ci)
{
    return string_ref(ci.name_);
}

namespace backend { namespace odbc { namespace {

// Deallocator type for detail::handle wrapper
struct handle_deallocator
{
    static void free(SQLHANDLE h, SQLSMALLINT type)
    {
        SQLFreeHandle(type, h);
    }
};

// Wrappers for different handle types
typedef detail::handle<SQLHSTMT, SQLSMALLINT, SQL_HANDLE_STMT, handle_deallocator> stmt_handle;
typedef detail::handle<SQLHENV, SQLSMALLINT, SQL_HANDLE_ENV, handle_deallocator> env_handle;
typedef detail::handle<SQLHDBC, SQLSMALLINT, SQL_HANDLE_DBC, handle_deallocator> dbc_handle;

// This data is stored in connection object but required everywhere, including statement and result objects
struct common_data
{
    env_handle env_;
    dbc_handle dbc_;
    bool wide_;
    string engine_;
    int ver_major_;
    int ver_minor_;
    string description_;
    string sequence_last_;
    string last_insert_id_;
    SQLUSMALLINT commit_behavior_;
    SQLUSMALLINT rollback_behavior_;
};

// backend name
const string g_backend("odbc");

// create boost locale compatible locale for conversion from UTF-16 message to system default locale
const locale g_system_locale = generator()("");

boost::format g_error_formatter("edba::backend::odbc %1% failed with error %2% (%3%)");

// TODO: Some code rely on this constrants.
// Review code and relax them
BOOST_STATIC_ASSERT(sizeof(unsigned) == 4);
BOOST_STATIC_ASSERT(sizeof(unsigned short) == 2);
BOOST_STATIC_ASSERT(sizeof(SQLWCHAR) == 2);

// This map allow to get SQL C type constant for C++ number types.
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

struct error_checker
{
    bool wide_;
    SQLHANDLE h_;
    SQLSMALLINT type_;
    const char* api_;

    error_checker(bool wide, SQLHANDLE h, SQLSMALLINT type)
        : wide_(wide), h_(h), type_(type)
    {}

    error_checker& operator()(const char* api)
    {
        api_ = api;
        return *this;
    }

    SQLRETURN operator=(SQLRETURN error) const
    {
        if(SQL_SUCCEEDED(error))
            return error;

        boost::format fmt("edba::backend::odbc %1% failed with error %2% (%3%)");

        int rec = 1;
        SQLINTEGER err;
        SQLSMALLINT len;
        string msg;

        if (wide_)
        {
            basic_string<SQLWCHAR> utf16_msg;

            for(;;)
            {
                SQLWCHAR msg_buf[SQL_MAX_MESSAGE_LENGTH + 2] = {0};
                SQLWCHAR stat[SQL_SQLSTATE_SIZE + 1] = {0};
                SQLRETURN r = SQLGetDiagRecW(type_, h_, rec++, stat, &err, msg_buf, sizeof(msg_buf)/sizeof(SQLWCHAR), &len);

                if(r == SQL_SUCCESS || r == SQL_SUCCESS_WITH_INFO)
                {
                    if(!utf16_msg.empty())
                    {
                        SQLWCHAR nl = L'\n';
                        utf16_msg += nl;
                    }
                    utf16_msg.append(msg_buf);
                }
                else
                    break;
            }

            msg = utf_to_utf<char>(utf16_msg);
        }
        else
        {
            for(;;)
            {
                SQLCHAR msg_buf[SQL_MAX_MESSAGE_LENGTH + 2] = {0};
                SQLCHAR stat[SQL_SQLSTATE_SIZE + 1] = {0};
                SQLRETURN r = SQLGetDiagRecA(type_, h_, rec++, stat, &err, msg_buf, sizeof(msg_buf), &len);
                
                if(r == SQL_SUCCESS || r == SQL_SUCCESS_WITH_INFO)
                {
                    if(!msg.empty())
                        msg += '\n';
                    msg += (char *)msg_buf;
                }
                else
                    break;

            }
        }

        throw edba_error((fmt % api_ % msg % err).str());
        return error;
    }
};



class result : public backend::result, public boost::static_visitor<bool>
{
    static const SQLULEN MAX_READ_BUFFER_SIZE = 4096;

    typedef multi_index_container<
        column_info
      , indexed_by<
            random_access<>
          , ordered_unique<member<column_info, std::string, &column_info::name_>, string_ref_less >
          >
      > columns_set;

public:
    result(SQLHSTMT stmt, bool wide) 
        : stmt_(stmt)
        , wide_(wide)
        , throw_on_error_(wide, stmt, SQL_HANDLE_STMT)
    {
        // Read number of columns
        SQLSMALLINT columns_count;
        throw_on_error_("SQLNumResultCols") = SQLNumResultCols(stmt_, &columns_count);
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
                throw_on_error_("SQLDescribeColW") = SQLDescribeColW(stmt_, col + 1, name, 256, &name_length, &ci.type_, &column_size, 0, 0);
                ci.name_ = utf_to_utf<char>(name);
            }
            else
            {
                SQLCHAR name[257] = {0};
                throw_on_error_("SQLDescribeColA") = SQLDescribeColA(stmt_, col + 1, name, 256, &name_length, &ci.type_, &column_size, 0, 0);
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

            columns_.push_back(ci);
        }

        max_column_size = (min)(max_column_size + 1, MAX_READ_BUFFER_SIZE);
        column_char_buf_.resize(max_column_size);
    }

    ~result()
    {
        // Don`t try to use SQLCloseCursor here because in case when statement cursor is not open, SQLCloseCursor will
        // turn your statement object into invalid state, and further operations will be impossible.
        // I found that when SELECT was done inside of transaction and SQLEndTran had closed statement cursor before 
        // we got to ~result. ~result had used SQLCloseCursor that days.

        // I`ve wasted  2 days trying figure out why statement in cache become broken and SQLExecute always fail with 
        // "incorrect cursor state".

        // Checkout out this article
        // http://msdn.microsoft.com/en-us/library/ms713402(v=vs.85).aspx
        // Read carefully about SQLCloseCursor 
        // http://msdn.microsoft.com/en-us/library/ms709301(v=vs.85).aspx
        // and take care of youself

        SQLFreeStmt(stmt_, SQL_CLOSE);
    }

    template<typename T>
    bool operator()(T* data, typename boost::enable_if< boost::is_arithmetic<T> >::type* = 0 )
    {
        typedef typename mpl::at<type_ids_map, T>::type data_pair;
        typedef typename data_pair::first c_type_id;
        typedef typename data_pair::second c_type;

        c_type tmp;
        SQLLEN indicator;

        throw_on_error_("SQLGetData") = SQLGetData(stmt_, fetch_col_, c_type_id::value, &tmp, sizeof(tmp), &indicator);

        if (SQL_NULL_DATA == indicator)
            return false;

        *data = static_cast<T>(tmp);

        return true;
    }

    bool operator()(tm* data)
    {
        TIMESTAMP_STRUCT tmp;
        SQLLEN indicator;

        throw_on_error_("SQLGetData") = SQLGetData(stmt_, fetch_col_, SQL_C_TYPE_TIMESTAMP, &tmp, sizeof(tmp), &indicator);

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
        mktime(data);

        return true;
    }

    bool operator()(string* _data)
    {
        SQLLEN indicator;
        string data;

        SQLRETURN r;

        SQLSMALLINT sqltype = columns_[fetch_col_ - 1].type_;
        bool        fetch_wchar = sqltype == SQL_WCHAR || sqltype == SQL_WVARCHAR || sqltype == SQL_WLONGVARCHAR;
        SQLSMALLINT ctype = fetch_wchar ? SQL_C_WCHAR : SQL_C_CHAR;
        size_t      csize = fetch_wchar ? sizeof(SQLWCHAR) : sizeof(SQLCHAR);

        do
        {
            r = throw_on_error_("SQLGetData") = SQLGetData(stmt_, fetch_col_, ctype, &column_char_buf_[0], column_char_buf_.size(), &indicator);

            if (SQL_NULL_DATA == indicator)
                return false;

            SQLLEN bytes_read = (min)(indicator, (SQLLEN)(column_char_buf_.size() - csize));

            if (fetch_wchar)
            {
                utf_to_utf<char>(
                    (SQLWCHAR*)&column_char_buf_[0]
                  , (SQLWCHAR*)(&column_char_buf_[0] + bytes_read)
                  , std::back_inserter(data)
                  );
            }
            else
                data.append(column_char_buf_.begin(), column_char_buf_.begin() + bytes_read);
        } while(SQL_SUCCESS_WITH_INFO == r);

        _data->swap(data);

        return true;
    }

    bool operator()(ostream* data)
    {
        SQLLEN indicator;
        SQLRETURN r;

        SQLSMALLINT sqltype = columns_[fetch_col_ - 1].type_;
        bool        fetch_wchar = sqltype == SQL_WCHAR || sqltype == SQL_WVARCHAR || sqltype == SQL_WLONGVARCHAR;
        SQLSMALLINT ctype = fetch_wchar ? SQL_C_WCHAR : SQL_C_BINARY;
        size_t      csize = fetch_wchar ? sizeof(SQLWCHAR) : 0;

        do
        {
            r = throw_on_error_("SQLGetData") = SQLGetData(stmt_, fetch_col_, ctype, &column_char_buf_[0], column_char_buf_.size(), &indicator);

            if (SQL_NULL_DATA == indicator)
                return false;

            SQLLEN bytes_read = (min)(indicator, (SQLLEN)(column_char_buf_.size() - csize));

            if (fetch_wchar)
            {
                utf_to_utf<char>(
                    (SQLWCHAR*)&column_char_buf_[0]
                  , (SQLWCHAR*)(&column_char_buf_[0] + bytes_read)
                  , std::ostreambuf_iterator<char>(*data)
                  );
            }
            else
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

        if(r == SQL_SUCCESS || r == SQL_SUCCESS_WITH_INFO)
            return true;

        if(r == SQL_NO_DATA)
            return false;

        throw_on_error_("SQLGetData") = r;

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
        SQLRETURN r = SQLGetData(stmt_, col + 1, SQL_C_DEFAULT, buf, 0, &indicator);
        
        if (!SQL_SUCCEEDED(r))
            throw_on_error_("SQLGetData") = SQLGetData(stmt_, col + 1, SQL_C_DEFAULT, buf, sizeof(buf), &indicator);

        return indicator == SQL_NULL_DATA;
    }

    virtual int cols()
    {
        return (int)columns_.size();
    }

    virtual boost::uint64_t rows()
    {
        // not supported by odbc
        return (boost::uint64_t)-1;
    }

    virtual int name_to_column(const string_ref& name)
    {
        BOOST_AUTO(&idx, columns_.get<1>());
        BOOST_AUTO(iter, idx.find(name));

        if (iter == idx.end())
            return -1;
        return columns_.project<0>(iter) - columns_.begin();
    }

    virtual string column_to_name(int col)
    {
        if ((size_t)col >= columns_.size())
            throw invalid_column();

        return columns_[col].name_;
    }

private:
    SQLHSTMT stmt_;
    bool wide_;
    int fetch_col_;
    columns_set columns_;
    vector<char> column_char_buf_;
    error_checker throw_on_error_;
};

class statement : public backend::bind_by_name_helper, public boost::static_visitor<boost::shared_ptr<pair<SQLLEN, string> > >
{
    typedef pair<SQLLEN, string> holder;
    typedef boost::shared_ptr<holder> holder_sp;

    struct param_desc
    {
        SQLSMALLINT data_type_;
        SQLULEN     param_size_;
        SQLSMALLINT decimal_digits_;
        SQLSMALLINT nullable_;
    };

public:
    statement(
        const common_data* cd
      , session_monitor* sm
      , const string_ref& q
      , bool prepared
      )
      : backend::bind_by_name_helper(sm, q, backend::question_marker())
      , cd_(cd)
      , prepared_(prepared)
      , throw_on_error_(cd->wide_, 0, SQL_HANDLE_STMT)
    {
        // Allocate statement handle
        stmt_handle stmt;
        error_checker(cd->wide_, cd->dbc_.get(), SQL_HANDLE_DBC)("SQLAllocHandle") = SQLAllocHandle(SQL_HANDLE_STMT, cd->dbc_.get(), stmt.ptr());
        throw_on_error_.h_ = stmt.get();

        // If prepared statement were requested, run SQLPrepare
        if(prepared_)
        {
            if(cd->wide_)
            {
                throw_on_error_("SQLPrepareW") = SQLPrepareW(
                        stmt.get()
                      , (SQLWCHAR*)utf_to_utf<SQLWCHAR>(patched_query()).c_str()
                      , SQL_NTS
                      );
            }
            else
            {
                throw_on_error_("SQLPrepareA") = SQLPrepareA(
                        stmt.get()
                      , (SQLCHAR*)patched_query().c_str()
                      , SQL_NTS
                      );
            }

            try_fill_params_descriptions(stmt.get());
        }

        stmt_ = boost::move(stmt);
    }

    virtual void bindings_reset_impl()
    {
        // Don`t try to use SQLCloseCursor here because in case when statement cursor is not open, SQLCloseCursor will
        // turn your statement object into invalid state, and further operations will be impossible.
        // I found that when SELECT was done inside of transaction and SQLEndTran had closed statement cursor before 
        // we got to ~result. ~result had used SQLCloseCursor that days.

        // I`ve wasted  2 days trying figure out why statement in cache become broken and SQLExecute always fail with 
        // "incorrect cursor state".

        // Checkout out this article
        // http://msdn.microsoft.com/en-us/library/ms713402(v=vs.85).aspx
        // Read carefully about SQLCloseCursor 
        // http://msdn.microsoft.com/en-us/library/ms709301(v=vs.85).aspx
        // and take care of youself

        SQLFreeStmt(stmt_.get(), SQL_CLOSE);
        SQLFreeStmt(stmt_.get(), SQL_RESET_PARAMS);

        params_.resize(0);
    }

    virtual void bind_impl(int col, bind_types_variant const& v)
    {
        bind_col_ = col;
        params_.push_back(v.apply_visitor(*this));
    }

    template<typename T>
    holder_sp operator()(T v)
    {
        typedef typename mpl::at<type_ids_map, T>::type data_pair;
        typedef typename data_pair::first c_type_id;
        typedef typename data_pair::second c_type;

        param_desc desc;
        desc.data_type_ = numeric_limits<T>::is_integer ? SQL_INTEGER : SQL_DOUBLE;
        desc.decimal_digits_ = 0;
        desc.param_size_ = sizeof(c_type);

        c_type tmp = (c_type)v;

        holder_sp value = boost::make_shared<holder>(0, string((const char*)&tmp, sizeof(c_type)));

        do_bind(false, c_type_id::value, desc, *value);

        return value;
    }

    holder_sp operator()(null_type)
    {
        holder_sp value = boost::make_shared<holder>();

        // All sql types are compatible with either SQL_VARCHAR or SQL_VARBINARY.
        // Null binding doesn`t work if specified sqltype is incompatible with actual type in database, because conversion 
        // of VARCHAR null to VARBINARY null is impossible :) I have tried this on mssql 2008 native client driver.
        //
        // Thus if SQLDescribeParam is not supported by particular driver, we can`t implement binding of nulls that will 
        // always work, we have to choose what type will be possible to bind null. VARCHAR is much more friendly in term of
        // conversion to different types

        do_bind(true, SQL_C_DEFAULT, get_param_desc(bind_col_), *value);
        return value;
    }

    holder_sp operator()(const string_ref& v)
    {
        holder_sp value;
        const param_desc& desc = get_param_desc(bind_col_);
        bool bind_as_wchar = desc.data_type_ == SQL_WVARCHAR || desc.data_type_ == SQL_WVARCHAR || desc.data_type_ == SQL_WLONGVARCHAR;

        if(bind_as_wchar)
        {
            basic_string<SQLWCHAR> wstr = utf_to_utf<SQLWCHAR>(v.begin(), v.end());

            value = boost::make_shared<holder>(0, string((const char*)&wstr[0], wstr.size() * sizeof(SQLWCHAR)));
            do_bind(false, SQL_C_WCHAR, desc, *value);
        }
        else
        {
            value = boost::make_shared<holder>();
            value->second.assign(v.begin(), v.end());
            do_bind(false, SQL_C_CHAR, desc, *value);
        }

        return value;
    }

    holder_sp operator()(const tm& v)
    {
        holder_sp value = boost::make_shared<holder>(0, format_time(v));
        
        param_desc desc;
        desc.data_type_ = SQL_TYPE_TIMESTAMP;
        desc.decimal_digits_ = 0;
        desc.param_size_ = value->second.size();

        do_bind(false, SQL_C_CHAR, desc, *value);
        return value;
    }

    holder_sp operator()(istream* v)
    {
        const param_desc& desc = get_param_desc(bind_col_);
        SQLSMALLINT ctype;
        switch(desc.data_type_)
        {
            case SQL_WCHAR:
            case SQL_WVARCHAR:
            case SQL_WLONGVARCHAR:
                ctype = SQL_C_WCHAR;
                break;
            case SQL_CHAR:
            case SQL_VARCHAR:
            case SQL_LONGVARCHAR:
                ctype = SQL_C_CHAR;
                break;
            default:
                ctype = SQL_C_BINARY;
                break;
        }

        holder_sp value = boost::make_shared<holder>(0, string());
        if (ctype == SQL_C_WCHAR)
        {
            wstring res;
            utf_to_utf<wchar_t>(istreambuf_iterator<char>(*v), istreambuf_iterator<char>(), back_inserter(res));
            value->second.assign((const char*)&res[0], res.size() * sizeof(wchar_t));
        }
        else
            value->second.assign(istreambuf_iterator<char>(*v), istreambuf_iterator<char>());

        do_bind(false, ctype, desc, *value);
        return value;
    }

    virtual long long sequence_last(string const &sequence)
    {
        // evaluate statement
        statement_ptr st;

        if (sequence.empty() && !cd_->last_insert_id_.empty())
            st.reset(new statement(cd_, 0, cd_->last_insert_id_, false));
        else if (!sequence.empty() && !cd_->sequence_last_.empty())
        {
            st.reset(new statement(cd_, 0, cd_->sequence_last_, false));
            st->bind(1, sequence);
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
        boost::intrusive_ptr<result> res = boost::static_pointer_cast<result>(st->run_query());
        long long last_id;

        if(!res->next() || res->cols() != 1 || !res->fetch(0, fetch_types_variant(&last_id)))
            throw edba_error("edba::odbc::sequence_last failed to fetch last value");

        return last_id;
    }

    virtual unsigned long long affected()
    {
        SQLLEN rows = 0;
        throw_on_error_("SQLRowCount") = SQLRowCount(stmt_.get(), &rows);
        return rows;
    }

    virtual backend::result_ptr query_impl()
    {
        BOOST_AUTO(p, real_exec());
        throw_on_error_(p.first) = p.second;
        return backend::result_ptr(new result(stmt_.get(), cd_->wide_));
    }

    virtual void exec_impl()
    {
        BOOST_AUTO(p, real_exec());
        
        if(p.second != SQL_NO_DATA)
            throw_on_error_(p.first) = p.second;
    }

    pair<const char*, SQLRETURN> real_exec()
    {
        if(prepared_) 
            return make_pair("SQLExecute", SQLExecute(stmt_.get()));
        else 
        {
            if(cd_->wide_)
                return make_pair("SQLExecDirectW", SQLExecDirectW(stmt_.get(), (SQLWCHAR*)utf_to_utf<SQLWCHAR>(patched_query()).c_str(), SQL_NTS));
            else
                return make_pair("SQLExecDirectA", SQLExecDirectA(stmt_.get(), (SQLCHAR*)patched_query().c_str(), SQL_NTS));
        }
    }

    // End of API

private:
    void try_fill_params_descriptions(SQLHSTMT stmt)
    {
        SQLSMALLINT params_no;

        if (!SQL_SUCCEEDED(SQLNumParams(stmt, &params_no)))
            // This is bad news for us, we can`t make uniform null binding.
            return;

        if (!params_no)
            return;

        params_desc_.resize(params_no);
        for (SQLSMALLINT i = 0; i < params_no; ++i)
        {
            param_desc& d = params_desc_[i];

            if (!SQL_SUCCEEDED(SQLDescribeParam(stmt, i + 1, &d.data_type_, &d.param_size_, &d.decimal_digits_, &d.nullable_)))
            {
                params_desc_.clear();
                return;
            }
        }
    }

    const param_desc& get_param_desc(int column) const
    {
        if (params_desc_.size() < (size_t)column)
            return s_generic_null_desc;
        else
            return params_desc_[column - 1];
    }

    void do_bind(bool null, SQLSMALLINT ctype, const param_desc& desc, holder& value)
    {
        if(null)
        {
            value.first = SQL_NULL_DATA;
            throw_on_error_("SQLBindParameter") = SQLBindParameter(stmt_.get(),
                bind_col_,
                SQL_PARAM_INPUT,
                ctype,
                desc.data_type_, // for null
                10, // COLUMNSIZE
                0, //  Presision
                0, // string
                0, // size
                &value.first);
        }
        else
        {
            value.first = value.second.size();
            size_t column_size = desc.param_size_;
            
            // Mumbo-jumbo with column size
            // I hate ODBC. Some related docs
            // http://msdn.microsoft.com/en-us/library/ms711786(v=vs.85).aspx

            if(ctype == SQL_C_WCHAR)
                column_size = value.second.size()/2;
            else if(ctype == SQL_C_CHAR)
                column_size = value.second.size();

            if(value.second.empty())
                column_size = 1;

            throw_on_error_("SQLBindParameter") = SQLBindParameter(
                stmt_.get(),
                bind_col_,
                SQL_PARAM_INPUT,
                ctype,
                desc.data_type_,
                column_size,                    // Column size
                desc.decimal_digits_,           // Precision
                (void*)value.second.c_str(),    // string
                value.second.size(),
                &value.first);
        }
    }

private:
    // Read only members, filled during constructions

    const common_data* cd_;
    stmt_handle stmt_;
    bool prepared_;                     // Should be statement prepared or executed immediatelly

    vector<param_desc> params_desc_;    // Remember all parameter descriptions, extracted by SQLDescribeParam 
                                        // Empty if api for SQLDescribeParam or SQLNumParams is not supported by backend.

    // Read write members, modified during statement life

    vector<holder_sp> params_;          // Contain data for parameters bound by SQLBindParameter. Data owned here will be referenced by 
                                        // ODBC driver during SQLExecute or SQLExecuteDirect invocation

    int bind_col_;                      // Transfer current bind column index from bind_impl to visitation operator()

    error_checker throw_on_error_;      // Utility for checking api return codes, 

    static param_desc s_generic_null_desc;
};

statement::param_desc statement::s_generic_null_desc = {SQL_CHAR, 0, 0, 1};

class connection : public backend::connection, private common_data
{
public:
    connection(const conn_info& ci, session_monitor* sm) : backend::connection(ci, sm), ci_(ci)
    {
        string_ref utf = ci.get("@utf", "narrow");
        const locale& loc = locale::classic();

        if(boost::iequals(utf, "narrow", loc))
            wide_ = false;
        else if(boost::iequals(utf, "wide", loc))
            wide_ = true;
        else
            throw edba_error("edba::odbc:: @utf property can be either 'narrow' or 'wide'");

        SQLRETURN r = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, env_.ptr());

        if(!SQL_SUCCEEDED(r))
            throw edba_error("edba::odbc:: failed to allocate environment handle");

        error_checker throw_on_env_error(wide_, env_.get(), SQL_HANDLE_ENV);

        throw_on_env_error("SQLSetEnvAttr") = SQLSetEnvAttr(env_.get(), SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);

        throw_on_env_error("SQLAllocHandle") = SQLAllocHandle(SQL_HANDLE_DBC, env_.get(), dbc_.ptr());

        error_checker throw_on_dbc_error(wide_, dbc_.get(), SQL_HANDLE_DBC);

        if(wide_) 
        {
            throw_on_dbc_error("SQLDriverConnectW") = SQLDriverConnectW(dbc_.get(), 0,
                (SQLWCHAR*)utf_to_utf<SQLWCHAR>(ci.conn_string()).c_str(),
                SQL_NTS, 0, 0, 0, SQL_DRIVER_COMPLETE);
        }
        else 
        {
            throw_on_dbc_error("SQLDriverConnectA") = SQLDriverConnectA(dbc_.get(), 0,
                (SQLCHAR*)ci.conn_string().c_str(),
                SQL_NTS, 0, 0, 0, SQL_DRIVER_COMPLETE);
        }

        // TODO: Reimplement to use unicode SQLGetInfo when wide_ is true

        // remeber real database name and database version
        // get database name
        char buf[256];
        SQLSMALLINT len = 0;
        SQLRETURN rc = SQLGetInfoA(dbc_.get(), SQL_DBMS_NAME, &buf, sizeof buf, &len );

        if( SQL_SUCCESS == rc || SQL_SUCCESS_WITH_INFO == rc )
        {
            if (boost::iequals(buf, "Postgresql", loc))
                engine_ = "PgSQL";
            else
                engine_ = buf;
        }
        else
            engine_ = "Unknown";

        // get version
        rc = SQLGetInfoA(dbc_.get(), SQL_DBMS_VER, &buf, sizeof buf, &len );

        if( SQL_SUCCESS == rc || SQL_SUCCESS_WITH_INFO == rc )
        {
            if (2 != EDBA_SSCANF(buf, "%2d.%2d", &ver_major_, &ver_minor_))
                ver_major_ = ver_minor_ = -1;
        }

        // get user name
        rc = SQLGetInfoA(dbc_.get(), SQL_USER_NAME, &buf, sizeof buf, &len );

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
            const string& eng=engine();
            if(boost::iequals(eng, "sqlite3", loc))
                last_insert_id_ = "select last_insert_rowid()";
            else if(boost::iequals(eng, "mysql", loc))
                last_insert_id_ = "select last_insert_id()";
            else if(boost::iequals(eng, "pgsql", loc))
            {
                last_insert_id_ = "select lastval()";
                sequence_last_ = "select currval(:seqname)";
            }
            else if(boost::iequals(eng, "Microsoft SQL Server", loc))
                last_insert_id_ = "select @@identity";
        }
        else
        {
            // TODO: avoid copying
            if(find(seq.begin(), seq.end(), '?') == seq.end())
                last_insert_id_.assign(seq.begin(), seq.end());
            else
                sequence_last_.assign(seq.begin(), seq.end());
        }

        // 
        throw_on_dbc_error("SQLGetInfoA(..., SQL_CURSOR_COMMIT_BEHAVIOR,...)") = SQLGetInfoA(
            dbc_.get(), SQL_CURSOR_COMMIT_BEHAVIOR, &commit_behavior_, sizeof(commit_behavior_), &len);

        throw_on_dbc_error("SQLGetInfoA(..., SQL_CURSOR_ROLLBACK_BEHAVIOR,...)") = SQLGetInfoA(
            dbc_.get(), SQL_CURSOR_ROLLBACK_BEHAVIOR, &rollback_behavior_, sizeof(rollback_behavior_), &len);
    }

    ~connection()
    {
        if(dbc_.get())
            SQLDisconnect(dbc_.get());
    }

    /// API
    virtual void begin_impl()
    {
        set_autocommit(false);
    }

    virtual void commit_impl()
    {
        error_checker(wide_, dbc_.get(), SQL_HANDLE_DBC)("SQLEndTran") = SQLEndTran(SQL_HANDLE_DBC, dbc_.get(), SQL_COMMIT);
        set_autocommit(true);
    }

    virtual void rollback_impl()
    {
        try
        {
            error_checker(wide_, dbc_.get(), SQL_HANDLE_DBC)("SQLEndTran") = SQLEndTran(SQL_HANDLE_DBC, dbc_.get(), SQL_ROLLBACK);
        }
        catch(...) {}

        try
        {
            set_autocommit(true);
        }
        catch(...){}
    }

    statement_ptr real_prepare(const string_ref& q, bool prepared)
    {
        return boost::intrusive_ptr<statement>(new statement(this, sm_, q, prepared));
    }

    virtual backend::statement_ptr prepare_statement_impl(const string_ref& q)
    {
        return real_prepare(q, true);
    }

    virtual backend::statement_ptr create_statement_impl(const string_ref& q)
    {
        return real_prepare(q, false);
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

            if (boost::empty(query))
                continue;

            create_statement_impl(*spl_iter)->run_exec();
        }
    }

    virtual string escape(const string_ref&)
    {
        throw not_supported_by_backend("cppcms::odbc:: string escaping is not supported");
    }

    virtual const string& backend()
    {
        return g_backend;
    }

    virtual const string& engine()
    {
        return engine_;
    }

    virtual void version(int& major, int& minor)
    {
        major = ver_major_;
        minor = ver_minor_;
    }

    virtual const string& description()
    {
        return description_;
    }

    void set_autocommit(bool on)
    {
        SQLPOINTER mode = (SQLPOINTER)(on ? SQL_AUTOCOMMIT_ON : SQL_AUTOCOMMIT_OFF);
        error_checker(wide_, dbc_.get(), SQL_HANDLE_DBC)("SQLSetConnectAttr") = SQLSetConnectAttr(
            dbc_.get(), // handler
            SQL_ATTR_AUTOCOMMIT, // option
            mode, //value
            0);
    }

private:
    conn_info ci_;
};


}}}} // edba, backend, odbc, anonymous

extern "C" 
{
    EDBA_DRIVER_API edba::backend::connection *edba_odbc_get_connection(const edba::conn_info& cs, edba::session_monitor* sm)
    {
        return new edba::backend::odbc::connection(cs, sm);
    }
}
