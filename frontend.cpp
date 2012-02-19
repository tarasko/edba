#define EDBA_SOURCE

#include <edba/frontend.hpp>
#include <edba/backend.hpp>

#include <boost/algorithm/string/find_iterator.hpp>
#include <boost/algorithm/string/finder.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/range/as_literal.hpp>
#include <boost/typeof/typeof.hpp>
#include <boost/foreach.hpp>

#ifdef _WIN32 
#  define EDBA_MEMCPY(Dst, BufSize, Src, ToCopy) memcpy_s(Dst, BufSize, Src, ToCopy)
#else 
#  define EDBA_MEMCPY(Dst, BufSize, Src, ToCopy) memcpy(Dst, Src, ToCopy)
#endif

namespace edba {

result::result() :
    eof_(false),
    fetched_(false),
    current_col_(0)
{
}

result::result(	
    const boost::intrusive_ptr<backend::result>& res,
    const boost::intrusive_ptr<backend::statement>& stat,
    const boost::intrusive_ptr<backend::connection>& conn
    )
    : eof_(false),
    fetched_(false),
    current_col_(0),
    res_(res),
    stat_(stat),
    conn_(conn)
{
}

int result::cols()
{
    return res_->cols();
}

bool result::next()
{
    if(eof_)
        return false;
    fetched_=true;
    eof_ = res_->next()==false;
    current_col_ = 0;
    return !eof_;
}

int result::index(const chptr_range& n)
{
    int c = res_->name_to_column(n);
    if(c<0)
        throw invalid_column();
    return c;
}

std::string result::name(int col)
{
    if(col < 0 || col>= cols())
        throw invalid_column();
    return res_->column_to_name(col);
}

int result::find_column(const chptr_range& name)
{
    int c = res_->name_to_column(name);
    if(c < 0)
        return -1;
    return c;
}

void result::rewind_column()
{
    current_col_ = 0;
}

bool result::empty()
{
    if(!res_)
        return true;
    return eof_ || !fetched_;
}

void result::clear()
{
    eof_ = true;
    fetched_ = true;
    res_.reset();
    stat_.reset();
    conn_.reset();
}

void result::check()
{
    if(empty())
        throw empty_row_access();
}

bool result::is_null(int col)
{
    return res_->is_null(col);
}
bool result::is_null(const chptr_range& n)
{
    return is_null(index(n));
}


bool result::fetch(int col,short &v) { return res_->fetch(col,v); }
bool result::fetch(int col,unsigned short &v) { return res_->fetch(col,v); }
bool result::fetch(int col,int &v) { return res_->fetch(col,v); }
bool result::fetch(int col,unsigned &v) { return res_->fetch(col,v); }
bool result::fetch(int col,long &v) { return res_->fetch(col,v); }
bool result::fetch(int col,unsigned long &v) { return res_->fetch(col,v); }
bool result::fetch(int col,long long &v) { return res_->fetch(col,v); }
bool result::fetch(int col,unsigned long long &v) { return res_->fetch(col,v); }
bool result::fetch(int col,float &v) { return res_->fetch(col,v); }
bool result::fetch(int col,double &v) { return res_->fetch(col,v); }
bool result::fetch(int col,long double &v) { return res_->fetch(col,v); }
bool result::fetch(int col,std::string &v) { return res_->fetch(col,v); }
bool result::fetch(int col,std::tm &v) { return res_->fetch(col,v); }
bool result::fetch(int col,std::ostream &v) { return res_->fetch(col,v); }

bool result::fetch(const chptr_range& n,short &v) { return res_->fetch(index(n),v); }
bool result::fetch(const chptr_range& n,unsigned short &v) { return res_->fetch(index(n),v); }
bool result::fetch(const chptr_range& n,int &v) { return res_->fetch(index(n),v); }
bool result::fetch(const chptr_range& n,unsigned &v) { return res_->fetch(index(n),v); }
bool result::fetch(const chptr_range& n,long &v) { return res_->fetch(index(n),v); }
bool result::fetch(const chptr_range& n,unsigned long &v) { return res_->fetch(index(n),v); }
bool result::fetch(const chptr_range& n,long long &v) { return res_->fetch(index(n),v); }
bool result::fetch(const chptr_range& n,unsigned long long &v) { return res_->fetch(index(n),v); }
bool result::fetch(const chptr_range& n,float &v) { return res_->fetch(index(n),v); }
bool result::fetch(const chptr_range& n,double &v) { return res_->fetch(index(n),v); }
bool result::fetch(const chptr_range& n,long double &v) { return res_->fetch(index(n),v); }
bool result::fetch(const chptr_range& n,std::string &v) { return res_->fetch(index(n),v); }
bool result::fetch(const chptr_range& n,std::tm &v) { return res_->fetch(index(n),v); }
bool result::fetch(const chptr_range& n,std::ostream &v) { return res_->fetch(index(n),v); }

bool result::fetch(short &v) { return res_->fetch(current_col_++,v); }
bool result::fetch(unsigned short &v) { return res_->fetch(current_col_++,v); }
bool result::fetch(int &v) { return res_->fetch(current_col_++,v); }
bool result::fetch(unsigned &v) { return res_->fetch(current_col_++,v); }
bool result::fetch(long &v) { return res_->fetch(current_col_++,v); }
bool result::fetch(unsigned long &v) { return res_->fetch(current_col_++,v); }
bool result::fetch(long long &v) { return res_->fetch(current_col_++,v); }
bool result::fetch(unsigned long long &v) { return res_->fetch(current_col_++,v); }
bool result::fetch(float &v) { return res_->fetch(current_col_++,v); }
bool result::fetch(double &v) { return res_->fetch(current_col_++,v); }
bool result::fetch(long double &v) { return res_->fetch(current_col_++,v); }
bool result::fetch(std::string &v) { return res_->fetch(current_col_++,v); }
bool result::fetch(std::tm &v) { return res_->fetch(current_col_++,v); }
bool result::fetch(std::ostream &v) { return res_->fetch(current_col_++,v); }

statement::statement() : placeholder_(1) {}

statement::statement(
    const boost::intrusive_ptr<backend::statement>& stat,
    const boost::intrusive_ptr<backend::connection>& conn
    ) 
    : placeholder_(1),
    stat_(stat),
    conn_(conn)
{
}

void statement::reset()
{
    placeholder_ = 1;
    stat_->reset();
}

statement &statement::operator<<(std::string const &v)
{
    return bind(v);
}
statement &statement::operator<<(char const *s)
{
    return bind(s);
}

statement &statement::operator<<(std::tm const &v)
{
    return bind(v);
}

statement &statement::operator<<(std::istream &v)
{
    return bind(v);
}

statement &statement::operator<<(void (*manipulator)(statement &st))
{
    manipulator(*this);
    return *this;
}
result statement::operator<<(result (*manipulator)(statement &st))
{
    return manipulator(*this);
}

statement &statement::bind(int v)
{
    stat_->bind(placeholder_++,v);
    return *this;
}
statement &statement::bind(unsigned v)
{
    stat_->bind(placeholder_++,v);
    return *this;
}
statement &statement::bind(long v)
{
    stat_->bind(placeholder_++,v);
    return *this;
}
statement &statement::bind(unsigned long v)
{
    stat_->bind(placeholder_++,v);
    return *this;
}
statement &statement::bind(long long v)
{
    stat_->bind(placeholder_++,v);
    return *this;
}
statement &statement::bind(unsigned long long v)
{
    stat_->bind(placeholder_++,v);
    return *this;
}
statement &statement::bind(double v)
{
    stat_->bind(placeholder_++,v);
    return *this;
}
statement &statement::bind(long double v)
{
    stat_->bind(placeholder_++,v);
    return *this;
}

statement &statement::bind(std::string const &v)
{
    stat_->bind(placeholder_++,chptr_range(v));
    return *this;
}
statement &statement::bind(char const *s)
{
    stat_->bind(placeholder_++,chptr_range(s));
    return *this;
}
statement &statement::bind(char const *b,char const *e)
{
    stat_->bind(placeholder_++,chptr_range(b,e));
    return *this;
}
statement &statement::bind(std::tm const &v)
{
    stat_->bind(placeholder_++,v);
    return *this;
}
statement &statement::bind(std::istream &v)
{
    stat_->bind(placeholder_++,v);
    return *this;
}
statement &statement::bind_null()
{
    stat_->bind_null(placeholder_++);
    return *this;
}


void statement::bind(int col,std::string const &v)
{
    stat_->bind(col,chptr_range(v));
}
void statement::bind(int col,char const *s)
{
    stat_->bind(col,chptr_range(s));
}
void statement::bind(int col,char const *b,char const *e)
{
    stat_->bind(col,chptr_range(b,e));
}
void statement::bind(int col,std::tm const &v)
{
    stat_->bind(col,v);
}
void statement::bind(int col,std::istream &v)
{
    stat_->bind(col,v);
}
void statement::bind(int col,int v)
{
    stat_->bind(col,v);
}
void statement::bind(int col,unsigned v)
{
    stat_->bind(col,v);
}
void statement::bind(int col,long v)
{
    stat_->bind(col,v);
}
void statement::bind(int col,unsigned long v)
{
    stat_->bind(col,v);
}
void statement::bind(int col,long long v)
{
    stat_->bind(col,v);
}
void statement::bind(int col,unsigned long long v)
{
    stat_->bind(col,v);
}
void statement::bind(int col,double v)
{
    stat_->bind(col,v);
}
void statement::bind(int col,long double v)
{
    stat_->bind(col,v);
}
void statement::bind_null(int col)
{
    stat_->bind_null(col);
}

long long statement::last_insert_id()
{
    return stat_->sequence_last(std::string());
}

long long statement::sequence_last(std::string const &seq)
{
    return stat_->sequence_last(seq);
}
unsigned long long statement::affected()
{
    return stat_->affected();
}

result statement::row()
{
    boost::intrusive_ptr<backend::result> backend_res(stat_->query());
    result res(backend_res,stat_,conn_);
    if(res.next()) {
        if(res.res_->has_next() == backend::result::next_row_exists) {
            throw multiple_rows_query();
        }
    }
    return res;
}

result statement::query()
{
    if (!stat_)
        throw empty_string_query();

    boost::intrusive_ptr<backend::result> res(stat_->query());
    return result(res,stat_,conn_);
}
statement::operator result()
{
    return query();
}
void statement::exec() 
{
    if (stat_)
        stat_->exec();
}

session::session()
{
}
session::session(const boost::intrusive_ptr<backend::connection>& conn) : conn_(conn) 
{
}

void session::close()
{
    conn_.reset();
}

bool session::is_open()
{
    return conn_;
}

statement session::prepare(const chptr_range& query)
{
    boost::intrusive_ptr<backend::statement> stat_ptr(conn_->prepare(query));
    statement stat(stat_ptr,conn_);
    return stat;
}

statement session::create_statement(const chptr_range& query)
{
    boost::intrusive_ptr<backend::statement> stat_ptr(conn_->get_statement(query));
    statement stat(stat_ptr,conn_);
    return stat;
}

statement session::create_prepared_statement(const chptr_range& query)
{
    boost::intrusive_ptr<backend::statement> stat_ptr(conn_->get_prepared_statement(query));
    statement stat(stat_ptr,conn_);
    return stat;
}

void session::exec_batch(const chptr_range& q)
{
    conn_->exec_batch(q);
}

statement session::operator<<(std::string const &q)
{
    return prepare(q);
}
statement session::operator<<(char const *s)
{
    return prepare(s);
}
void session::begin()
{
    conn_->begin();
}
void session::commit()
{
    conn_->commit();
}
void session::rollback()
{
    conn_->rollback();
}
std::string session::escape(char const *b,char const *e)
{
    return conn_->escape(b,e);
}
std::string session::escape(char const *s)
{
    return conn_->escape(s);
}
std::string session::escape(std::string const &s)
{
    return conn_->escape(s);
}
const std::string& session::backend()
{
    return conn_->backend();
}
const std::string& session::engine()
{
    return conn_->engine();
}
void session::version(int& major, int& minor)
{
    conn_->version(major, minor);
}
const std::string& session::description()
{
    return conn_->description();
}

transaction::transaction(session& s) : s_(s), commited_(false)
{
    s_.begin();
}

void transaction::commit()
{
    s_.commit();
    commited_ =true;
}
void transaction::rollback()
{
    if(!commited_)
        s_.rollback();
    commited_=true;
}
transaction::~transaction()
{
    rollback();
}

struct conn_info::data 
{
    struct chptr_range_icmp
    {
        bool operator()(const chptr_range& f, const chptr_range& s) const
        {
            return boost::algorithm::ilexicographical_compare(f, s);
        }
    };

    typedef std::map<chptr_range, chptr_range, chptr_range_icmp> rng_map;
    rng_map pairs_;
    std::string clean_conn_string_;
};

conn_info::conn_info(const char* cs) : data_(new data)
{
    using namespace boost;
    using namespace boost::algorithm;

    chptr_range rng = as_literal(cs);
    BOOST_AUTO(si, (make_split_iterator(rng, first_finder(";"))));
    BOOST_TYPEOF(si) end_si;

    // iterate over pairs
    for(;si != end_si; ++si) 
    {
        chptr_range trimmed_pair = trim(*si);

        // split by '=' on key and value
        const char* eq_sign = std::find(trimmed_pair.begin(), trimmed_pair.end(), '=');
        chptr_range key(trimmed_pair.begin(), eq_sign);
        chptr_range val;
        if (eq_sign != trimmed_pair.end()) 
            val = chptr_range(eq_sign + 1, trimmed_pair.end());

        // trim key and value
        trim(key);
        trim(val);

        if (key.empty()) 
            continue;

        // insert pair to map
        data_->pairs_.insert(std::make_pair(key, val));

        // prepare clean connection string without edba specific tags
        if('@' == key.front()) 
            continue;

        data_->clean_conn_string_.append(key.begin(), key.end());
        data_->clean_conn_string_.append("=");
        data_->clean_conn_string_.append(val.begin(), val.end());
        data_->clean_conn_string_.append("; ");
    }
}

bool conn_info::has(const char* key) const
{
    using namespace boost;
    return data_->pairs_.find(as_literal(key)) != data_->pairs_.end();
}

chptr_range conn_info::get(const char* key, const char* def) const
{
    using namespace boost;

    BOOST_AUTO(res, data_->pairs_.find(as_literal(key)));
    return data_->pairs_.end() == res ? as_literal(def) : res->second;
}

std::string conn_info::get_copy(const char* key, const char* def) const
{
    return boost::copy_range<std::string>(get(key, def));
}

int conn_info::get(const char* key, int def) const
{
    using namespace boost;

    BOOST_AUTO(res, data_->pairs_.find(as_literal(key)));
    if (data_->pairs_.end() == res || res->second.empty()) 
        return def;

    char buf[20] = {0};
    EDBA_MEMCPY(buf, 20, res->second.begin(), res->second.size());

    return atoi(buf);
}

const std::string& conn_info::conn_string() const
{
    return data_->clean_conn_string_;
}

std::string conn_info::pgsql_conn_string() const
{
    std::string pq_str;

    BOOST_FOREACH(data::rng_map::const_reference p, data_->pairs_)
    {
        if(p.first[0]=='@')
            continue;

        pq_str.append(p.first.begin(), p.first.end());
        pq_str += "='";
        append_escaped(p.second, pq_str);
        pq_str += "' ";
    }

    return pq_str;
}

void conn_info::append_escaped(const chptr_range& rng, std::string& dst)
{
    for(chptr_range::difference_type i=0; i < rng.size(); ++i) {
        if(rng[i]=='\\')
            dst+="\\\\";
        else if(rng[i]=='\'')
            dst+="\\\'";
        else
            dst+=rng[i];
    }
}

#if defined(_WIN32)
#  include <windows.h>
#  define RTLD_LAZY 0

namespace {
    void *dlopen(char const *name,int /*unused*/)
    {
        return LoadLibrary(name);
    }
    void dlclose(void *h)
    {
        HMODULE m=(HMODULE)(h);
        FreeLibrary(m);
    }
    void *dlsym(void *h,char const *sym)
    {
        HMODULE m=(HMODULE)(h);
        return (void *)GetProcAddress(m,sym);
    }
}

#else
#	include <dlfcn.h>
#endif

loadable_driver::loadable_driver(const char* path, const char* driver)
{
    assert("Path not null" && path);

    module_ = dlopen(path, RTLD_LAZY);

    if (!module_)
        throw edba_error("edba::loadable_driver::failed to load " + std::string(path));

    std::string entry_func_name("edba_");
    entry_func_name += driver;
    entry_func_name += "_get_connection";

    connect_ = reinterpret_cast<connect_function_type>(
        dlsym(module_, entry_func_name.c_str())
        );

    if (!connect_)
    {
        dlclose(module_);
        throw edba_error("edba::loadable_driver::failed to get " + entry_func_name + " address in " + std::string(path));
    }
}

loadable_driver::~loadable_driver()
{
    if (module_)
        dlclose(module_);
}

session loadable_driver::open(const conn_info& ci, session_monitor* sm)
{
    return session(boost::intrusive_ptr<edba::backend::connection>(connect_(ci, sm)));
}

session static_driver::open(const conn_info& ci, session_monitor* sm)
{
    return session(boost::intrusive_ptr<edba::backend::connection>(connect_(ci, sm)));
}

}  // edba
