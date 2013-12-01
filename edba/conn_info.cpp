#include <edba/conn_info.hpp>

#include <edba/detail/utils.hpp>

#include <boost/foreach.hpp>
#include <boost/typeof/typeof.hpp>
#include <boost/algorithm/string/find_iterator.hpp>

#include <map>

using namespace std;

namespace edba {

struct conn_info::data 
{
    struct string_ref_icmp
    {
        bool operator()(const string_ref& f, const string_ref& s) const
        {
            return boost::algorithm::ilexicographical_compare(f, s);
        }
    };

    typedef map<string_ref, string_ref, string_ref_icmp> rng_map;
    rng_map pairs_;
    string driver_name_;
    string clean_conn_string_;
};

conn_info::conn_info(const string_ref& conn_string) : data_(new data)
{
    using namespace boost;
    using namespace boost::algorithm;

    // First get driver name
    BOOST_AUTO(colon_iter, (find(conn_string.begin(), conn_string.end(), ':')));
    if (colon_iter == conn_string.end())
        throw invalid_connection_string("invalid_connection_string: " + to_string(conn_string) + " - driver name was not specified");

    BOOST_AUTO(driver_name_rng, (make_iterator_range(conn_string.begin(), colon_iter)));
    trim(driver_name_rng);
    data_->driver_name_.assign(driver_name_rng.begin(), driver_name_rng.end());

    // Iterate over all properties in connection string
    BOOST_AUTO(props, (make_iterator_range(++colon_iter, conn_string.end())));
    BOOST_AUTO(si, (make_split_iterator(props, first_finder(";"))));
    BOOST_TYPEOF(si) end_si;

    for(;si != end_si; ++si) 
    {
        string_ref trimmed_pair = trim(*si);

        // split by '=' on key and value
        const char* eq_sign = find(trimmed_pair.begin(), trimmed_pair.end(), '=');
        string_ref key(trimmed_pair.begin(), eq_sign);
        string_ref val;
        if (eq_sign != trimmed_pair.end()) 
            val = string_ref(eq_sign + 1, trimmed_pair.end());

        // trim key and value
        trim(key);
        trim(val);

        if (key.empty()) 
            continue;

        // insert pair to map
        data_->pairs_.insert(make_pair(key, val));

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

string_ref conn_info::get(const char* key, const char* def) const
{
    using namespace boost;

    BOOST_AUTO(res, data_->pairs_.find(as_literal(key)));
    return data_->pairs_.end() == res ? as_literal(def) : res->second;
}

string conn_info::get_copy(const char* key, const char* def) const
{
    return boost::copy_range<string>(get(key, def));
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

const string& conn_info::conn_string() const
{
    return data_->clean_conn_string_;
}

string conn_info::pgsql_conn_string() const
{
    string pq_str;

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

void conn_info::append_escaped(const string_ref& rng, string& dst)
{
    for(string_ref::difference_type i=0; i < rng.size(); ++i) 
    {
        if(rng[i]=='\\')
            dst+="\\\\";
        else if(rng[i]=='\'')
            dst+="\\\'";
        else
            dst+=rng[i];
    }
}

string_ref conn_info::driver_name() const
{
    return data_->driver_name_;
}

}
