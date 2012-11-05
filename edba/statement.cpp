#include <edba/statement.hpp>
#include <edba/backend/backend.hpp>

namespace edba {

statement::statement() : placeholder_(1) {}

statement::statement(const boost::intrusive_ptr<backend::statement>& stat) 
    : placeholder_(1)
    , stat_(stat)
{
}

void statement::reset()
{
    placeholder_ = 1;
    stat_->bindings().reset();
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
    result res(stat_->query());

    if(res.next()) 
    {
        if(res.res_->has_next() == backend::result::next_row_exists)
            throw multiple_rows_query();
    }
    return res;
}

result statement::query()
{
    if (!stat_)
        throw empty_string_query();

    return result(stat_->query());
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

statement& statement::bind(int col, const bind_types_variant& v)
{
    stat_->bindings().bind(col, v);
    return *this;
}

statement& statement::bind(const string_ref& name, const bind_types_variant& v)
{
    stat_->bindings().bind(name, v);
    return *this;
}


}
