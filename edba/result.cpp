#include <edba/result.hpp>
#include <edba/backend/backend.hpp>

namespace edba {

result::result() :
    eof_(false),
    fetched_(false),
    current_col_(0)
{
}

result::result(const boost::intrusive_ptr<backend::result>& res)
    : eof_(false)
    , fetched_(false)
    , current_col_(0)
    , res_(res)
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

int result::index(const string_ref& n)
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

int result::find_column(const string_ref& name)
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
bool result::is_null(const string_ref& n)
{
    return is_null(index(n));
}

bool result::fetch(int col, fetch_types_variant& v)
{
    return res_->fetch(col, v);
}

}
