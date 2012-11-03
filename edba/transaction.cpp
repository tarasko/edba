#include <edba/transaction.hpp>
#include <edba/session.hpp>

namespace edba {

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

}
