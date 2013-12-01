#ifndef EDBA_TRANSACTION_HPP
#define EDBA_TRANSACTION_HPP

#include <edba/session.hpp>

namespace edba {

/// \brief The transaction guard
///
/// This class is RAII transaction guard that causes automatic transaction rollback on stack unwind, unless
/// the transaction is committed
class transaction : boost::noncopyable 
{
public:
    /// Begin a transaction on session \a s, calls s.begin()
    transaction(session &s) : s_(s), commited_(false)
    {
        s_.begin();
    }
   
    /// If the transaction wasn't committed or rolled back calls session::rollback() for the session it was created with.
    ~transaction()
    {
        rollback();
    }
    
    /// Commit a transaction on the session.  Calls session::commit() for the session it was created with.
    void commit()
    {
        s_.commit();
        commited_ = true;
    }

    /// Rollback a transaction on the session.  Calls session::rollback() for the session it was created with.
    void rollback()
    {
        if(!commited_)
            s_.rollback();
        commited_=true;
    }

private:
    session& s_;
    bool commited_;
};

}

#endif // EDBA_TRANSACTION_HPP
