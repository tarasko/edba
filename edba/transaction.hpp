#ifndef EDBA_TRANSACTION_HPP
#define EDBA_TRANSACTION_HPP

#include <edba/string_ref.hpp>
#include <edba/backend/backend.hpp>

namespace edba {

class session;

///
/// \brief The transaction guard
///
/// This class is RAII transaction guard that causes automatic transaction rollback on stack unwind, unless
/// the transaction is committed
///
class EDBA_API transaction : boost::noncopyable 
{
public:
    ///
    /// Begin a transaction on session \a s, calls s.begin()
    ///
    transaction(session &s);
    ///
    /// If the transaction wasn't committed or rolled back calls session::rollback() for the session it was created with.
    ///
    ~transaction();
    ///
    /// Commit a transaction on the session.  Calls session::commit() for the session it was created with.
    ///
    void commit();
    ///
    /// Rollback a transaction on the session.  Calls session::rollback() for the session it was created with.
    ///
    void rollback();
private:
    session& s_;
    bool commited_;
};

}

#endif EDBA_TRANSACTION_HPP
