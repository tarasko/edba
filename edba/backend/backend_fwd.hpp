#ifndef EDBA_BACKEND_FWD_HPP
#define EDBA_BACKEND_FWD_HPP

#include <edba/utils.hpp>

namespace edba { namespace backend {

class result;
class bindings;
class statement;
class connection;

EDBA_ADD_INTRUSIVE_PTR_SUPPORT_FOR_TYPE(result)
EDBA_ADD_INTRUSIVE_PTR_SUPPORT_FOR_TYPE(bindings)
EDBA_ADD_INTRUSIVE_PTR_SUPPORT_FOR_TYPE(statement)
EDBA_ADD_INTRUSIVE_PTR_SUPPORT_FOR_TYPE(connection)


}} // namespace edba, backend

#endif // EDBA_BACKEND_FWD_HPP
