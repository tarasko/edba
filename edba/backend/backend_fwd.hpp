#ifndef EDBA_BACKEND_FWD_HPP
#define EDBA_BACKEND_FWD_HPP

#include <edba/detail/utils.hpp>

namespace edba { 

class conn_info;
class session_monitor;

namespace backend {

class result;
class bindings;
class statement;
class connection;

EDBA_ADD_INTRUSIVE_PTR_SUPPORT_FOR_TYPE(result)
EDBA_ADD_INTRUSIVE_PTR_SUPPORT_FOR_TYPE(bindings)
EDBA_ADD_INTRUSIVE_PTR_SUPPORT_FOR_TYPE(statement)
EDBA_ADD_INTRUSIVE_PTR_SUPPORT_FOR_TYPE(connection)

}} // namespace edba, backend

///
/// This function type is the function that is generally resolved from the shared objects when loaded
///
extern "C" typedef edba::backend::connection* (*connect_function_type)(const edba::conn_info& cs, edba::session_monitor* sm);

namespace edba { namespace backend {

EDBA_API connect_function_type get_connect_function(const char* path, const char* driver);

}} // namespace edba, backend


#endif // EDBA_BACKEND_FWD_HPP
