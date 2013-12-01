#ifndef EDBA_DRIVER_MANAGER_HPP
#define EDBA_DRIVER_MANAGER_HPP

#include <edba/types.hpp>
#include <edba/conn_info.hpp>

namespace edba {

struct EDBA_API driver_manager
{
    static backend::connection_ptr create_conn(const conn_info& ci, session_monitor* sm);
};

}

#endif
