#ifndef EDBA_DRIVER_MANAGER_HPP
#define EDBA_DRIVER_MANAGER_HPP

#include <edba/types.hpp>

namespace edba {

struct EDBA_API driver_manager
{
    backend::connection_ptr create_conn(const conn_info& ci) const;
};

}

#endif
