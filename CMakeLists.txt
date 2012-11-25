cmake_minimum_required(VERSION 2.8)

option(EDBA_INSTALL "Install edba libraries and backend. In case of standalone build this would be done automatically" OFF)
option(EDBA_ENABLE_TESTS "Add targets for test building. In case of standalone build this would be done automatically" OFF)

set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake ${CMAKE_MODULE_PATH})

function(edba_install_targets)
  if(EDBA_STANDALONE_BUILD OR EDBA_INSTALL)
    install(TARGETS ${ARGV} 
      RUNTIME DESTINATION bin
      ARCHIVE DESTINATION lib
      LIBRARY DESTINATION lib)
  endif()
endfunction(edba_install_targets)

# Figure out if we are included from another cmake
if(CMAKE_PARENT_LIST_FILE STREQUAL CMAKE_CURRENT_LIST_FILE)
	set(EDBA_STANDALONE_BUILD 1)
endif()

if(EDBA_STANDALONE_BUILD)
	set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/stage/bin)
	set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/stage/lib)
	set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/stage/lib)	
endif()

find_package(Boost 1.48.0 COMPONENTS date_time locale thread)

if (WIN32)
    add_definitions(-D_CRT_SECURE_NO_WARNINGS -D_SCL_SECURE_NO_WARNINGS -DBOOST_ALL_NO_LIB)
endif (WIN32)

include_directories(${Boost_INCLUDE_DIRS})
include_directories(${CMAKE_CURRENT_SOURCE_DIR})

set(sources
  edba/detail/exports.hpp
  edba/detail/utils.hpp
  edba/detail/utils.cpp
  edba/conn_info.hpp
  edba/conn_info.cpp
  edba/edba.hpp
  edba/errors.hpp
  edba/session.hpp
  edba/session_monitor.hpp
  edba/session_pool.hpp
  edba/session_pool.cpp
  edba/statement.hpp
  edba/string_ref.hpp
  edba/types.hpp
  edba/transaction.hpp
  edba/rowset.hpp
  edba/backend/bind_by_name_helper.hpp
  edba/backend/interfaces.hpp
  edba/backend/implementation_base.hpp
  edba/backend/implementation_base.cpp
  edba/driver/odbc.hpp
  edba/driver/sqlite3.hpp
  edba/driver/mysql.hpp
  edba/driver/oracle.hpp
  edba/driver/postgresql.hpp
  edba/driver/all.hpp
  edba/types_support/std_shared_ptr.hpp
  edba/types_support/std_unique_ptr.hpp
  edba/types_support/std_tuple.hpp
  edba/types_support/boost_shared_ptr.hpp
  edba/types_support/boost_scoped_ptr.hpp
  edba/types_support/boost_tuple.hpp
  edba/types_support/boost_optional.hpp
  edba/types_support/boost_fusion.hpp
  edba/types_support/boost_gregorian_date.hpp
  edba/types_support/boost_posix_time_ptime.hpp 
)

add_library(edba SHARED ${sources})
target_link_libraries(edba ${Boost_LIBRARIES})

foreach(src ${sources})
	string(REGEX REPLACE "(.*)/.*" "\\1" dir ${src})
	string(REPLACE "/" "\\" msvc_folder ${dir})
	source_group(${msvc_folder} FILES ${src})
endforeach(src ${sources})

add_subdirectory(backends)
add_subdirectory(tests)

edba_install_targets(edba)
