# Use it inside backend to add backend static and shared libraries into solution
function(edba_add_backend backend_name)
	string(TOUPPER ${backend_name} upper_backend_name)
	string(TOLOWER ${backend_name} lower_backend_name)

	set(source ${lower_backend_name}_backend.cpp)

	include_directories(${${upper_backend_name}_INCLUDE_DIR})

	add_library(edba_${lower_backend_name} SHARED ${source})
	add_library(edba_${lower_backend_name}_s STATIC ${source})

	target_link_libraries(edba_${lower_backend_name} ${${upper_backend_name}_LIBRARIES} edba)
	target_link_libraries(edba_${lower_backend_name}_s ${${upper_backend_name}_LIBRARIES} edba)

	set_target_properties(edba_${lower_backend_name} PROPERTIES COMPILE_DEFINITIONS "edba_DRIVER_EXPORTS")
  
  edba_install_targets(edba_${lower_backend_name} edba_${lower_backend_name}_s)
endfunction(edba_add_backend)

# Lookup client API using find_package and if API found then add backend subdirectory
function(edba_find_backend backend_name)
  find_package(${backend_name})
  
	string(TOUPPER ${backend_name} upper_backend_name)
	string(TOLOWER ${backend_name} lower_backend_name)
    
  if(${upper_backend_name}_FOUND)
    message("${backend_name} - Found")
    message("${upper_backend_name}_INCLUDE_DIR: " ${${upper_backend_name}_INCLUDE_DIR})
    message("${upper_backend_name}_LIBRARIES: " ${${upper_backend_name}_LIBRARIES})
    add_subdirectory(${lower_backend_name})
  endif()
endfunction(edba_find_backend)

edba_find_backend(MySQL)
edba_find_backend(PostgreSQL)
edba_find_backend(ODBC)
edba_find_backend(Oracle)
edba_find_backend(SQLite3)
