# Use it inside backend to add backend static and shared libraries into solution
function(edba_add_backend backend_name)
	string(TOUPPER ${backend_name} upper_backend_name)
	string(TOLOWER ${backend_name} lower_backend_name)

	if(${upper_backend_name}_FOUND)
		message(STATUS "${backend_name} - Found")
		message(STATUS "${upper_backend_name}_INCLUDE_DIR: " ${${upper_backend_name}_INCLUDE_DIR})
		message(STATUS "${upper_backend_name}_LIBRARIES: " ${${upper_backend_name}_LIBRARIES})
		
		set(source ${lower_backend_name}_backend.cpp)

		include_directories(${${upper_backend_name}_INCLUDE_DIR})

		if(EDBA_BACKEND_SHARED OR EDBA_S_BACKEND_SHARED)
			add_library(edba_${lower_backend_name} SHARED ${source})
			target_link_libraries(edba_${lower_backend_name} ${${upper_backend_name}_LIBRARIES} edba)
			set_target_properties(edba_${lower_backend_name} PROPERTIES COMPILE_DEFINITIONS "edba_DRIVER_EXPORTS")
		endif()
	  
		if(NOT EDBA_BACKEND_SHARED OR NOT EDBA_S_BACKEND_SHARED)
			add_library(edba_${lower_backend_name}_s STATIC ${source})
			set_target_properties(edba_${lower_backend_name}_s PROPERTIES COMPILE_DEFINITIONS "edba_STATIC;edba_DRIVER_STATIC")
		endif()
	  
		edba_install_targets(edba_${lower_backend_name} edba_${lower_backend_name}_s)
	endif()
endfunction(edba_add_backend)

# Lookup client API using find_package and if API found then add backend subdirectory
function(edba_find_backend backend_name)
	find_package(${backend_name})

	string(TOUPPER ${backend_name} upper_backend_name)
	string(TOLOWER ${backend_name} lower_backend_name)

	add_subdirectory(${lower_backend_name})	
endfunction(edba_find_backend)

edba_find_backend(MySQL)
edba_find_backend(PostgreSQL)
edba_find_backend(ODBC)
edba_find_backend(Oracle)
edba_find_backend(SQLite3)
