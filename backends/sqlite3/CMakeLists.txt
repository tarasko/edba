if(NOT SQLITE3_FOUND AND NOT EDBA_BACKEND_SQLITE3_DONT_USE_AMALGAMATION)
	add_subdirectory(amalgamation)
	message(STATUS "SQLite3 Amalgamation has been used to enable SQLite3 backend")
endif()

edba_add_backend(sqlite3)