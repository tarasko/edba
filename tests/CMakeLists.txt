find_package(Boost 1.48.0 COMPONENTS date_time locale thread unit_test_framework)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/..)

add_executable(edba.example example.cpp monitor.hpp)
target_link_libraries(edba.example edba edba_odbc ${Boost_LIBRARIES})

add_executable(edba.tests.backends_smoke_test backends_smoke_test.cpp monitor.hpp)
target_link_libraries(edba.tests.backends_smoke_test edba edba_odbc ${Boost_LIBRARIES})

add_executable(edba.tests.types_support_test types_support_test.cpp monitor.hpp)
target_link_libraries(edba.tests.types_support_test edba edba_sqlite3 ${Boost_LIBRARIES})

add_executable(edba.tests.session_pool_test session_pool_test.cpp monitor.hpp)
target_link_libraries(edba.tests.session_pool_test edba edba_sqlite3 ${Boost_LIBRARIES})

