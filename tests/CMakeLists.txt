 include_directories(${CMAKE_CURRENT_SOURCE_DIR}/..)
add_executable(edba.example example.cpp)
target_link_libraries(edba.example edba edba_odbc_s ${Boost_LIBRARIES})

add_executable(edba.tests.odbc odbc_test.cpp)
target_link_libraries(edba.tests.odbc edba edba_odbc_s ${Boost_LIBRARIES})
