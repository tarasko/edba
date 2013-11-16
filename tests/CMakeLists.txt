find_package(Boost 1.48.0 COMPONENTS date_time locale thread unit_test_framework)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/..)

add_executable(edba.example.simple example_simple.cpp)
target_link_libraries(edba.example.simple edba ${Boost_LIBRARIES})

add_executable(edba.example.monitor example_monitor.cpp)
target_link_libraries(edba.example.monitor edba ${Boost_LIBRARIES})

add_executable(edba.tests 
	monitor.hpp
	backends_smoke_test.cpp
	types_support_test.cpp
	session_pool_test.cpp
	)

target_link_libraries(edba.tests edba ${Boost_LIBRARIES})
