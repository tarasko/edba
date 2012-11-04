find_package(Boost 1.48.0 COMPONENTS iostreams)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/..)
add_executable(edba.example example.cpp)
target_link_libraries(edba.example edba ${Boost_LIBRARIES})
