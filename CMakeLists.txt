cmake_minimum_required(VERSION 3.13)

set(CMAKE_C_COMPILER "gcc")
set(CMAKE_CXX_COMPILER "g++")

project(redis-starter-cpp)
set(CMAKE_CXX_STANDARD 11) # Enable the C++20 standard

set(SOURCE_FILES src/Server.cpp src/string_utils.cpp)

add_executable(server ${SOURCE_FILES})

# Link with pthread
target_link_libraries(server pthread)



# FIND_PACKAGE( Boost 1.70 COMPONENTS program_options REQUIRED )
# message("Boost lib: ${Boost_LIBRARIES}")
# target_link_libraries(server ${Boost_LIBRARIES})
