cmake_minimum_required(VERSION 3.25)
project(admintool C)

set(CMAKE_C_STANDARD 11)

add_executable(admintool main.c)
target_link_libraries(admintool tidesdb m)
