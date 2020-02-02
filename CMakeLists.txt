cmake_minimum_required(VERSION 3.12)
project(DBMS)

set(CMAKE_CXX_STANDARD 17)

add_executable(DBMS main.cpp Cursor.cpp Table.cpp Pager.cpp)