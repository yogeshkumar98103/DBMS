cmake_minimum_required(VERSION 3.12)
project(DBMS)

set(CMAKE_CXX_STANDARD 17)
set(LNCURSES_COMPILE_FLAG "-lreadline")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${LNCURSES_COMPILE_FLAG}" )
#set_source_files_properties(main.cpp CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${LNCURSES_COMPILE_FLAG}")

add_executable(DBMS main.cpp Cursor.cpp Table.cpp TableManager.cpp string.cpp)
add_executable(ExtSort ExternalSortTest.cpp string.cpp)
set_target_properties(ExtSort PROPERTIES RUNTIME_OUTPUT_DIRECTORY ../ExtSort)