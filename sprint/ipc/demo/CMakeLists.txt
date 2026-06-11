set(PROJECT sprint_ipc_demo)

project(
    ${PROJECT}
    VERSION ${CMAKE_PROJECT_VERSION}
    LANGUAGES CXX
)

add_executable(${PROJECT})

target_sources(${PROJECT}
    PRIVATE
        ${CMAKE_CURRENT_LIST_DIR}/src/main.cpp
)

target_include_directories(${PROJECT}
    PRIVATE
        ${CMAKE_CURRENT_LIST_DIR}/src
)

target_link_libraries(${PROJECT} PUBLIC sprint::ipc)

