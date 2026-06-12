set(PROJECT sprint_ipc)

project(
    ${PROJECT}
    VERSION ${CMAKE_PROJECT_VERSION}
    LANGUAGES CXX
)

add_library(${PROJECT})
add_library(sprint::ipc ALIAS ${PROJECT})

target_sources(${PROJECT}
    PRIVATE
        ${CMAKE_CURRENT_LIST_DIR}/src/backend/posix.cpp
        ${CMAKE_CURRENT_LIST_DIR}/src/ipc.cpp
)


target_include_directories(${PROJECT}
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_link_libraries(${PROJECT} PUBLIC pthread)

if(SPRINT_HAS_CUDA)
    target_compile_definitions(${PROJECT} PUBLIC SPRINT_IPC_ENABLE_CUDA)
    target_link_libraries(${PROJECT} PUBLIC CUDA::cudart)
endif()