set(SPRINT_HAS_CUDA FALSE)

if(SPRINT_ENABLE_CUDA)
    # check for CUDA/nvcc
    include(CheckLanguage)
    check_language(CUDA)

    if (CMAKE_CUDA_COMPILER)
        # CUDA available
        enable_language(CUDA)

        find_package(CUDAToolkit REQUIRED)

        set(SPRINT_HAS_CUDA TRUE)

        if(NOT DEFINED CMAKE_CUDA_ARCHITECTURES)
            set(CMAKE_CUDA_ARCHITECTURES native)
        endif()


        message(STATUS "CUDA enabled. Arch: ${CMAKE_CUDA_ARCHITECTURES}. Toolkit version: ${CUDAToolkit_VERSION}")
    else()
        message(WARNING "CUDA requested but unavailable, falling back to CPU only.")
    endif()
else()
    message(STATUS "CUDA support disabled.")
endif()