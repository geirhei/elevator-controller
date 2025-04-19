add_subdirectory(ElevatorController)
add_subdirectory(Messaging)

add_executable("${PROJECT_NAME}" main.cpp)
target_link_libraries("${PROJECT_NAME}" PRIVATE elevator-controller-lib)