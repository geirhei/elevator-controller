#include "ElevatorController/Controller.hpp"

#include <iostream>

using namespace ElevatorController;

int main()
{
    std::cout << "Hello World!\n";
    Controller controller {};
    controller.run();
    return 0;
}