#ifndef SRC_MEM_LOGGER_HPP_
#define SRC_MEM_LOGGER_HPP_

// C++ Standard Headers
#include <iostream>
#include <fstream>
#include <string>
#include <csignal>
#include <ctime>

// note that pretty much everything has to be static
// for the timer callback mechanism to work

class MemLogger {
 public :
    // constructor, parameters are..
    // output filestream for the log messages - defaults to cout
    // memory limit - defaults to 4GB
    // will asynchronously call the check_mem function every 'interval' milliseconds 
    // causes job to commit suicide if it goes over the memory limit
    MemLogger();

    // timer signal handler - logs memory usage and causes job to
    // commit suicide if over the memory limit
    static void check_mem(int, siginfo_t*, void*);
    static double get_mem_usage();
 
private :
    static std::string slurp(std::ifstream& in);

    static double mem_limit;
    static double current_mem;
    static uint64_t interval;
};

#endif

