#include <cstdint>
#include <csignal>
#include <sstream>

// linux system headers
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

// boost headers
#include <boost/lexical_cast.hpp>
#include <boost/regex.hpp>

// google libraries
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "MemLogger.hpp"

// define command-line flags
DEFINE_double(mem_limit, 4000.0, "Memory Limit (MB)");
DEFINE_uint64(mem_check_interval, 60, "memory check interval (seconds)");

// the static members...
double MemLogger::mem_limit=4.0*1024.0*1024.0*1024.0; // 4GB in bytes
uint64_t MemLogger::interval=60*1000;  // milliseconds

MemLogger::MemLogger() {
    // reset the static members
	mem_limit=FLAGS_mem_limit*1024.0*1024.0;
	interval=FLAGS_mem_check_interval*1000;

	// set up the signal handler
	timer_t timerid;
	int sig_no=SIGRTMIN;
	int sec=interval/1000;
	int ms=interval % 1000;
	struct sigaction sa;
	sa.sa_flags = SA_SIGINFO;
	sa.sa_sigaction = check_mem;
	sigemptyset(&sa.sa_mask);
	sigaction(sig_no, &sa, 0);

	// Create Timer
	struct sigevent se;
	se.sigev_notify=SIGEV_SIGNAL;
	se.sigev_signo=sig_no;
	se.sigev_value.sival_ptr = &timerid;
	timer_create(CLOCK_REALTIME, &se, &timerid);

	// enable it
	struct itimerspec	value;
	value.it_interval.tv_sec = sec;
	value.it_interval.tv_nsec = ms*1000000L;
	value.it_value.tv_sec = sec;
	value.it_value.tv_nsec = ms*1000000L;
	timer_settime (timerid, 0, &value, NULL);
}

// memory check funtion
void MemLogger::check_mem(int, siginfo_t*, void*) {
    double current=get_mem_usage();
	if (current>mem_limit) {
        LOG(INFO) << "MEM USAGE " << current << "MB ABOVE LIMIT " << current << "MB, COMMITTING SUICIDE!"<< std::endl;
		std::exit(1);
	} else {
    	LOG(INFO) << "MEM USAGE : " << current << "MB" << std::endl;
    }
}

// get the memory usage (in MB) for the current process
// from /proc/<pid>/statm
double MemLogger::get_mem_usage() {
	pid_t pid=getpid();
	std::string fname="/proc/"+boost::lexical_cast<std::string>(pid)+"/statm";
	std::ifstream statm_f(fname.c_str());
	std::string statm_str=slurp(statm_f);
	statm_f.close();
	static boost::regex re_statm("^(\\d+)\\s+(\\d+)\\s+(\\d+)\\s+(\\d+)\\s+(\\d+)\\s+(\\d+)\\s+(\\d+).*$");
	boost::cmatch matches; 
	double mem=0.0;
	if (boost::regex_match(statm_str.c_str(), matches, re_statm)) {
		long size=boost::lexical_cast<long>(matches[1]);
		long resident=boost::lexical_cast<long>(matches[2]);
		//long share=boost::lexical_cast<long>(matches[3]);
		//long text=boost::lexical_cast<long>(matches[4]);
		//long lib=boost::lexical_cast<long>(matches[5]);
		//long data=boost::lexical_cast<long>(matches[6]);
		//long dt=boost::lexical_cast<long>(matches[7]);
		mem=1.0*static_cast<double>(size+resident)*4096.0/(1024.0*1024.0);
	}
	return mem;
}

// read the entire contents of a file into a string
std::string MemLogger::slurp(std::ifstream& in) {
    std::stringstream sstr;
    sstr << in.rdbuf();
    return sstr.str();
}
