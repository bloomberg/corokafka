#ifndef BLOOMBERGLP_COROKAFKA_TESTS_PROGRAM_OPTIONS_H
#define BLOOMBERGLP_COROKAFKA_TESTS_PROGRAM_OPTIONS_H

#include <boost/program_options.hpp>
#include <corokafka/corokafka.h>
#include <quantum/quantum.h>
#include <string>
#include <iostream>

namespace po = boost::program_options;
namespace ck = Bloomberg::corokafka;
namespace qm = Bloomberg::quantum;

namespace Bloomberg {
namespace corokafka {
namespace tests {

class ProgramOptions
{
public:
    ProgramOptions() = default;
    
    ProgramOptions(int argc, char *argv[]) noexcept
    {
        //set option definitions
        std::string type;
        po::options_description desc("USAGE: task [options]");
        desc.add_options()
                ("broker,b", po::value<std::string>(&_broker)->required(), "Kafka broker")
                ("kafka-type,k", po::value<std::string>(&type)->required(), "Kafka type: 'producer' or 'consumer'")
                ("topic-header,t", po::value<std::string>(&_topicWithHeaders)->required(),
                                   "Name of topic which contains headers")
                ("topic-no-header,n", po::value<std::string>(&_topicWithoutHeaders)->required(),
                                      "Name of topic which does not contain headers")
                ("help,h", "Show options")
                ("skip-timestamp,s", po::value<bool>(&_skipTimestampCompare)->implicit_value(true), "Skip timestamp comparison");
        po::variables_map vm;
        po::store(po::command_line_parser(argc, argv).options(desc).run(), vm);
        
        if (vm.count("help") > 0) {
            std::cout << desc << std::endl;
            exit(0);
        }
        try {
            po::notify(vm);
        }
        catch (const std::exception &ex) {
            std::cout << ex.what() << std::endl;
            exit(0);
        }
        _kafkaType = (type == "consumer") ? KafkaType::Consumer : KafkaType::Producer;
    }
    
    std::string _broker;
    KafkaType   _kafkaType;
    std::string _topicWithHeaders;
    std::string _topicWithoutHeaders;
    bool        _skipTimestampCompare{false};
};

inline
ProgramOptions& programOptions()
{
    static ProgramOptions _po;
    return _po;
}

}}}

#endif //BLOOMBERGLP_COROKAFKA_TESTS_PROGRAM_OPTIONS_H
