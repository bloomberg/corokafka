#include <corokafka_tests_callbacks.h>
#include <gtest/gtest.h>

namespace Bloomberg {
namespace corokafka {
namespace tests {

TEST(Quantum, Drain)
{
    //This test must run last
    dispatcher().drain();
    dispatcher().terminate();
}

}}}
