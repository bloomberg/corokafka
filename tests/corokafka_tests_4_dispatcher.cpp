#include <corokafka_tests_callbacks.h>
#include <gtest/gtest.h>

namespace Bloomberg::corokafka::tests
{

class QuantumEnvironment : public ::testing::Environment
{
    public:
        ~QuantumEnvironment() override{};

        void TearDown() override { dispatcher().drain();
            dispatcher().terminate();
        }
};  // class QuantumEnvironment

::testing::Environment* const quantumEnvironment =
        testing::AddGlobalTestEnvironment(new QuantumEnvironment);

}    // namespace Bloomberg::corokafka::tests
