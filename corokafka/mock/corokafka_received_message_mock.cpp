#include <corokafka/mock/corokafka_received_message_mock.h>
#include <corokafka/corokafka_headers.h>

namespace Bloomberg {
namespace corokafka {
namespace mocks {

HeaderAccessorMock<int> dummyHeaderMock;
ReceivedMessageMock<int, int, corokafka::Headers<int>> dummyMessageMock;

}}}