#include <string>
#include <vector>

using namespace std;

// Parses string of format +<message>\r\n
void parse_simple_string(string &message, int index) {}

// Parses string of format $<length>\r\n<string>\r\n
void parse_string(string &message, int index) {}

// Parses an array of format *<numbner of
// elements>\r\n<element1>\r\n<element2>...\r\n assume that the array is an
// array of strings
void parse_array(string &message, int index) {}

vector<string> parse_command(string &full_message) {}
