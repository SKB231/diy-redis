#include "string_utils.h"

void split_string(string full_string, char delimitter, vector<string> &result) {
  result.clear();

  int index = 0;
  int N = full_string.size();
  for (int i = 0; i < full_string.size(); i++) {
    if (full_string[i] == ' ') {
      result.push_back(full_string.substr(index, (i - index)));
      index = i;
    }
  }

  if (full_string[N - 1] != ' ') {
    result.push_back(full_string.substr(index, (N - index)));
  }
}
