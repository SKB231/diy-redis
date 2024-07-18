#include "command_executor.h"

void CommandExecutor::add_to_query(vector<string> appended_command) {
  for (auto word : appended_command) {
    m_command_query.push_back(word);
  }
}

void CommandExecutor::execute(vector<string> &resp) {}
