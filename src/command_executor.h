#include "string"
#include <iostream>
#include <unordered_map>
#include <vector>

class CommandExecutor {
  vector<string> m_command_query{};

public:
  CommandExecutor() = default;
  ~CommandExecutor() = default;
  void add_to_query(vector<string> appended_command);
  void execute(vector<string> &resp);

private:
};
