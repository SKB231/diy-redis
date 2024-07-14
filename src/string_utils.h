#pragma once
#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>

using namespace std;
void split_string(string full_string, char delimitter, vector<string> &result);

void read_file(const string &file_path);
void get_database_section(const vector<unsigned char> &file, int &start,
                          int &end);
void split_string(string full_string, char delimitter, vector<string> &result);

using BYTE = unsigned char;
using std::ifstream;
using std::string;
using bytes = std::vector<BYTE>;

class redis_database {
  string file_name;
  int database_start{};
  int hashtable_size{};
  bytes file_bytes;

public:
  unordered_map<string, string> redis_map;
  unordered_map<string, uint64_t> entry_expiration;
  bool isHealthy = true;

  redis_database(const string &file_path);
  void read_database();
  void read_pair(int &index, string &key, string &val, uint64_t &expiration);
  void decode_string(int &index, string &key);
  void get_database_section(const std::vector<unsigned char> &file, int &start,
                            int &end);
  int decode_size(int &index);
};
