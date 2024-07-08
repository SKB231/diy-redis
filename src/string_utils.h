#pragma once
#include <string>
#include <vector>

using namespace std;
void split_string(string full_string, char delimitter, vector<string> &result);

void read_file(const string& file_path);
void get_database_section(const vector<unsigned char> &file, int &start, int &end);
