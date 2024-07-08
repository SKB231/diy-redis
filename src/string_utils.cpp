#include "string_utils.h"
#include <fstream>
#include <iostream>
#include <unordered_map>

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

using BYTE = unsigned char;
using std::string;
using std::ifstream;
using bytes = std::vector<BYTE>;
class redis_database {
    string file_name;
    bytes file_bytes;
    int database_start{};
    int hashtable_size{};
    unordered_map<string, string> redis_map;


public:
    redis_database(const string& file_path) {
      ifstream file(file_path, std::ios::binary);

      // Move cursor to the end of the file
      file.seekg(0, std::ios::end);
      // Retrive cursor position to get file size
      cout << file.tellg();
      int size = int(file.tellg());
      // Move cursor to the beginning of the file
      file.seekg(0, std::ios::beg);


      // Read the binary data
      file_bytes = bytes(size);
      file.read((char*) &file_bytes[0], size);
      int _;

      get_database_section(file_bytes, database_start, _);
      hashtable_size = file_bytes[database_start+3];
      cout << "Hashtable size: " << hashtable_size << endl;

    }


    void read_database() {
      int start = database_start + 5;
      string key{}, val{};
      for(int i = 0; i < hashtable_size; i++) {
        read_pair(start, key, val);
        redis_map.insert({key, val});
        key = "";
        val = "";
      }
      cout << endl;
    }

    void read_pair(int& index, string& key, string& val) {
      int expirey_time;
      if(file_bytes[index] == 0xfc) {
        index += 1;
        int start_index = index;
        for(expirey_time = file_bytes[index], index+=1; index < start_index + 8; index++) {
          expirey_time = (expirey_time << 1) + file_bytes[index];
        }
        cout << "expirey_time: "<<  hex << expirey_time << endl;

      } else if(file_bytes[index] == 0xfd) {

      }

      //cout << "Starting to read key value pair at index: " << index << " \n"<< endl;
      int valType = file_bytes[index];
      //cout << " => " << valType << " <-- valType" << endl;
      index ++;
      decode_string(index, key);
      //cout << "Key: " << key << endl;
      decode_string(index, val);
      //cout << "Val: " << val << endl;
      //cout << "=============" << endl;
      //cout << "index at " << dec << index << " " << hex << file_bytes[index] << endl;

    }

    void decode_string(int &index, string& key) {
      int size = decode_size(index);


      //cout << " Size of key string: " << abs(size) << "\n" << endl;
      if(size >= 0) {
        for(int i = index; i < index+size; i++) {
          key += file_bytes[i];
        }
        //cout << "Incrimenting size of index to: " << index + size << endl;
        index += size;
      } else {
        // we're decding a value which might be of a little endian form
        if (size == -8) {
          key = std::to_string(int(file_bytes[index]));
          index += 1;
        }
        else if (size == -16) {
          // file_bytes[index+1] file_bytes[index]
          key = string{};
          key += file_bytes[index+1];
          key += file_bytes[index];
          index += 2;
        } else {
          key = string{};
          key += file_bytes[index+3];
          key += file_bytes[index+2];
          key += file_bytes[index+1];
          key += file_bytes[index];
          index += 4;
        }
      }
    }

    int decode_size(int& index) {
      int first_two_bits = file_bytes[index] >> 6;
      int size{};
      //cout << "Decoding size..." << endl;
      //cout << "First two bits: " <<  first_two_bits << " " << std::hex << int(file_bytes[index]) << endl;
      switch(first_two_bits) {
        case 0:
          //cout << "using type 0: " << endl;
          size = file_bytes[index] << 2;
          size = size >> 2;
          index ++;
          break;
        case 1:
          //cout << "using type 1: " << endl;
          //cout << hex<< file_bytes[index] << endl <<  hex << file_bytes[index+1] << endl;
          //cout << ((file_bytes[index] << 2) >> 2) << endl;
          //cout << (((file_bytes[index] << 2) >> 2) << 8) + file_bytes[index+1] << endl;
          size = (((file_bytes[index] << 2) >> 2) << 8) + file_bytes[index+1];
          index += 2;
          break;
        case 2:
          //cout << "using type 2: " << endl;
          size = (file_bytes[index+1] << 24) + (file_bytes[index+2]  << 16) + (file_bytes[index + 3] << 8) + (file_bytes[index + 4]);
          index += 5;
          break;
        case 3:
          //cout << "using type 3: " << endl;
          if(file_bytes[index] == 0xc0) {
            size = -8;
            index += 1;
          } else if(file_bytes[index] == 0xc1) {
            size = -16;
            index += 1;
          }else if(file_bytes[index] == 0xc2) {
            size = -32;
            index += 2;
          }
          break;
      }
      return size;
    }

    void get_database_section(const std::vector<unsigned char> &file, int &start, int &end) {
      for(int i = 0; i < file.size(); i++) {
        if(int(file[i]) == 0xfe) {
          // start of databse section
          start = i;
        }
        if(int(file[i]) == 0xff) {
          // end of databsae section
          end = i;
          return;
        }
        //cout << std::hex << int(file[i]) << " ";
      }
    }
};

int main() {
  //read_file("./dump.rdb");

  redis_database rdb("/Users/shreekrishnarbhat/dev/diy-redis/src/dump.rdb");
  rdb.read_database();

  return 0;
}
