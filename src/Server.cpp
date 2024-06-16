#include "string_utils.h"
#include <arpa/inet.h>
#include <cctype>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <exception>
#include <functional>
#include <iostream>
#include <mutex>
#include <netdb.h>
#include <netinet/in.h>
#include <new>
#include <string>
#include <sys/socket.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <utility>
#include <vector>

// Function prototypes:
void handle(int socket_fd, struct sockaddr_in *client_addr);
void handshake_and_replication_handle();
std::string *get_full_message(int socket_fd, int* count);
const int BUF_SIZE = 250;

// Global Variables:
using redis_map = std::unordered_map<std::string, std::string>;
using std::pair;
using std::cout;


redis_map mem_database{};
long long total_written = -1;

class Worker {

public:
  pair<int, struct sockaddr_in *> params;
  pair<int, std::string> params_deletion;
  enum Job_Type {
    connection,
    deleter,
  } job_type;

  std::condition_variable cv;
  std::mutex m;
  int id;

  Worker(int id) { this->id = id; }

  static void run(Worker &worker) {
    while (true) {
      // If cv is notified, then run the function with the parameters
      std::unique_lock lk(worker.m);

      // cout << "Thread " << worker.id << " waiting for job. \n";
      worker.cv.wait(lk);
      // cout << "Thread " << worker.id << " running a job. \n";

      if (worker.job_type == Job_Type::connection) {

        handle(worker.params.first, worker.params.second);

        // Unlock m and reset params for use in next iteration
        worker.params = pair<int, struct sockaddr_in *>{};
      } else {

        // cout << "Attempting to delete after "
        //<< worker.params_deletion.first << " milliseconds\n";

        std::this_thread::sleep_for(
            std::chrono::milliseconds(worker.params_deletion.first));
        std::string key_to_delete = worker.params_deletion.second;
        mem_database.erase(mem_database.find(key_to_delete));
        worker.job_type = Job_Type::connection;
      }

      lk.unlock();
      // run the function
      // cout << "Thread " << worker.id
      // << " completed the job. Resetting.. \n";
    }
  }
};

/**
 * This thread master initializes a set of worker threads. Each worker thread
 * will be waiting for the callback function to become non-null. When work is
 * assigned, a thread will run that job and wait for the next job when the job
 * is completed.
 */
class Master {
  std::vector<Worker *> workers;
  std::vector<std::thread *> threads_in_use;
  int thread_index;
  int thread_count;
  std::mutex worker_lock;

public:
  Master(int thread_count) {
    thread_index = {};
    for (int i = 0; i < thread_count; i++) {
      Worker *new_worker = new (std::nothrow) Worker(i);

      if (!new_worker)
        return;

      std::thread *worker_thread =
          new std::thread(&Worker::run, std::ref(*new_worker));
      threads_in_use.push_back(worker_thread);
      workers.push_back(new_worker);
    }
  }

  // Currently no plans to use copy function
  Master &operator=(const Master &master) = delete;
  Master(const Master &master) = delete;

  void run_connection(pair<int, struct sockaddr_in *> &params) {

    std::lock_guard<std::mutex> guard(
        worker_lock); // Use guard to lock worker vector usage
    workers[thread_index]->params = params;
    workers[thread_index]->cv.notify_one();
    thread_index = (thread_index + 1) % workers.size();
    // cout << thread_index << " armed" << std::endl;
  }

  void run_deletion(pair<int, std::string> &params_deletion) {

    std::lock_guard<std::mutex> guard(
        worker_lock); // Use guard to lock worker vector usage
    workers[thread_index]->params_deletion = params_deletion;
    workers[thread_index]->job_type = Worker::Job_Type::deleter;
    workers[thread_index]->cv.notify_one();
    thread_index = (thread_index + 1) % workers.size();
    // cout << thread_index << " armed" << std::endl;
  }

  ~Master() {
    for (int i = 0; i < thread_count; i++) {
      delete threads_in_use[i];
      delete workers[i];
    }
  }
};

// Our threads
Master master = Master(4);

struct Config_Settings {
public:
  enum { master, slave } server_role;
  int server_port;
  pair<std::string, int> master_info;
  std::vector<int> replica_fd{};
};

Config_Settings parse_arguments(int argc, char **argv) {
  if (argc == 1) {
    return Config_Settings{Config_Settings::master, 6379, {}};
  }
  Config_Settings config_settings;

  config_settings.server_port = 6379;
  for (int i = 1; i < argc - 1; i++) {

    std::string query = std::string(argv[i]);
    std::string val = std::string(argv[i + 1]);

    cout << query << " " << (query == "--replicaof") << " "
              << (i + 2 < argc) << std::endl;

    if (query == "--port") {
      try {
        size_t siz{};
        int port_num = std::stoi(val, &siz, 10);
        config_settings.server_port = port_num;
      } catch (const std::exception &e) {
        cout << "Param conversion error\n";
        cout << e.what();
      }
    } else if (query == "--replicaof") {
      // Slave settings
      config_settings.server_role = Config_Settings::slave;

      std::vector<std::string> master_info{};

      split_string(argv[i + 1], ' ', master_info);

      cout << "Finished parsing master information \n";

      for (auto str : master_info)
        cout << str << ", ";

      cout << std::endl;

      if (master_info.size() < 2) {
      }
      std::string master_port_int = master_info[1];

      config_settings.master_info = {master_info[0], 0};
      try {
        size_t siz{};
        // cout << "Converting: " << val << std::endl;
        int port_num = std::stoi(master_port_int, &siz, 10);
        config_settings.master_info.second = port_num;
      } catch (const std::exception &e) {
        cout << "Param conversion error 2: " << e.what() << std::endl;
      }
    }
  }
  return config_settings;
}
Config_Settings config;

in_addr_t string_to_addr(std::string &addr) {
  in_addr_t ret_addr;
  inet_pton(AF_INET, addr.c_str(), (void *)(&ret_addr));
  return ret_addr;
}

int main(int argc, char **argv) {

  config = parse_arguments(argc, argv);

  // You can use print statements as follows for debugging, they'll be
  // visible when running tests.
  cout << "Logs from your program will appear here!\n";

  // Uncomment this block to pass the first stage

  int server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0) {
    std::cerr << "Failed to create server socket\n";
    return 1;
  }

  // Since the tester restarts your program quite often, setting SO_REUSEADDR
  // ensures that we don't run into 'Address already in use' errors
  int reuse = 1;
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) <
      0) {
    std::cerr << "setsockopt failed\n";
    return 1;
  }

  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = INADDR_ANY;
  server_addr.sin_port = htons(config.server_port);

  if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) !=
      0) {
    std::cerr << "Failed to bind to port " << config.server_port << std::endl;
    return 1;
  }

  int connection_backlog = 5;
  if (listen(server_fd, connection_backlog) != 0) {
    std::cerr << "listen failed\n";
    return 1;
  }

  if (config.server_role == Config_Settings::slave) {
    new std::thread{handshake_and_replication_handle}; // run handshake in a seperate thread
                                    // without requirement to destroy
  }

  struct sockaddr_in client_addr;
  int client_addr_len = sizeof(client_addr);
  while (true) {
    cout << "Waiting for a client to connect...\n";

    int client_fd = accept(server_fd, (struct sockaddr *)&client_addr,
                           (socklen_t *)&client_addr_len);
    if (client_fd < 0) {
      cout << "Socket connection failed\n" << std::endl;
      continue;
    }
    cout << "Client connected\n";
    // handle(client_fd, (struct sockaddr_in *)&client_addr);
    pair<int, struct sockaddr_in *> params{
        client_fd, (struct sockaddr_in *)&client_addr};
    master.run_connection(params);
  }

  return 0;
}

std::string *get_full_message(int socket_fd, long long* count = nullptr) {
    using sString = std::string;

    int data_written = 1;
    auto *final_string = new sString("");
    char buff[BUF_SIZE];
    
    data_written = recv(socket_fd, buff, BUF_SIZE, 0);
    for (int i = 0; i < data_written; i++) {
      *final_string += buff[i];
    }
    cout << *final_string << endl;
    if(count) {
      cout << "DATA WRITTEN: " << data_written << " " << *final_string
           << std::endl;
      *count += data_written;
    }
    return final_string;
}

std::vector<std::string> *split_by_clrf(std::string &full_message) {
  int last_token = 0;
  std::vector<std::string> *words{new std::vector<std::string>};
  for (int i = 0; i < full_message.length() - 1; i += 1) {
    if (full_message[i] == '\r' && full_message[i + 1] == '\n') {
      std::string word = full_message.substr(last_token, i - last_token);
      if (word.length() > 0 && word[0] != '$' && word[0] != '*') {
        words->push_back(word);
      }
      last_token = i + 2;
    }
  }
  return words;
}

std::string get_resp_bulkstring(std::string word) {
  return "$" + std::to_string(word.size()) + "\r\n" + word + "\r\n";
}

std::string get_resp_bulk_arr(std::vector<string> words) {
  std::string final_str = "*" + std::to_string(words.size()) + "\r\n";
  for (auto word : words) {
    final_str += get_resp_bulkstring(word);
  }
  return final_str;
}

void parse_command(std::vector<std::string> &command,
                   std::vector<std::string> &resp, bool is_replica = false) {
  for (int i = 0; i < command.size(); i += 1) {
    for (int j = 0; j < command[i].size(); j++) {
      command[i][j] = std::tolower(command[i][j]);
    }
  }

  for (int i = 0; i < command.size();) {
    cout << "Parsing command: " << command[i] << std::endl;

    if (command[i] == "echo") {
      // return "+" + command[1] + "\r\n";
      if(!is_replica) resp.push_back("+" + command[i + 1] + "\r\n");

      i += 2;
      continue;
    }

    if (command[i] == "wait") {
      int repl_count = std::stoi(command[i+1]);
      int timeout = std::stoi(command[i+2]);
      if (!is_replica) resp.push_back(":" + std::to_string(0) + "\r\n");
      i += 2;
    }

    if (command[i] == "ping") {
      // return "+" + std::string("PONG") + "\r\n";
      if(!is_replica) resp.push_back("+" + std::string("PONG") + "\r\n");

      i += 1;
      continue;
    }

    if (command[i] == "set") {
      cout << "Set command" << std::endl;
      std::size_t pos{};
      int skip_amount = 3;

      int lifetime{};
      if (i + 3 < command.size()) {
        for (int j = 0; j < command[i + 3].size(); j++) {
          command[i + 3][j] = std::tolower(command[i + 3][j]);
        }
        cout << (command[i + 3] == "px");
      }

      if (i + 3 < command.size() && command[i + 3] == "px") {
        cout << "Expirable command" << std::endl;
        lifetime = std::stoi(command[i + 4], &pos);
        skip_amount += 2;
      }

      mem_database[command[i + 1]] = command[i + 2];

      if (lifetime > 0) {

        auto params = pair<int, std::string>{lifetime, command[i + 1]};
        cout << "Running delayed deletion by " << lifetime << "ms"
                  << std::endl;
        master.run_deletion(params);
      }

      // return "+" + std::string("OK") + "\r\n";
      i += skip_amount;
      cout << "End SET Command Skipping by " << skip_amount << " to " << i
                << std::endl;
      if (!is_replica) resp.push_back("+OK\r\n");
      continue;
    }

    if (command[i] == "info") {

      if (command[i + 1] == "replication") {
        std::string resp_str{""};
        switch (config.server_role) {
        case Config_Settings::master:
          resp_str = "role:master";
          break;
        case Config_Settings::slave:
          resp_str = "role:slave";
          break;
        }
        std::string repl_id =
            "master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
        std::string repl_offset = "master_repl_offset:0";
        resp.push_back(get_resp_bulkstring(resp_str + repl_id + repl_offset));
        i += 2;
      } else {
        i += 1;
      }
      continue;
    }

    if (i + 1 < command.size() && command[i] == "replconf" &&
        command[i + 1] == "getack") {

        if(total_written < 0) {
          resp.push_back(get_resp_bulk_arr({"REPLCONF", "ACK", "0"}));
          total_written = 0;
        } else {
          resp.push_back(get_resp_bulk_arr({"REPLCONF", "ACK", std::to_string(total_written)}));
        }
        i += 2;
        continue;
    }

    if (command[i] == "replconf") {
      resp.push_back(std::string("+OK\r\n"));
      i += 3;
      if (i < command.size() && command[i] == "capa") {
        i += 2;
      }
      continue;
    }

    if (command[i] == "psync") {
      resp.push_back(std::string(
          "+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n"));
      i += 3;
      continue;
    }

    if (command[i] == "get") {
      auto it = mem_database.find(command[i + 1]);
      if (it == mem_database.end()) {
        // element doesn't exist
        resp.push_back(std::string("$-1\r\n"));
      } else {
        auto res = mem_database[command[i + 1]];
        resp.push_back(std::string("$") + std::to_string(res.size()) + "\r\n" +
                       res + std::string("\r\n"));
      }
      i += 2;
      continue;
    }
    // incriment in default case
    i += 1;
  }

  cout << "Finised parsing " << std::endl;
  for (int i = 0; i < resp.size(); i++)
    cout << resp[i] << ", ";
}

/**
 * convert_to_binary converts a hex string into an array of bytes containing the
 * data. Note that the bytes are packed in little endian format and set the
 * buffer
 *
 */
char *convert_to_binary(std::string hex) {
  std::unordered_map<char, char> hex_binary_map{
      {'0', 0},  {'1', 1},  {'2', 2},  {'3', 3},  {'4', 4},  {'5', 5},
      {'6', 6},  {'7', 7},  {'8', 8},  {'9', 9},  {'a', 10}, {'b', 11},
      {'c', 12}, {'d', 13}, {'e', 14}, {'f', 15},
  };
  cout << "Using size: " << hex.size() / 2 << std::endl;
  char *buf = new char[hex.size() / 2];

  for (int i = 0; i < hex.size(); i += 2) {
    int idx = i / 2;
    char byte = hex_binary_map[hex[i]];
    byte = byte << 4;
    byte += hex_binary_map[hex[i + 1]];
    buf[idx] = byte;
  }
  return buf;
}

void follow_up_commands(std::string sent_command, int socket_fd) {
  if (sent_command.substr(0, 11) == "+FULLRESYNC") {
    cout << "SEND Empty RDB File:";
    std::string empty_rdb_file_hex =
        "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62"
        "697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa0861"
        "6f662d62617365c000fff06e3bfec0ff5aa2";

    char *binary_file = convert_to_binary(empty_rdb_file_hex);
    int size = empty_rdb_file_hex.size() / 2;

    std::string resp = "$" + std::to_string(size) + "\r\n";
    for (int i = 0; i < size; i++) {
      resp += binary_file[i];
    }
    send(socket_fd, (void *)resp.c_str(), resp.size(), 0);

    // Since this indicates an additional replica, add this to the
    // config_settings slave file descriptors
    config.replica_fd.push_back(socket_fd);
  }
}

void follow_up_slave(std::vector<std::string> &req, std::string original_req) {
  if (config.server_role == Config_Settings::slave || req[0] != "set")
    return;

  cout << "Propogating request to slaves: " << std::endl;
  for (auto fd : config.replica_fd) {
    send(fd, (void *)original_req.c_str(), original_req.size(), 0);
  }
}

void handle(int socket_fd, struct sockaddr_in *client_addr) {
  bool closefd = false;
  while (!closefd) {
    char buff[32] = {};
    int total_written = 0;
    cout << "Master - Listening for message: " << std::endl;
    std::string req = *(get_full_message(socket_fd));
    if (req.length() <= 1) {
      break;
    }
    cout << "Master - Received message: " << req << std::endl;
    std::vector<std::string> *all_words = split_by_clrf(req);
    cout << "Master - ARRAY: ";

    for (std::string word : *all_words) {
      cout << word << ", ";
    }
    cout << std::endl;

    std::vector<std::string> response{};
    parse_command(*all_words, response);

    for (int i = 0; i < response.size(); i++) {
      std::string resp = response[i];
      cout << "Master - Server Response: " << resp << std::endl;
      send(socket_fd, (void *)resp.c_str(), resp.size(), 0);
      follow_up_commands(resp, socket_fd);
      follow_up_slave(*all_words, req);
    }
  }
  close(socket_fd);
}



/**
 * This is the handling method for the slave server that, after handshake, receives updates from the server containing database updates
 */
void handshake_and_replication_handle() {
  // Use the current config variable to connect to the master
  //
  // Create a file descriptor to connect to the master server. We initialize
  // it with the same parameters: Ipv4 and reliable socket_stream
  int replica_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (replica_fd < 0) {
    std::cerr << "Failed to create replica socket\n";
    return;
  }

  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = string_to_addr(config.master_info.first);
  server_addr.sin_port = htons(config.master_info.second);

  cout << "\nAttemtping to connect to server" << std::endl;
  int res =
      connect(replica_fd, (struct sockaddr *)&server_addr, sizeof(server_addr));
  if (res < 0) {
    cout << "Failed to connect replica to master\n";
  }

  cout << "Connected to master.\n";

  // HANDSHAKE: PING
  const char *message{"*1\r\n$4\r\nping\r\n"};
  send(replica_fd, message, std::string(message).size(), 0);
  std::string *response_ping = get_full_message(replica_fd);

  const char *repl_conf{
      "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n"};
  send(replica_fd, repl_conf, std::string(repl_conf).size(), 0);
  std::string *response_repl_1 = get_full_message(replica_fd);

  const char *repl_conf_2{
      "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"};
  send(replica_fd, repl_conf_2, std::string(repl_conf_2).size(), 0);
  std::string *response_repl_2 = get_full_message(replica_fd);

  const char *psync{"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"};
  send(replica_fd, psync, std::string(psync).size(), 0);

  // Handshake complete. Any further messages can include COMMANDS after RDB

  bool closefd = false;
  // LISTEN to commands from the master and process them like regular commands
  while (!closefd) {
    char buff[32] = {};
    cout << "Replica - Listening for message..." << std::endl;

    long long* counter = (total_written < 0) ? nullptr : &total_written;
    std::string req = *(get_full_message(replica_fd, counter));

    if (req.length() <= 1) {
      break;
    }
    cout << "Replica - Received message: " << req << std::endl;
    cout << "Splitting by CLRF" << std::endl;
    std::vector<std::string> *all_words = split_by_clrf(req);
    cout << "Replica - ARRAY: ";

    for (std::string word : *all_words) {
      cout << word << ", ";
    }
    cout << std::endl;
    std::vector<std::string> response{};
    cout << "Parsing array: \n";
    parse_command(*all_words, response, true);

    for (int i = 0; i < response.size(); i++) {
      std::string resp = response[i];
      cout << "Replica - Server Response: " << resp << std::endl;
      send(replica_fd, (void *)resp.c_str(), resp.size(), 0);
    }
  }
  close(replica_fd);
}
