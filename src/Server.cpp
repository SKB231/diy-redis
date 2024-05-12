#include <arpa/inet.h>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <mutex>
#include <netdb.h>
#include <netinet/in.h>
#include <new>
#include <stdio.h>
#include <string>
#include <sys/socket.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <utility>
#include <vector>

struct Config_Settings {
public:
  enum { master, slave } server_role;
  int server_port;
  std::pair<std::string, int> master_info;
  std::vector<int> replica_fd{};
};

struct connection_params {
  int fd;
  bool is_master;
  struct sockaddr_in *addr;
};

// Function prototypes:
void handle(struct connection_params *params);
void run_handshake();

std::string *get_full_message(int socket_fd, std::string caller);
void test_handle(int socket_fd, struct sockaddr_in *client_addr);

// Global Variables:
std::unordered_map<std::string, std::string> mem_database{};

class Worker {

public:
  struct connection_params *params;
  std::pair<int, std::string> params_deletion;
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

      worker.cv.wait(lk);

      if (worker.job_type == Job_Type::connection) {

        handle(worker.params);

        // Unlock m and reset params for use in next iteration
        worker.params = {};
      } else {

        std::this_thread::sleep_for(
            std::chrono::milliseconds(worker.params_deletion.first));
        std::string key_to_delete = worker.params_deletion.second;
        mem_database.erase(mem_database.find(key_to_delete));
        worker.job_type = Job_Type::connection;
      }

      lk.unlock();
      // run the function
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

  void run_connection(struct connection_params *params) {

    std::lock_guard<std::mutex> guard(
        worker_lock); // Use guard to lock worker vector usage
    workers[thread_index]->params = params;
    workers[thread_index]->cv.notify_one();
    thread_index = (thread_index + 1) % workers.size();
  }

  void run_deletion(std::pair<int, std::string> &params_deletion) {

    std::lock_guard<std::mutex> guard(
        worker_lock); // Use guard to lock worker vector usage
    workers[thread_index]->params_deletion = params_deletion;
    workers[thread_index]->job_type = Worker::Job_Type::deleter;
    workers[thread_index]->cv.notify_one();
    thread_index = (thread_index + 1) % workers.size();
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

Config_Settings parse_arguments(int argc, char **argv) {
  if (argc == 1) {
    return Config_Settings{Config_Settings::master, 6379, {}};
  }
  Config_Settings config_settings;

  config_settings.server_port = 6379;
  for (int i = 1; i < argc - 1; i++) {
    std::string query = std::string(argv[i]);
    std::string val = std::string(argv[i + 1]);
    if (query == "--port") {
      try {
        size_t siz{};
        int port_num = std::stoi(val, &siz, 10);
        config_settings.server_port = port_num;
      } catch (const std::exception &e) {
        std::cout << "Param conversion error\n";
        std::cout << e.what();
      }
    } else if (query == "--replicaof" && (i + 2 < argc)) {
      // Slave settings
      config_settings.server_role = Config_Settings::slave;
      std::string master_port_int = std::string(argv[i + 2]);

      config_settings.master_info = {val, 0};
      try {
        size_t siz{};
        // std::cout << "Converting: " << val << std::endl;
        int port_num = std::stoi(master_port_int, &siz, 10);
        config_settings.master_info.second = port_num;
      } catch (const std::exception &e) {
        std::cout << "Param conversion error 2: " << e.what() << std::endl;
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
  std::cout << "Logs from your program will appear here!\n";

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
    std::cout << "Reached here!";
    run_handshake();
  }

  struct sockaddr_in client_addr;
  int client_addr_len = sizeof(client_addr);
  while (true) {
    std::cout << "Waiting for a client to connect...\n";

    int client_fd = accept(server_fd, (struct sockaddr *)&client_addr,
                           (socklen_t *)&client_addr_len);
    if (client_fd < 0) {
      std::cout << "Socket connection failed\n" << std::endl;
      continue;
    }
    std::cout << "Client connected\n";
    // handle(client_fd, (struct sockaddr_in *)&client_addr);
    // std::pair<int, struct sockaddr_in *> params{
    //    client_fd, (struct sockaddr_in *)&client_addr};

    struct connection_params *params = new connection_params{
        client_fd, config.server_role == Config_Settings::master,
        (struct sockaddr_in *)&client_addr};
    master.run_connection(params);
  }

  return 0;
}

std::string *get_full_message(int socket_fd, std::string caller) {
  int data_written = 1;
  std::string *final_string = new std::string("");
  char buff[256];
  data_written = recv(socket_fd, buff, 256, 0);
  for (int i = 0; i < data_written; i++) {
    *final_string += buff[i];
  }
  std::cout << caller << "DATA WRITTEN: " << data_written << " "
            << *final_string << std::endl;
  return final_string;
}

std::vector<std::string> *split_by_clrf(std::string &full_message) {
  int last_token = 0;
  std::vector<std::string> *words{new std::vector<std::string>};
  for (int i = 0; i < full_message.length() - 1; i += 1) {
    if (full_message[i] == '\r' && full_message[i + 1] == '\n') {
      // abcd\r\n
      // lt   i
      std::string word = full_message.substr(last_token, i - last_token);
      // std::cout << word << "\n";
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

void parse_command(std::vector<std::string> &command,
                   std::vector<std::string> &resp, std::string caller) {
  for (int i = 0; i < command.size();) {
    // We currently are in the COMMAND string. The remaining words need to be
    // unaltered to ensure case-sensitivity
    for (int j = 0; j < command[i].size(); j++) {
      command[i][j] = std::tolower(command[i][j]);
    }

    if (command[i] == "echo") {
      // return "+" + command[1] + "\r\n";
      resp.push_back("+" + command[i + 1] + "\r\n");
      i += 2;
      continue;
    }

    if (command[i] == "ping") {
      // return "+" + std::string("PONG") + "\r\n";
      resp.push_back("+" + std::string("PONG") + "\r\n");
      i += 1;
      continue;
    }

    if (command[i] == "set") {
      std::cout << "Set command" << std::endl;
      std::size_t pos{};
      int skip_amount = 3;

      int lifetime{};
      if (i + 3 < command.size()) {
        for (int j = 0; j < command[i + 3].size(); j++) {
          command[i + 3][j] = std::tolower(command[i + 3][j]);
        }
        std::cout << (command[i + 3] == "px");
      }

      if (i + 3 < command.size() && command[i + 3] == "px") {
        std::cout << "Expirable command" << std::endl;
        lifetime = std::stoi(command[i + 4], &pos);
        skip_amount += 2;
      }

      std::cout << caller << "setting " << command[i + 1] << " to "
                << command[i + 2] << std::endl;

      mem_database[command[i + 1]] = command[i + 2];

      if (lifetime > 0) {

        auto params = std::pair<int, std::string>{lifetime, command[i + 1]};
        std::cout << "Running delayed deletion by " << lifetime << "ms"
                  << std::endl;
        master.run_deletion(params);
      }

      // return "+" + std::string("OK") + "\r\n";
      i += skip_amount;
      std::cout << "End SET Command Skipping by " << skip_amount << " to " << i
                << std::endl;
      resp.push_back("+OK\r\n");
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
  }

  std::cout << "Finised parsing " << std::endl;
  for (int i = 0; i < resp.size(); i++)
    std::cout << resp[i] << ", ";
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
  std::cout << "Using size: " << hex.size() / 2 << std::endl;
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
    std::cout << "SEND Empty RDB File:";
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

  std::cout << "Propogating request to slaves: " << std::endl;
  for (auto fd : config.replica_fd) {
    send(fd, (void *)original_req.c_str(), original_req.size(), 0);
  }
}

void handle(struct connection_params *params) {

  int socket_fd = params->fd;
  struct sockaddr_in *client_addr = params->addr;
  std::string server_type = (params->is_master) ? "Master - " : "Replica - ";

  bool closefd = false;
  while (!closefd) {
    char buff[32] = {};
    int total_written = 0;
    std::cout << server_type + "Listening for message: " << std::endl;
    std::string req = *(get_full_message(socket_fd, server_type + ""));
    if (req.length() <= 1) {
      break;
    }
    std::cout << server_type + "Received message: " << req << std::endl;
    std::vector<std::string> *all_words = split_by_clrf(req);
    std::cout << server_type + "ARRAY: ";

    for (std::string word : *all_words) {
      std::cout << word << ", ";
    }
    std::cout << std::endl;

    std::vector<std::string> response{};
    parse_command(*all_words, response, server_type + "");

    for (int i = 0; i < response.size(); i++) {
      std::string resp = response[i];
      std::cout << server_type + "Server Response: " << resp << std::endl;
      send(socket_fd, (void *)resp.c_str(), resp.size(), 0);
      follow_up_commands(resp, socket_fd);
      follow_up_slave(*all_words, req);
    }
  }
  close(socket_fd);
}

void run_handshake() {
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

  std::cout << "\nAttemtping to connect to server" << std::endl;
  int res =
      connect(replica_fd, (struct sockaddr *)&server_addr, sizeof(server_addr));
  if (res < 0) {
    std::cout << "Failed to connect replica to master\n";
  }

  std::cout << "Connected to master.\n";

  // HANDSHAKE: PING
  const char *message{"*1\r\n$4\r\nping\r\n"};
  send(replica_fd, message, std::string(message).size(), 0);
  std::string *response_ping = get_full_message(replica_fd, "Replica - ");
  std::cout << "Finished PING" << std::endl;

  const char *repl_conf{
      "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n"};
  send(replica_fd, repl_conf, std::string(repl_conf).size(), 0);
  std::string *response_repl_1 = get_full_message(replica_fd, "Replica - ");
  std::cout << "Finished REPL-CONF" << std::endl;

  const char *repl_conf_2{
      "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"};
  send(replica_fd, repl_conf_2, std::string(repl_conf_2).size(), 0);
  std::string *response_repl_2 = get_full_message(replica_fd, "Replica - ");
  std::cout << "Finished REPL-CONF-2" << std::endl;

  const char *psync{"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"};
  send(replica_fd, psync, std::string(psync).size(), 0);
  std::string *response_psync = get_full_message(replica_fd, "Replica - ");
  std::string *handle_RDB = get_full_message(replica_fd, "Replica - ");
  std::cout << "Finished RDB" << std::endl;

  bool closefd = false;
  // LISTEN to commands from the master and process them like regular commands
  while (!closefd) {
    char buff[32] = {};
    int total_written = 0;
    std::cout << "Replica - Listening for message..." << std::endl;
    std::string req = *(get_full_message(replica_fd, "Replica - "));

    if (req.length() <= 1) {
      break;
    }
    std::cout << "Replica - Received message: " << req << std::endl;
    std::vector<std::string> *all_words = split_by_clrf(req);
    std::cout << "Replica - ARRAY: ";

    for (std::string word : *all_words) {
      std::cout << word << ", ";
    }
    std::cout << std::endl;
    std::vector<std::string> response{};
    parse_command(*all_words, response, "Replica - ");
  }
  close(replica_fd);
}
