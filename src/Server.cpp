#include <arpa/inet.h>
#include <cctype>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
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

void handle(int socket_fd, struct sockaddr_in *client_addr);

void test_handle(int socket_fd, struct sockaddr_in *client_addr);

std::unordered_map<std::string, std::string> mem_database{};

class Worker {

public:
  std::pair<int, struct sockaddr_in *> params;
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

      std::cout << "Thread " << worker.id << " waiting for job. \n";
      worker.cv.wait(lk);
      std::cout << "Thread " << worker.id << " running a job. \n";

      if (worker.job_type == Job_Type::connection) {

        handle(worker.params.first, worker.params.second);

        // Unlock m and reset params for use in next iteration
        worker.params = std::pair<int, struct sockaddr_in *>{};
      } else {

        std::cout << "Attempting to delete after "
                  << worker.params_deletion.first << " milliseconds\n";

        std::this_thread::sleep_for(
            std::chrono::milliseconds(worker.params_deletion.first));
        std::string key_to_delete = worker.params_deletion.second;
        mem_database.erase(mem_database.find(key_to_delete));
        worker.job_type = Job_Type::connection;
      }

      lk.unlock();
      // run the function
      std::cout << "Thread " << worker.id
                << " completed the job. Resetting.. \n";
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

  void run_connection(std::pair<int, struct sockaddr_in *> &params) {

    std::lock_guard<std::mutex> guard(
        worker_lock); // Use guard to lock worker vector usage
    workers[thread_index]->params = params;
    workers[thread_index]->cv.notify_one();
    thread_index = (thread_index + 1) % workers.size();
    std::cout << thread_index << " armed" << std::endl;
  }

  void run_deletion(std::pair<int, std::string> &params_deletion) {

    std::lock_guard<std::mutex> guard(
        worker_lock); // Use guard to lock worker vector usage
    workers[thread_index]->params_deletion = params_deletion;
    workers[thread_index]->job_type = Worker::Job_Type::deleter;
    workers[thread_index]->cv.notify_one();
    thread_index = (thread_index + 1) % workers.size();
    std::cout << thread_index << " armed" << std::endl;
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
  std::pair<std::string, int> master_info;
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
    std::pair<int, struct sockaddr_in *> params{
        client_fd, (struct sockaddr_in *)&client_addr};
    master.run_connection(params);
  }

  return 0;
}

void test_handle(int socket_fd, struct sockaddr_in *client_addr) {
  std::cout << "====RUN HANDLE FOR " << socket_fd << "========\n" << std::endl;
  int time = std::rand() % 10;
  std::cout << "Sleeping for " << time << "seconds" << std::endl;
  std::this_thread::sleep_for(std::chrono::seconds(time));
  std::cout << "====RUN HANDLE COMPLETED FOR " << socket_fd << "========\n"
            << std::endl;
}

std::string *get_full_message(int socket_fd) {
  int data_written = 1;
  std::string *final_string = new std::string("");
  char buff[256];
  data_written = recv(socket_fd, buff, 256, 0);
  for (int i = 0; i < data_written; i++) {
    *final_string += buff[i];
  }
  std::cout << "DATA WRITTEN: " << data_written << std::endl;
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

std::string parse_command(std::vector<std::string> &command) {
  for (int i = 0; i < command[0].size(); i++) {
    command[0][i] = std::tolower(command[0][i]);
  }
  if (command[0] == "echo") {
    return "+" + command[1] + "\r\n";
  }

  if (command[0] == "ping") {
    return "+" + std::string("PONG") + "\r\n";
  }

  if (command.size() == 5 && command[0] == "set") {
    std::size_t pos{};
    const int lifetime{std::stoi(command[4], &pos)};
    mem_database[command[1]] = command[2];
    auto params = std::pair<int, std::string>{lifetime, command[1]};
    std::cout << "Running delayed deletion by " << lifetime << "ms"
              << std::endl;
    master.run_deletion(params);
    return "+" + std::string("OK") + "\r\n";
  }

  if (command[0] == "set") {
    mem_database[command[1]] = command[2];
    return "+" + std::string("OK") + "\r\n";
  }

  if (command[0] == "info") {
    if (command[1] == "replication") {
      std::string resp{""};
      switch (config.server_role) {
      case Config_Settings::master:
        resp = "role:master";
        break;
      case Config_Settings::slave:
        resp = "role:slave";
        break;
      }
      std::string repl_id =
          "master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
      std::string repl_offset = "master_repl_offset:0";

      return get_resp_bulkstring(resp + repl_id + repl_offset);
    }
  }

  auto it = mem_database.find(command[1]);
  if (it == mem_database.end()) {
    // element doesn't exist
    return std::string("$-1\r\n");
  } else {
    auto res = mem_database[command[1]];
    return std::string("$") + std::to_string(res.size()) + "\r\n" + res +
           std::string("\r\n");
  }
}

void handle(int socket_fd, struct sockaddr_in *client_addr) {
  bool closefd = false;
  while (!closefd) {
    char buff[32] = {};
    int total_written = 0;
    std::cout << "Listening for message: " << std::endl;
    std::string req = *(get_full_message(socket_fd));
    if (req.length() <= 1) {
      break;
    }
    std::cout << "Received message: " << req << std::endl;
    std::vector<std::string> *all_words = split_by_clrf(req);
    std::cout << "ARRAY: ";

    for (std::string word : *all_words) {
      std::cout << word << ", ";
    }
    std::cout << std::endl;

    std::string response = parse_command(*all_words);
    std::cout << "Server Response: " << response << std::endl;
    send(socket_fd, (void *)response.c_str(), response.size(), 0);
  }
  close(socket_fd);
}
