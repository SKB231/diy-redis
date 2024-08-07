#include "string_utils.h"
#include <arpa/inet.h>
#include <cctype>
#include <chrono>
#include <climits>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <exception>
#include <iostream>
#include <memory>
#include <mutex>
#include <netdb.h>
#include <netinet/in.h>
#include <set>
#include <stdexcept>
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
void request_ack();
std::string get_resp_bulkstring(std::string word);
std::string *get_full_message(int socket_fd, int *count);
const int BUF_SIZE = 250;
class RedisStream;

// Global Variables:
using vector_strings = std::vector<std::string>;
using redis_map = std::unordered_map<std::string, std::string>;
using redis_stream_map =
    std::unordered_map<std::string, unique_ptr<RedisStream>>;
using stream_entry = pair<string, pair<string, string>>; // <id, <key, val>>
using stack_of_pairs = std::vector<stream_entry>;
using std::cout;
using std::pair;
using SHOULD_INSERT_TO_ACK_FD = bool;
using transaction_map = unordered_map<int, vector<string>>;
redis_map mem_database{};
bool in_multi_state = false;
redis_stream_map streams{};
set<int> validated_fd{};
long long total_written = -1;
long long next_written = 0;
int replica_validation_count = 0;
bool wait_command_progressing = false;
int transaction_count = 0;
// vector<string> txn_queue{};
transaction_map txn_map{};

std::string get_resp_bulk_arr(std::vector<string> words);
template <typename T> bool contains(set<T> &s, T val) {
  return s.find(val) != s.end();
}

template <typename T, typename V> bool contains(unordered_map<T, V> &s, T val) {
  return s.find(val) != s.end();
}

class RedisStream {
  stack_of_pairs stream;

public:
  string stream_key{};

  explicit RedisStream(string stream_key) : stream_key(stream_key) {
    stream = stack_of_pairs();
  }
  string add_to_stream(string id, string key, string val) {
    cout << "Adding to stream: " << id << endl;
    if (id == "*") {
      // generate unix time
      uint64_t msSinceEpoch =
          chrono::duration_cast<chrono::milliseconds>(
              chrono::system_clock::now().time_since_epoch())
              .count();
      id = to_string(msSinceEpoch) + "-0";
      stream.push_back({id, {key, val}});
      return get_resp_bulkstring(id);
    }

    uint64_t milliseconds_time;
    int sequence_number;
    uint64_t top_ms_time = 0;
    int top_seq_num = 0;
    bool wildcard_seq = false;
    bool wildcard_ms_time = false;
    bool tmp = false;
    if (stream.size() > 0) {
      stream_entry top_entry = stream.back();
      parse_id(top_entry.first, top_ms_time, top_seq_num, tmp, tmp);
    }
    parse_id(id, milliseconds_time, sequence_number, wildcard_ms_time,
             wildcard_seq);

    if (wildcard_seq) {
      int prev_seq_num;
      uint64_t tmp_i;
      if (stream.size() > 0) {
        find_top(false, milliseconds_time, tmp_i, prev_seq_num);
        sequence_number = prev_seq_num + 1;
      } else {
        sequence_number = 1;
      }
      cout << "Using new sequence number = " << sequence_number << endl;
      id = to_string(milliseconds_time) + "-" + to_string(sequence_number);
    }

    if (milliseconds_time == 0 && sequence_number <= 0) {
      return "-ERR The ID specified in XADD must be greater than 0-0\r\n";
    }

    cout << "Comparisions: " << endl;
    cout << milliseconds_time << " " << top_ms_time << endl;
    cout << sequence_number << " " << top_seq_num << endl;

    if (stream.size() > 0 &&
        (milliseconds_time < top_ms_time || (milliseconds_time == top_ms_time &&
                                             sequence_number <= top_seq_num))) {
      return "-ERR The ID specified in XADD is equal or smaller than the "
             "target stream top item\r\n";
    }
    stream.push_back({id, {key, val}});
    return get_resp_bulkstring(id);
  }
  string get_read(string start_id) {
    int start_idx = search_id(start_id);
    if (stream[start_idx].first == start_id) {
      start_idx += 1;
    }
    vector<string> final_resp{};
    for (int i = start_idx; i < stream.size(); i++) {
      // cout << "adding " << stream[i].first << endl;
      string val_element =
          get_resp_bulk_arr({stream[i].second.first, stream[i].second.second,
                             stream[i].second.first, stream[i].second.second});
      val_element =
          "*2\r\n" + get_resp_bulkstring(stream[i].first) + val_element;
      final_resp.push_back(val_element);
    }

    string resp_string = "*" + to_string(final_resp.size()) + "\r\n";
    for (auto x : final_resp) {
      resp_string += x;
    }

    return resp_string;
  }

  string get_range(string start_id, string end_id) {
    // first we get the idx for the end id directly
    int end_idx = search_id(end_id);
    int start_idx = search_id(start_id);
    if (stream[start_idx].first != start_id &&
        compare_ids(stream[start_idx].first, start_id) < 0) {
      start_idx += 1;
    }

    vector<string> final_resp{};
    for (int i = start_idx; i <= end_idx; i++) {
      cout << "adding " << stream[i].first << endl;
      string val_element =
          get_resp_bulk_arr({stream[i].second.first, stream[i].second.second,
                             stream[i].second.first, stream[i].second.second});
      val_element =
          "*2\r\n" + get_resp_bulkstring(stream[i].first) + val_element;
      final_resp.push_back(val_element);
    }

    cout << final_resp.size() << endl;

    string resp_string = "*" + to_string(final_resp.size()) + "\r\n";
    for (auto x : final_resp) {
      resp_string += x;
    }
    cout << endl;
    return resp_string;
  }

  static string
  get_resp_arr_of_arr(const vector<pair<string, string>> &stream_responses) {
    string return_string = "*" + to_string(stream_responses.size()) + "\r\n";
    for (auto element : stream_responses) {
      return_string += "*2\r\n";
      return_string += get_resp_bulkstring(element.first);
      return_string += element.second;
    }
    return return_string;
  }

  stream_entry stack_top(bool &isEmpty) {
    if (stream.size() == 0) {
      isEmpty = true;
    }
    isEmpty = false;
    return stream[stream.size() - 1];
  }

  ~RedisStream() = default;

private:
  bool validate_id(string id) {}
  void find_top(bool latest, uint64_t time, uint64_t &out_ms_time,
                int &out_seq_number) {
    stream_entry top_entry = stream.back();
    bool tmp;
    if (latest) {
      parse_id(top_entry.first, out_ms_time, out_seq_number, tmp, tmp);
      return;
    }

    // search for the first stream with time part of id = time
    cout << "Searching for entry with time: " << time << endl;

    int resp_idx = search_id(to_string(time) + "-" + to_string(INT_MAX));
    uint64_t ms_time;
    int seq_num;
    bool temp;
    parse_id(stream[resp_idx].first, ms_time, seq_num, temp, temp);
    if (ms_time == time) {
      out_ms_time = ms_time;
      out_seq_number = seq_num;
      return;
    }
    out_ms_time = time;
    out_seq_number = -1;
  }

  void parse_id(string id, uint64_t &millisecondsTime, int &sequenceNumber,
                bool &wildcard_ms_time, bool &wildcard_seq) {
    int pos = id.find("-", 0);
    string msTime = id.substr(0, pos);
    string seqNumber = id.substr(pos + 1, id.size() - pos - 1);
    try {
      if (msTime != "*") {
        millisecondsTime = std::stoll(msTime);
      } else {
        wildcard_ms_time = true;
      }
      if (seqNumber != "*") {
        sequenceNumber = std::stoi(seqNumber);
      } else {
        wildcard_seq = true;
      }
    } catch (const std::invalid_argument) {
      cerr << "Invalid Argument" << endl;
    }
  }

  // binary search for the closest id which is less than or equal to target_id
  int search_id(string target_id) {
    int start = 0;
    int end = stream.size();
    cout << "looking for: " << target_id << endl;
    while (end - 1 > start) {
      // cout << start << " => " << stream[start].first << "  ||||  "
      //      << stream[end - 1].first << " <= " << end << endl;
      int mid = (start + end) / 2; // floor the int
      // cout << mid << " <=> " << stream[mid].first << endl;
      int compare = compare_ids(stream[mid].first, target_id);
      if (compare == 0) {
        // cout << "Found at index: " << mid << " => " << stream[mid].first
        //      << endl;
        return mid;
      }
      if (compare < 0) {
        // mid is smaller than target
        start = mid;
      } else {
        // mid is larger than target
        end = mid;
      }
    }
    // cout << "Ending at: " << start << " -> " << stream[start].first <<
    // endl;
    return start;
  }

  // compare id left and right of the format <millisecond>-<sequencenumber>.
  // Note that no wildcards are expected. returns 0 if both are equal, -1 if
  // left is smaller, +1 if left is larger
  int compare_ids(string left, string right) {
    bool temp;
    uint64_t left_ms, right_ms;
    int left_seq, right_seq;
    parse_id(left, left_ms, left_seq, temp, temp);
    parse_id(right, right_ms, right_seq, temp, temp);

    if (left_ms != right_ms)
      return left_ms > right_ms ? 1 : -1;

    if (right_seq == left_seq)
      return 0;

    return left_seq > right_seq ? 1 : -1;
  }
};

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
      std::unique_lock<std::mutex> lk(worker.m);

      // cout << "Thread " << worker.id << " waiting for job. \n";
      worker.cv.wait(lk);
      // cout << "Thread " << worker.id << " running a job. \n";

      if (worker.job_type == Job_Type::connection) {

        handle(worker.params.first, worker.params.second);

        // Unlock m and reset params for use in next iteration
        worker.params = pair<int, struct sockaddr_in *>{};
      } else {

        const auto start = std::chrono::high_resolution_clock::now();
        const auto timeWait =
            std::chrono::milliseconds(worker.params_deletion.first);
        while (true) {
          const auto end = std::chrono::high_resolution_clock::now();
          if (end - start >= timeWait)
            break;

          // If the duration takes more than a day:
          if (end - start >= std::chrono::seconds(3600 * 24)) {
            this_thread::sleep_for(std::chrono::seconds(3600 * 24));
            continue;
          }
          if (end - start >= std::chrono::seconds(3600)) {
            this_thread::sleep_for(std::chrono::seconds(3600));
            continue;
          }

          if (end - start >= std::chrono::seconds(60)) {
            this_thread::sleep_for(std::chrono::seconds(60));
            continue;
          }
        }
        // std::this_thread::sleep_for(
        //     std::chrono::milliseconds(worker.params_deletion.first - 10));
        cout << "Timeup for deletion\n";
        const std::string &key_to_delete = worker.params_deletion.second;
        size_t x = mem_database.erase(key_to_delete);
        worker.job_type = Job_Type::connection;
      }
      lk.unlock();
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
  // Master &operator=(const Master &master) = delete;
  // Master(const Master &master) = delete;

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
Master master(10);

struct Config_Settings {
public:
  enum { master, slave } server_role;
  string dir;
  string dbfilename;
  int server_port;
  pair<std::string, int> master_info;
  std::vector<int> replica_fd{};
};

Config_Settings parse_arguments(int argc, char **argv) {
  if (argc == 1) {
    auto returnConfig = Config_Settings{};
    returnConfig.server_role = Config_Settings::master;
    returnConfig.server_port = 6379;
    returnConfig.master_info = {};
    return returnConfig;
  }
  Config_Settings config_settings;

  config_settings.server_port = 6379;
  for (int i = 1; i < argc - 1; i += 2) {

    string query = std::string(argv[i]);
    string val = std::string(argv[i + 1]);

    if (query == "--port") {
      try {
        size_t siz{};
        int port_num = std::stoi(val, &siz, 10);
        config_settings.server_port = port_num;
      } catch (const std::exception &e) {
        cout << "Param conversion error\n";
        cout << e.what();
      }
    } else if (query == "--dbfilename") {
      config_settings.dbfilename = val;
      cout << "Setting dbfilename as " << val << endl;
    } else if (query == "--dir") {
      config_settings.dir = val;
      cout << "Setting dir as " << val << endl;
    } else if (query == "--replicaof") {
      // Slave settings
      config_settings.server_role = Config_Settings::slave;

      vector_strings master_info{};

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

void prefill_db() {
  if (config.dbfilename == "")
    return;
  std::string full_path = config.dir + "/" + config.dbfilename;
  redis_database db{full_path};
  db.read_database();
  if (!db.isHealthy) {
    return;
  }

  for (auto pair : db.redis_map) {
    if (contains(db.entry_expiration, pair.first)) {

      // if (lifetime > 0) {

      //  auto params = pair<int, std::string>{lifetime, command[i + 1]};
      //  cout << "Running delayed deletion by " << lifetime << "ms" <<
      //  std::endl; master.run_deletion(params);
      //}

      auto now = std::chrono::system_clock::now();
      long long now_timestamp = std::chrono::seconds(std::time(NULL)).count();
      now_timestamp = now_timestamp * 1000;

      cout << now_timestamp << "  vs  " << db.entry_expiration[pair.first]
           << endl;
      if (db.entry_expiration[pair.first] < now_timestamp) {
        continue;
      }
    }
    mem_database[pair.first] = pair.second;
  }
}

int main(int argc, char **argv) {

  config = parse_arguments(argc, argv);
  prefill_db();

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
  /*
  struct sockaddr_in {
          __uint8_t       sin_len;
          sa_family_t     sin_family;
          in_port_t       sin_port;
          struct  in_addr sin_addr;
          char            sin_zero[8];
  };
   */
  const struct sockaddr_in *server_addr =
      new sockaddr_in{.sin_family = AF_INET,
                      .sin_addr = INADDR_ANY,
                      .sin_port = htons(config.server_port)};

  const struct sockaddr *sockaddr_casted = (const struct sockaddr *)server_addr;

  // bind(int, const struct sockaddr *,socklen_t)
  if (bind((int)server_fd, sockaddr_casted, (socklen_t)sizeof(sockaddr)) != 0) {
    std::cerr << "Failed to bind to port " << config.server_port << std::endl;
    return 1;
  }

  int connection_backlog = 5;
  if (listen(server_fd, connection_backlog) != 0) {
    std::cerr << "listen failed\n";
    return 1;
  }

  if (config.server_role == Config_Settings::slave) {
    new std::thread{
        handshake_and_replication_handle}; // run handshake in a seperate
                                           // thread without requirement to
                                           // destroy
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
    pair<int, struct sockaddr_in *> params{client_fd,
                                           (struct sockaddr_in *)&client_addr};
    master.run_connection(params);
  }

  return 0;
}

std::string *get_full_message(int socket_fd, long long *count = nullptr) {
  using sString = std::string;

  int data_written = 1;
  auto *final_string = new sString("");
  char buff[BUF_SIZE];

  data_written = recv(socket_fd, buff, BUF_SIZE, 0);
  for (int i = 0; i < data_written; i++) {
    *final_string += buff[i];
  }
  cout << "From " << socket_fd << " DATA WRITTEN: " << data_written << " "
       << *final_string << std::endl;
  if (count) {
    *count += data_written;
  }
  if (data_written < 0) {
    final_string = nullptr;
  }
  return final_string;
}

vector_strings *split_by_clrf(std::string &full_message) {
  int last_token = 0;
  vector_strings *words{new vector_strings};
  for (int i = 0; i < full_message.length() - 1; i += 1) {
    if (full_message[i] == '\r' && full_message[i + 1] == '\n') {
      std::string word = full_message.substr(last_token, i - last_token);
      if (word.length() > 0 && word[0] != '*') {
        if (word[0] != '$' || word == "$") {
          cout << word << ", ";
          words->push_back(word);
        }
      }
      last_token = i + 2;
    }
  }
  cout << endl;
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

void append_vector(vector_strings &dest, vector_strings &source, int start,
                   int size) {
  for (int i = start; i < start + size; i++) {
    dest.push_back(source[i]);
  }
}

// Returns whether the command was sent by a client or replica
SHOULD_INSERT_TO_ACK_FD parse_command(vector_strings &command,
                                      vector_strings &resp,
                                      bool is_replica = false,
                                      int caller_fd = 0) {

  SHOULD_INSERT_TO_ACK_FD should_insert = false;
  for (int i = 0; i < command.size(); i += 1) {
    for (int j = 0; j < command[i].size(); j++) {
      command[i][j] = std::tolower(command[i][j]);
    }
  }
  // WRITE COMMANDS: set, incr, xadd
  for (int i = 0; i < command.size();) {
    cout << "Parsing command: " << command[i] << std::endl;

    // ECHO <string to echo>
    if (command[i] == "echo") {
      // return "+" + command[1] + "\r\n";
      if (!is_replica)
        resp.push_back("+" + command[i + 1] + "\r\n");
      i += 2;
      continue;
    }

    if (command[i] == "wait") {
      int timeout = std::stoi(command[i + 2]);

      if (!is_replica) {
        int count = config.replica_fd.size();
        if (count == 0 || total_written < 0) {
          resp.push_back(":" + std::to_string(count) + "\r\n");
        } else {
          wait_command_progressing = true;
          validated_fd.clear();
          replica_validation_count = 0;

          new thread(
              [&](vector_strings &resp) -> void {
                this_thread::sleep_for(std::chrono::milliseconds(200));
                request_ack();
              },
              std::ref(resp));
          cout << "Sleepiping for " << timeout << " ms" << endl;

          this_thread::sleep_for(std::chrono::milliseconds(timeout));
          int total_validated = validated_fd.size();
          cout << "Reached timeout... " << "Using size: " << total_validated
               << endl;
          resp.push_back(":" + std::to_string(total_validated) + "\r\n");
          wait_command_progressing = false;
          total_written += next_written;
          next_written = 0;
        }
      }
      i += 2;
    }

    // XREAD [BLOCK milliseconds] STREAMS key [key ...] id [id ...]
    if (command[i] == "xread") {
      // handle block option
      int64_t block_time = -1;
      if (command[i + 1] == "block") {
        block_time = stoll(command[i + 2]);
        i += 2;
      }

      i += 2;
      vector<string> stream_keys{};
      vector<string> target_ids{};
      while (i < command.size() && contains(streams, command[i])) {
        stream_keys.push_back(command[i]);
        i += 1;
      }
      vector<pair<string, string>> response_vec;
      for (int j = 0; j < stream_keys.size() && i < command.size(); j++, i++) {
        cout << "Getting read for " << stream_keys[j] << " " << command[i]
             << endl;
        if (command[i] == "$") {
          bool is_empty = false;
          auto top_element = streams[stream_keys[j]]->stack_top(is_empty);
          if (is_empty) {
            target_ids.push_back("0-0");
          } else {
            target_ids.push_back(top_element.first);
          }
        } else {
          target_ids.push_back(command[i]);
        }
        // resp_stream_map.push_back(
        //     {stream_keys[j],
        //     streams[stream_keys[j]]->get_read(command[i])});
      }

      // Keep querying till timeout
      bool empty_response = true;
      auto start_time = chrono::high_resolution_clock::now();
      std::chrono::milliseconds wait_time =
          std::chrono::milliseconds(block_time);
      while (empty_response) {
        response_vec.clear();
        for (int j = 0; j < stream_keys.size(); j++) {
          string query_response =
              streams[stream_keys[j]]->get_read(target_ids[j]);
          cout << "Response for " << stream_keys[j] << " => " << query_response
               << endl;
          empty_response =
              (query_response == "" ||
               query_response == "*0\r\n"); // check for empty response
          response_vec.push_back({stream_keys[j], query_response});
        }

        if (block_time == -1) {
          break;
        }
        auto now_time = chrono::high_resolution_clock::now();
        if (now_time - start_time < wait_time || block_time == 0) {
          // cout << (std::chrono::duration_cast<std::chrono::milliseconds>(
          //              now_time - start_time))
          //             .count()
          //      << "  vs " << wait_time.count() << endl;
          cout << "Waiting ..." << endl;
          this_thread::sleep_for(std::chrono::milliseconds(10));
          continue;
        }
        break;
      }
      if (!empty_response) {
        string resp_arr = RedisStream::get_resp_arr_of_arr(response_vec);
        resp.push_back(resp_arr);
      } else {
        resp.push_back("$-1\r\n");
      }

      continue;
    }

    // XRANGE stream_key_name start_id end_id
    if (command[i] == "xrange") {
      string stream_key_name = command[i + 1];
      string start_id = command[i + 2];
      string end_id = command[i + 3];
      if (start_id == "-") {
        start_id = "0-0";
      }

      resp.push_back(streams[stream_key_name]->get_range(start_id, end_id));
      i += 4;
      continue;
    }

    if (command[i] == "xadd") {
      string stream_key_name = command[i + 1];
      int offset = 0;
      string entryID = command[i + 2];
      cout << command.size() << " || " << i << endl;
      if (command.size() < i + 5) {
        offset = -1;
        entryID = "*";
      }
      string key = command[i + 3 + offset];
      string val = command[i + 4 + offset];

      if (!contains(streams, stream_key_name)) {
        auto rd_stream =
            unique_ptr<RedisStream>(new RedisStream(stream_key_name));

        streams.insert({stream_key_name, std::move(rd_stream)});
      }
      string addResponse =
          streams[stream_key_name]->add_to_stream(entryID, key, val);
      resp.push_back(addResponse);
      i += (5 + offset);
      continue;
    }

    // PING
    if (command[i] == "ping") {
      // return "+" + std::string("PONG") + "\r\n";

      if (!is_replica)
        resp.push_back("+" + std::string("PONG") + "\r\n");
      i += 1;
      continue;
    }

    // INCR key
    if (command[i] == "incr") {
      cout << "IN MULTI STATE: " << in_multi_state << endl;
      if (contains(txn_map, caller_fd)) {
        append_vector(txn_map[caller_fd], command, i, 2);
        resp.push_back("+QUEUED\r\n");
        i += 2;
        continue;
      }

      string key = command[i + 1];

      i += 2;
      if (!contains(mem_database, key)) {
        mem_database[key] = "1";
        resp.push_back(":1\r\n");
        continue;
      }

      string current_key = mem_database[key];
      try {
        long int_key = stoi(current_key);
        int_key += 1;
        resp.push_back(":" + to_string(int_key) + "\r\n");
        mem_database[key] = to_string(int_key);
      } catch (...) {
        resp.push_back("-ERR value is not an integer or out of range\r\n");
      }
      continue;
    }

    // EXEC
    if (command[i] == "exec") {
      if (!contains(txn_map, caller_fd)) {
        resp.push_back("-ERR EXEC without MULTI\r\n");
        i += 1;
        continue;
      }
      vector_strings exec_responses{};

      cout << "EXEC QUEUE: " << endl;
      for (auto x : txn_map[caller_fd]) {
        cout << x << ", ";
      }

      vector_strings txn_queue{std::move(txn_map[caller_fd])};
      txn_map.erase(caller_fd);

      SHOULD_INSERT_TO_ACK_FD exec_resp =
          parse_command(txn_queue, exec_responses, is_replica, caller_fd);
      should_insert = should_insert || exec_resp;

      string resp_element = "*" + to_string(exec_responses.size()) + "\r\n";

      for (auto x : exec_responses) {
        resp_element += x;
      }
      resp.push_back(resp_element);
    }

    if (command[i] == "discard") {
      if (!contains(txn_map, caller_fd)) {
        resp.push_back("-ERR DISCARD without MULTI\r\n");
        i += 1;
        continue;
      }

      txn_map.erase(caller_fd);
      resp.push_back("+OK\r\n");
      i += 1;
      continue;
    }

    // MULTI
    if (command[i] == "multi") {
      resp.push_back("+OK\r\n");
      in_multi_state = true;
      if (contains(txn_map, caller_fd))
        txn_map.erase(caller_fd);

      txn_map[caller_fd] = vector_strings{};
      i += 1;
      continue;
    }

    // SET key value [EX seconds]
    if (command[i] == "set") {
      if (contains(txn_map, caller_fd)) {
        resp.push_back("+QUEUED\r\n");
        append_vector(txn_map[caller_fd], command, i, 3);
        i += 3;
        if (i < command.size() && command[i] == "ex") {
          append_vector(txn_map[caller_fd], command, i, 2);
          i += 2;
        }
        continue;
      }

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
        cout << "Running delayed deletion by " << lifetime << "ms" << std::endl;
        master.run_deletion(params);
      }

      // return "+" + std::string("OK") + "\r\n";
      i += skip_amount;
      cout << "End SET Command Skipping by " << skip_amount << " to " << i
           << std::endl;
      if (!is_replica)
        resp.push_back("+OK\r\n");
      continue;
    }

    // TYPE key
    if (command[i] == "type") {
      string key = command[i + 1];

      if (contains(mem_database, key)) {
        resp.push_back("+string\r\n");
      } else if (contains(streams, key)) {
        resp.push_back("+stream\r\n");
      } else {
        resp.push_back("+none\r\n");
      }
      i += 2;
      continue;
    }
    // KEYS
    if (command[i] == "keys") {

      vector<string> allkeys{};

      for (auto pair : mem_database) {
        allkeys.push_back(pair.first);
      }
      resp.push_back(get_resp_bulk_arr(allkeys));
      i += 1;
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
        command[i + 1] == "ack") {
      if (command.size() > 2 && command[0] == "replconf" &&
          command[1] == "ack") {
        const int replica_written = std::stoi(command[2]);
        cout << "The replica has written: " << replica_written
             << " Server recorded : " << total_written << endl;
        if (replica_written == total_written) {
          replica_validation_count += 1;
          should_insert = true;
        }
      }
      i += 2;
    }

    if (i + 1 < command.size() && command[i] == "replconf" &&
        command[i + 1] == "getack") {

      if (total_written < 0) {
        resp.push_back(get_resp_bulk_arr({"REPLCONF", "ACK", "0"}));
        total_written = 0;
      } else {
        resp.push_back(get_resp_bulk_arr(
            {"REPLCONF", "ACK", std::to_string(total_written)}));
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
      cout << "PSYNC COMMAND CALLED";
      continue;
    }

    if (command[i] == "get") {
      if (contains(txn_map, caller_fd)) {
        resp.push_back("+QUEUED\r\n");
        append_vector(txn_map[caller_fd], command, i, 2);
        i += 2;
        continue;
      }

      if (command[i + 1] == "dir") {
        resp.push_back(get_resp_bulk_arr({"dir", config.dir}));
        i += 2;
        continue;
      }
      if (command[i + 1] == "dbfilename") {
        resp.push_back(get_resp_bulk_arr({"dbfilename", config.dbfilename}));
        i += 2;
        continue;
      }

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
  return should_insert;
}

/**
 * convert_to_binary converts a hex string into an array of bytes containing
 * the data. Note that the bytes are packed in little endian format and set
 * the buffer
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
    std::string empty_rdb_file_hex = "524544495330303131fa0972656469732d76657"
                                     "205372e322e30fa0a72656469732d62"
                                     "697473c040fa056374696d65c26d08bc65fa087"
                                     "57365642d6d656dc2b0c41000fa0861"
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

void follow_up_slave(vector_strings &req, std::string original_req) {
  if (config.server_role == Config_Settings::slave || req[0] != "set")
    return;
  // Master to keep track of total_written
  cout << "Propogating request to slaves: " << original_req.size() << " => "
       << original_req << endl;
  for (auto fd : config.replica_fd) {
    cout << "Propogating to :" << fd << endl;
    new std::thread([=]() -> void {
      send(fd, (void *)original_req.c_str(), original_req.size(), 0);
    });
  }
  if (total_written < 0)
    total_written = 0;
  total_written += original_req.size();
}

// We are going to operate on elements of a vector containing which FDs should
// be tested for. Whenever a file descriptor is confirmed, we remove it from
// the array It's reset in the function call in the command_parser method.
/**
 * The plan is to use this array scoped only to the wait request.
 * - Prereqs:
 * -- The get_full_message should have a max timeout of 500 ms.
 * -- If the replca total_written_match our total_written, remove the element
 * from the array, by setting it to -1.
 * -- Wait min(100 ms, param_timeout) after calling request_ack(), and check
 * the number of confirmed replicas. If the count is greater than the param ,
 * return the count.
 * -- Else: if more time is remaining, then redo the above. Else, send the
 * count that's completed.
 */
void request_ack() {
  cout << "Requesting replicas for confirmation..." << endl;
  std::string req = get_resp_bulk_arr({"REPLCONF", "GETACK", "*"});
  std::set<int> validated_replicas{};

  for (auto fd : config.replica_fd) {
    if (contains(validated_fd, fd))
      continue;
    // struct timeval tv{};
    // tv.tv_sec = 0;
    // tv.tv_usec =50;
    //  Setting timeout to not ensure we don't wait too long
    // setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof tv);
    cout << "Requesting: " << fd << endl;
    new std::thread([=]() -> void {
      cout << "CALLING " << fd << endl;
      // Trigger requests to the replica servers. The replica server handlers
      // should duress by updating the global replica_fd set.
      send(fd, (void *)req.c_str(), req.size(), 0);
    });
  }
  next_written = req.size();
}

void handle(int socket_fd, struct sockaddr_in *client_addr) {
  bool closefd = false;
  while (!closefd) {

    cout << std::to_string(socket_fd) + " Master - Listening for message: "
         << std::endl;
    std::string *req_ptr = get_full_message(socket_fd);
    if (!req_ptr) {
      cout << "Empty string... skipping" << endl;
      continue;
    }
    std::string req = *(req_ptr);
    if (req.length() <= 1) {
      break;
    }
    cout << std::to_string(socket_fd) + " Master - Received message: " << req
         << std::endl;
    vector_strings *all_words = split_by_clrf(req);
    cout << std::to_string(socket_fd) + " Master - ARRAY: ";

    for (std::string word : *all_words) {
      cout << word << ", ";
    }
    cout << std::endl;

    vector_strings response{};
    bool tmp;
    bool should_insert = (parse_command(*all_words, response, tmp, socket_fd));
    if (should_insert) {
      validated_fd.insert(socket_fd);
    }

    for (int i = 0; i < response.size(); i++) {
      std::string resp = response[i];
      send(socket_fd, (void *)resp.c_str(), resp.size(), 0);
      follow_up_commands(resp, socket_fd);
      follow_up_slave(*all_words, req);
    }
  }
  close(socket_fd);
}

/**
 * This is the handling method for the slave server that, after handshake,
 * receives updates from the server containing database updates
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

    long long *counter = (total_written < 0) ? nullptr : &total_written;
    std::string req = *(get_full_message(replica_fd, counter));

    if (req.length() <= 1) {
      break;
    }
    cout << "Replica - Received message: " << req << std::endl;
    cout << "Splitting by CLRF" << std::endl;
    vector_strings *all_words = split_by_clrf(req);
    cout << "Replica - ARRAY: ";

    for (std::string word : *all_words) {
      cout << word << ", ";
    }
    cout << std::endl;
    vector_strings response{};
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
