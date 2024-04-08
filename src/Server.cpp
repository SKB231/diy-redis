#include <arpa/inet.h>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <mutex>
#include <netdb.h>
#include <netinet/in.h>
#include <new>
#include <ostream>
#include <string>
#include <sys/socket.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

void handle(int socket_fd, struct sockaddr_in *client_addr);

void test_handle(int socket_fd, struct sockaddr_in *client_addr);

class Worker {

public:
  std::pair<int, struct sockaddr_in *> params;
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

      // run the function
      handle(worker.params.first, worker.params.second);

      // Unlock m and reset params for use in next iteration
      worker.params = std::pair<int, struct sockaddr_in *>{};
      lk.unlock();
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

  void run(std::pair<int, struct sockaddr_in *> &params) {

    std::lock_guard<std::mutex> guard(
        worker_lock); // Use guard to lock worker vector usage
    workers[thread_index]->params = params;
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

int main(int argc, char **argv) {

  // You can use print statements as follows for debugging, they'll be visible
  // when running tests.
  std::cout << "Logs from your program will appear here!\n";

  // Our threads
  Master master = Master(4);

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
  server_addr.sin_port = htons(6379);

  if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) !=
      0) {
    std::cerr << "Failed to bind to port 6379\n";
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
    master.run(params);
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

void handle(int socket_fd, struct sockaddr_in *client_addr) {
  bool closefd = false;
  while (!closefd) {
    char buff[32] = {};
    std::string final_message = "";
    int total_written = 0;
    while (false) {
      // if (data_written == -1) {
      //   std::cout << errno << "\n";
      //   closefd = true;
      //   break;
      // }
      // for (char i : buff) {
      //   final_message += i;
      // }
      // total_written += data_written;
      // std::cout << "Message at the moment: \n" << final_message << std::endl;
      // if (data_written < 32) {
      //   closefd = true;
      //   break;
      // }
    }

    std::cout << "Listening for message: " << std::endl;
    int data_written = recv(socket_fd, buff, 32, 0);
    std::cout << "Client sent: " << data_written << std::endl;
    if (data_written <= 0) {
      // I'm not sure how this works, will have to look into it later
      std::cout << "Client most likely disconnected" << std::endl;
      break;
    }
    std::string resp = std::string("+PONG\r\n");
    send(socket_fd, (void *)resp.c_str(), resp.size(), 0);
    std::cout << "sent resp " << socket_fd << std::endl;
    // std::cout << "Recieved chunk of size " << data_written << "\n";
    //// Until stage 4:
    // break;
    // if (data_written > 0) {
    //   break;
    // }
    // final_message = final_message.substr(0, total_written);
    // std::cout << "Recieved message: " << final_message;
    // std::cout << final_message.size() << "\n";

    // if (final_message.size() >= 6) {
    //   int message_len = final_message.size();
    //   std::string pingCmd = final_message.substr(message_len - 6, 4);
    //   std::cout << "Last 6 characters " << pingCmd;
    //   if (pingCmd == "ping") {
    //     std::string resp = std::string("+PONG\r\n");
    //     send(socket_fd, (void *)resp.c_str(), resp.size(), 0);
    //     std::cout << "sending message " << resp;
    //   }
    // }
  }
  close(socket_fd);
}
