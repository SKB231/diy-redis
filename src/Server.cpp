#include <arpa/inet.h>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <netdb.h>
#include <ostream>
#include <string>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

void handle(int socket_fd, struct sockaddr *client_addr);

int main(int argc, char **argv) {
  // You can use print statements as follows for debugging, they'll be visible
  // when running tests.
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
    handle(client_fd, (struct sockaddr *)&client_addr);
  }

  return 0;
}

void handle(int socket_fd, struct sockaddr *client_addr) {
  char buff[32] = {};
  std::string final_message = "";
  while (true) {
    std::cout << "Listening for message: " << std::endl;
    int data_written = recv(socket_fd, buff, 32, 0);
    std::cout << "Recieved chunk of size " << data_written << "\n";
    if (data_written == -1) {
      std::cout << errno << "\n";
      break;
    }
    for (char i : buff) {
      final_message += i;
    }
    std::cout << "Message at the moment: \n" << final_message << std::endl;
    if (data_written < 32) {
      break;
    }
  }
  std::cout << "Recieved message: " << final_message;

  if (final_message.size() > 6) {
    int message_len = final_message.size();
    std::string pingCmd = final_message.substr(message_len - 6, 4);
    if (pingCmd == "ping") {
      std::string resp = std::string("+PONG\r\n");
      send(socket_fd, (void *)resp.c_str(), resp.size(), 0);
      std::cout << "sending message " << resp;
    }
  }
}
