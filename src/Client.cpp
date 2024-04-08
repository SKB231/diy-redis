#include <arpa/inet.h>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <netdb.h>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

int main(int argc, char **argv) {
  std::cout << "Hello World";

  int client_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (client_fd < 0) {
    std::cerr << "Failed to create server socket\n";
    return 1;
  }

  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = INADDR_ANY;
  server_addr.sin_port = htons(6379);

  std::cout << "\nAttemtping to connect to server" << std::endl;
  int res =
      connect(client_fd, (struct sockaddr *)&server_addr, sizeof(server_addr));
  if (res < 0) {
    std::cout << "Failed to connect client to server";
  }

  std::cout << "Connected to server.\n";

  // std::string message = "*2\r\n$4\r\necho\r\n$3\r\nhey\r\n";
  std::string message = "*1\r\n$4\r\nping\r\n";
  std::cout << message.c_str();
  message += "\r\n";
  send(client_fd, (void *)message.c_str(), message.size(), 0);

  // message = message_2;
  // std::cout << message.c_str();
  // message += "\r\n";
  // send(client_fd, (void *)message.c_str(), message.size(), 0);
  close(client_fd);
  return 0;
}
