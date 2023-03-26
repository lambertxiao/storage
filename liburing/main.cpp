#include <cstring>
#include <cstdlib>
#include <iostream>
#include "disk_util.h"

void test_read(DiskUtil& disk, off_t offset, size_t length) {
  char* buffer = nullptr;
  int ret = posix_memalign((void**)&buffer, 4096, length);
  if (ret < 0) {
    std::cout << "malloc buffer error";
    return;
  }

  IORequest* req = new IORequest;
  req->opcode = OP_READ;
  req->buffer = buffer;
  req->offset = offset;
  req->length = length;
  req->callback = [req](int ret) {
    if (ret == 0) {
      std::cout << "Read complete, data:" << req->buffer << std::endl;
    } else {
      std::cout << "Read failed: " << ret << std::endl;
    }

    free(req->buffer);
    delete req;
  };
  disk.submit_request(req);
}

void test_write(DiskUtil& disk, off_t offset, size_t length, const char* data) {
  char* buffer = nullptr;
  int ret = posix_memalign((void**)&buffer, 4096, length);
  if (ret < 0) {
    std::cout << "malloc buffer error";
    return;
  }
  
  std::memcpy(buffer, data, length);
  IORequest* req = new IORequest();
  req->opcode = OP_WRITE;
  req->buffer = buffer;
  req->offset = offset;
  req->length = length;
  req->callback = [req](int ret) {
    if (ret == 0) {
      std::cout << "Write complete" << std::endl;
    } else {
      std::cout << "Write failed: " << ret << std::endl;
    }
    free(req->buffer);
    delete req;
  };
  disk.submit_request(req);
}

int main() {
  DiskUtil disk("test.disk", BLOCK_SIZE);
  disk.start();
  size_t length = 4096;
  const char* data = "Hello world";
  test_write(disk, 0, length, data);
  std::this_thread::sleep_for(std::chrono::seconds(1));
  test_read(disk, 0, length);
  // std::system("rm test.disk");
  disk.stop();
  return 0;
}
