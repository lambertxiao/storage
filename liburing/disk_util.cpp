#include <vector>
#include <string>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <stdexcept>
#include <iostream>
#include <liburing.h>
#include "disk_util.h"

DiskUtil::DiskUtil(const std::string& file_path, int block_size)
 : file_path_(file_path), block_size_(block_size) {
  fd_ = open_file();
  if (fd_ < 0) {
    throw std::runtime_error("Failed to open file");
  }

  int ret = io_uring_queue_init(4, &io_ring_, 0);
  if (ret < 0) {
    close(fd_);
    throw std::runtime_error("Failed to setup io ring, ret:" + std::to_string(ret));
  }

  is_running_ = true;
  io_worker_ = std::thread(std::bind(&DiskUtil::io_worker_thread, this));
}

DiskUtil::~DiskUtil() {
  stop();
  io_uring_queue_exit(&io_ring_);
  close_file();
}

void DiskUtil::submit_request(IORequest* req) {
  submit_io_request(req);
}

void DiskUtil::start() {
  is_running_ = true;
}

void DiskUtil::stop() {
  if (is_running_) {
    is_running_ = false;
    if (io_worker_.joinable()) {
        io_worker_.join();
    }
  }
}

int DiskUtil::open_file() {
  int flags = O_RDWR | O_DIRECT | O_CREAT;
  int mode = S_IRUSR | S_IWUSR;
  return open(file_path_.c_str(), flags);
}

void DiskUtil::close_file() {
  if (fd_ >= 0) {
    close(fd_);
  }
}

int DiskUtil::submit_io_request(IORequest* req) {
  struct io_uring_sqe* sqe = io_uring_get_sqe(&io_ring_);
  if (!sqe) {
      return -1;
  }  

  struct iovec iov;
  iov.iov_base = req->buffer;
  iov.iov_len = req->length;
  
  if (req->opcode == OP_READ) {
    io_uring_prep_readv(sqe, fd_, &iov, 1, req->offset);
  } else if (req->opcode == OP_WRITE) {
    io_uring_prep_writev(sqe, fd_, &iov, 1, req->offset);
  }
  io_uring_sqe_set_data(sqe, (void*)req);
  io_uring_submit(&io_ring_);
  return 0;
}

void DiskUtil::io_worker_thread() {
  while (is_running_) {
    struct io_uring_cqe* cqe = nullptr;
    int ret = io_uring_wait_cqe(&io_ring_, &cqe);
    if (ret < 0) {
      std::cout << "io_uring_wait_cqe error:" << ret << std::endl;
      return;
    }
    std::cout << "ret:" << ret << std::endl;
    if (cqe->res < 0) {
      std::cerr << "IO error: " << std::strerror(-cqe->res) << " ret_code:" << cqe->res << std::endl;
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
      cqe = nullptr;
      continue;
    } else {
      IORequest* req = (IORequest*)io_uring_cqe_get_data(cqe);
      if (req->callback) {
        req->callback(0);
      }
    }
    io_uring_cqe_seen(&io_ring_, cqe);
  }
 }

