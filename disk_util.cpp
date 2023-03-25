#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <libaio.h>
#include <iostream>
#include <thread>
#include "disk_util.h"

DiskUtil::DiskUtil(const std::string& file_path, int block_size)
  : file_path_(file_path), block_size_(block_size) {
  fd_ = open_file();
  if (fd_ < 0) {
      throw std::runtime_error("Failed to open file");
  }
  io_ctx_ = 0;
  int rc = io_setup(128, &io_ctx_);
  if (rc < 0) {
      close_file();
      throw std::runtime_error("Failed to setup io context");
  }
  is_running_ = true;
  io_worker_ = std::thread(std::bind(&DiskUtil::io_worker_thread, this));
}

DiskUtil::~DiskUtil() {
  stop();
  close_file();
  io_destroy(io_ctx_);
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
    if (io_ctx_) {
      io_destroy(io_ctx_);
      io_ctx_ = 0;
    }
    if (io_worker_.joinable()) {
      io_worker_.join();
    }
  }
}

void DiskUtil::io_worker_thread() {
  const int MAX_EVENTS = 128;
  struct io_event events[MAX_EVENTS];
  while (is_running_) {
    int num_events = 0;
    while (num_events <= 0) {
      num_events = io_getevents(io_ctx_, 1, MAX_EVENTS, events, NULL);
      if (num_events < 0) {
        std::cerr << "io_getevents returned error: " << num_events << std::endl;
        break;
      }
    }
    if (num_events > 0) {
      complete_io_request(io_ctx_, events, num_events);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
}

int DiskUtil::open_file() {
  int flags = O_RDWR | O_DIRECT | O_SYNC | O_CREAT;
  int mode = S_IRUSR | S_IWUSR;
  return open(file_path_.c_str(), flags, mode);
}

void DiskUtil::close_file() {
  if (fd_ >= 0) {
    close(fd_);
    fd_ = -1;
  }
}

void DiskUtil::process_io_request(IORequest* req) {
  std::cout << "process op:" << req->opcode << std::endl;
  //if (req->opcode == OP_READ) {
  //  pread(fd_, req->buffer, req->length, req->offset);
  //} else if (req->opcode == OP_WRITE) {
  //  pwrite(fd_, req->buffer, req->length, req->offset);
  // }
  if (req->callback) {
    req->callback(0);
  }
}

int DiskUtil::submit_io_request(IORequest* req) {
  struct iocb* cb = new iocb;
  
  if (req->opcode == OP_WRITE) {
    io_prep_pwrite(cb, fd_, req->buffer, req->length, req->offset);
  } else {
    io_prep_pread(cb, fd_, req->buffer, req->length, req->offset);
  }

  // 将当前的请求的指针绑定到callback上
  cb->data = (void*)req;
  int num_events = 1;
  int rc = io_submit(io_ctx_, num_events, &cb);
  if (rc != num_events) {
    return -1;
  }
  return 0;
}

void DiskUtil::complete_io_request(io_context_t ctx, io_event* events, int num_events) {
  for (int i = 0; i < num_events; i++) {
    struct iocb *io_cb = reinterpret_cast<struct iocb *>(events[i].obj);
    IORequest *req = reinterpret_cast<IORequest *>(io_cb->data);
    process_io_request(req);
    delete io_cb;
  }
}

