#include <functional>
#include <vector>
#include <string>
#include <libaio.h>
#include <thread>

enum IO_OP { OP_READ = 0, OP_WRITE = 1 };

struct IORequest {
  IO_OP opcode; // 0: read, 1: write
  char* buffer;
  off_t offset;
  size_t length;
  std::function<void(int)> callback;
};

class DiskUtil {
public:
  DiskUtil(const std::string& file_path, int block_size);
  ~DiskUtil();
  void submit_request(IORequest* req);
  void start();
  void stop();
private:
  void io_worker_thread();
  int open_file();
  void close_file();
  void process_io_request(IORequest* req);
  int submit_io_request(IORequest* req);
  void complete_io_request(io_context_t ctx, io_event* events, int num_events);
private:
  std::vector<IORequest> req_queue_;
  int fd_ = -1;
  const std::string file_path_;
  const int block_size_;
  // 对应一个会话的上下文
  io_context_t io_ctx_ = 0;
  std::thread io_worker_;
  bool is_running_ = false;
};

