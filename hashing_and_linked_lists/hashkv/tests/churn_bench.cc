#include "hashkv/kv_store.h"

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>
#include <unistd.h>

namespace {

using Clock = std::chrono::steady_clock;

void Check(hashkv::Status status, const std::string &error) {
  if (status != hashkv::Status::kOk) {
    std::cerr << hashkv::StatusName(status) << ": " << error << std::endl;
    std::exit(1);
  }
}

double Seconds(Clock::time_point start) {
  return std::chrono::duration<double>(Clock::now() - start).count();
}

}  // namespace

int main(int argc, char **argv) {
  const int records = argc > 1 ? std::stoi(argv[1]) : 30000;
  if (records <= 0 || records % 2 != 0) {
    std::cerr << "usage: churn_bench [positive even record count]" << std::endl;
    return 1;
  }
  char path[] = "/tmp/hashkv-churn-XXXXXX";
  const int fd = ::mkstemp(path);
  if (fd < 0) {
    std::cerr << "mkstemp failed" << std::endl;
    return 1;
  }
  ::close(fd);

  hashkv::Options options;
  options.path = path;
  options.truncate = true;
  std::string error;
  auto store = hashkv::Store::Open(options, &error);
  if (!store) {
    std::cerr << "open: " << error << std::endl;
    return 1;
  }

  const std::string value(512, 'v');
  auto start = Clock::now();
  for (int i = 0; i < records; ++i) {
    Check(store->Put("key-" + std::to_string(i), value, &error), error);
  }
  const double insert_seconds = Seconds(start);

  start = Clock::now();
  for (int i = 0; i < records; i += 2) {
    Check(store->Erase("key-" + std::to_string(i), &error), error);
  }
  const double delete_seconds = Seconds(start);

  start = Clock::now();
  for (int i = 0; i < records / 2; ++i) {
    Check(store->Put("new-" + std::to_string(i), value, &error), error);
  }
  const double reuse_seconds = Seconds(start);
  const hashkv::Stats stats = store->GetStats();

  std::cout << "records=" << records << " insert_s=" << insert_seconds
            << " delete_s=" << delete_seconds
            << " reuse_s=" << reuse_seconds
            << " reused=" << stats.reused_regions
            << " file_bytes=" << stats.file_bytes << std::endl;
  store.reset();
  ::unlink(path);
  ::unlink((std::string(path) + ".wal").c_str());
}
