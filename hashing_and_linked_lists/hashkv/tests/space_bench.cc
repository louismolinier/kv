#include "hashkv/kv_store.h"

#include <cstdlib>
#include <iostream>
#include <string>
#include <unistd.h>

namespace {

void Check(hashkv::Status status, const std::string &error) {
  if (status != hashkv::Status::kOk) {
    std::cerr << hashkv::StatusName(status) << ": " << error << std::endl;
    std::exit(1);
  }
}

}  // namespace

int main(int argc, char **argv) {
  const int records = argc > 1 ? std::stoi(argv[1]) : 1000;
  if (records <= 0) {
    std::cerr << "usage: space_bench [positive record count]" << std::endl;
    return 1;
  }
  char path[] = "/tmp/hashkv-space-XXXXXX";
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

  const std::string big(8192, 'B');
  const std::string small(1024, 's');
  for (int i = 0; i < records; ++i) {
    Check(store->Put("big-" + std::to_string(i), big, &error), error);
  }
  const auto original_bytes = store->GetStats().file_bytes;
  for (int i = 0; i < records; ++i) {
    Check(store->Erase("big-" + std::to_string(i), &error), error);
  }
  for (int i = 0; i < 4 * records; ++i) {
    Check(store->Put("small-" + std::to_string(i), small, &error), error);
  }
  const auto stats = store->GetStats();
  std::cout << "big_records=" << records << " small_records=" << 4 * records
            << " original_bytes=" << original_bytes
            << " final_bytes=" << stats.file_bytes
            << " growth_bytes=" << stats.file_bytes - original_bytes
            << " free_bytes=" << stats.free_bytes
            << " free_regions=" << stats.free_regions << std::endl;

  store.reset();
  options.truncate = false;
  store = hashkv::Store::Open(options, &error);
  if (!store) {
    std::cerr << "reopen: " << error << std::endl;
    return 1;
  }
  for (int i = 0; i < 4 * records; ++i) {
    std::string value;
    Check(store->Get("small-" + std::to_string(i), &value, &error), error);
    if (value != small) {
      std::cerr << "reopened value mismatch at " << i << std::endl;
      return 1;
    }
  }
  store.reset();
  ::unlink(path);
  ::unlink((std::string(path) + ".wal").c_str());
}
