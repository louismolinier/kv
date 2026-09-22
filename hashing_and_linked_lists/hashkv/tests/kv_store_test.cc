#include "hashkv/kv_store.h"

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

namespace {

void Check(bool condition, const std::string &message) {
  if (!condition) {
    std::cerr << "FAIL: " << message << std::endl;
    std::exit(1);
  }
}

void CheckStatus(hashkv::Status actual, hashkv::Status expected,
                 const std::string &error) {
  Check(actual == expected,
        std::string("expected ") + hashkv::StatusName(expected) + ", got " +
            hashkv::StatusName(actual) + " (" + error + ")");
}

std::string TempPath() {
  char path[] = "/tmp/hashkv-draft-test-XXXXXX";
  int fd = ::mkstemp(path);
  Check(fd >= 0, "mkstemp");
  ::close(fd);
  ::unlink(path);
  return path;
}

void Put(hashkv::Store *store, const std::string &key,
         const std::string &value) {
  std::string error;
  CheckStatus(store->Put(key, value, &error), hashkv::Status::kOk, error);
}

std::string Get(hashkv::Store *store, const std::string &key) {
  std::string value;
  std::string error;
  CheckStatus(store->Get(key, &value, &error), hashkv::Status::kOk, error);
  return value;
}

}  // namespace

int main() {
  const std::string path = TempPath();
  hashkv::Options options;
  options.path = path;
  options.bucket_count = 1;  // Force une seule linked list.
  options.truncate = true;

  std::string error;
  auto store = hashkv::Store::Open(options, &error);
  Check(store != nullptr, "open: " + error);

  Put(store.get(), "a", "one");
  Put(store.get(), "b", "two");
  Put(store.get(), "c", "three");
  Check(Get(store.get(), "a") == "one", "collision lookup");

  Put(store.get(), "b", std::string(2048, 'B'));
  Put(store.get(), "b", "small");
  Check(Get(store.get(), "b") == "small", "update");

  Put(store.get(), "large", std::string(4096, 'L'));
  const auto size_before = store->GetStats().file_bytes;
  CheckStatus(store->Erase("large", &error), hashkv::Status::kOk, error);
  Put(store.get(), "reused", std::string(1024, 'R'));
  const auto reuse_stats = store->GetStats();
  Check(reuse_stats.file_bytes == size_before, "free region was not reused");
  Check(reuse_stats.reused_regions > 0, "reuse counter");

  std::vector<std::pair<std::string, std::string>> scan;
  CheckStatus(store->Scan("b", 3, &scan, &error), hashkv::Status::kOk,
              error);
  Check(scan.size() == 3 && std::is_sorted(scan.begin(), scan.end()),
        "ordered scan");
  Check(scan[0].first == "b" && scan[1].first == "c" &&
            scan[2].first == "reused",
        "scan keys");

  // L'index de scan existe désormais : inserts, déplacements et deletes
  // doivent le maintenir à jour.
  Put(store.get(), "bb", "new");
  Put(store.get(), "c", std::string(3000, 'C'));
  CheckStatus(store->Scan("b", 4, &scan, &error), hashkv::Status::kOk,
              error);
  Check(scan.size() == 4 && scan[1].first == "bb" &&
            scan[2].second == std::string(3000, 'C'),
        "scan index after insert and growing update");
  CheckStatus(store->Erase("bb", &error), hashkv::Status::kOk, error);
  CheckStatus(store->Scan("b", 4, &scan, &error), hashkv::Status::kOk,
              error);
  Check(scan.size() == 3 && scan[1].first == "c",
        "scan index after delete");

  const std::string long_key(300, 'z');
  const std::string binary_key("x\0y", 3);
  Put(store.get(), long_key, "long-key-value");
  Put(store.get(), binary_key, std::string("binary\0value", 12));
  Check(Get(store.get(), long_key) == "long-key-value",
        "long key fallback");
  Check(Get(store.get(), binary_key) == std::string("binary\0value", 12),
        "binary key and value");

  Put(store.get(), "spare", std::string(2048, 'S'));
  CheckStatus(store->Erase("spare", &error), hashkv::Status::kOk, error);

  Check(store->Flush(&error), "flush: " + error);
  store.reset();
  options.truncate = false;
  store = hashkv::Store::Open(options, &error);
  Check(store != nullptr, "reopen: " + error);
  Check(Get(store.get(), "b") == "small", "persistent update");
  Check(Get(store.get(), "c") == std::string(3000, 'C'),
        "persistent growing update");
  Check(Get(store.get(), "reused") == std::string(1024, 'R'),
        "persistent reused region");
  Check(Get(store.get(), long_key) == "long-key-value",
        "persistent long key");
  const auto size_after_reopen = store->GetStats().file_bytes;
  Put(store.get(), "post-reopen", "fits-in-a-free-region");
  Check(store->GetStats().file_bytes == size_after_reopen,
        "free regions rebuilt after reopening");

  std::vector<std::thread> threads;
  for (int thread = 0; thread < 4; ++thread) {
    threads.emplace_back([&, thread] {
      for (int i = 0; i < 25; ++i) {
        Put(store.get(), "t" + std::to_string(thread) + "-" +
                             std::to_string(i),
            "value");
      }
    });
  }
  for (auto &thread : threads) {
    thread.join();
  }
  Check(Get(store.get(), "t3-24") == "value", "concurrent access");

  Put(store.get(), "hot", "old");
  std::atomic<bool> readers_ok {true};
  std::thread writer([&] {
    for (int i = 0; i < 200; ++i) {
      Put(store.get(), "hot", i % 2 == 0 ? "newer-value" : "old");
    }
  });
  std::vector<std::thread> readers;
  for (int thread = 0; thread < 3; ++thread) {
    readers.emplace_back([&] {
      for (int i = 0; i < 200; ++i) {
        std::string value;
        std::string local_error;
        if (store->Get("hot", &value, &local_error) != hashkv::Status::kOk ||
            (value != "old" && value != "newer-value")) {
          readers_ok.store(false);
          return;
        }
        std::vector<std::pair<std::string, std::string>> rows;
        if (store->Scan("hot", 1, &rows, &local_error) !=
                hashkv::Status::kOk ||
            rows.size() != 1 || rows[0].first != "hot" ||
            (rows[0].second != "old" &&
             rows[0].second != "newer-value")) {
          readers_ok.store(false);
          return;
        }
      }
    });
  }
  writer.join();
  for (auto &reader : readers) {
    reader.join();
  }
  Check(readers_ok.load(), "concurrent readers, scans and writer");

  store.reset();
  ::unlink(path.c_str());
  std::cout << "kv_store_test: OK" << std::endl;
}
