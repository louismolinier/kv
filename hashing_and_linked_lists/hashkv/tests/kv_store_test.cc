#include "hashkv/kv_store.h"

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <iostream>
#include <map>
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

void RemoveStoreFiles(const std::string &path) {
  ::unlink(path.c_str());
  ::unlink((path + ".wal").c_str());
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

void TestAllocator() {
  const std::string path = TempPath();
  hashkv::Options options;
  options.path = path;
  options.bucket_count = 16;
  options.truncate = true;
  std::string error;
  auto store = hashkv::Store::Open(options, &error);
  Check(store != nullptr, "allocator open: " + error);

  Put(store.get(), "big", std::string(4096, 'B'));
  const auto original_size = store->GetStats().file_bytes;
  CheckStatus(store->Erase("big", &error), hashkv::Status::kOk, error);
  Put(store.get(), "small", std::string(512, 's'));
  Check(store->GetStats().file_bytes == original_size,
        "split free region without growing the file");
  Check(store->GetStats().free_bytes > 3000,
        "split returns the unused tail to the allocator");

  store.reset();
  options.truncate = false;
  store = hashkv::Store::Open(options, &error);
  Check(store != nullptr, "reopen split region: " + error);
  Check(Get(store.get(), "small") == std::string(512, 's'),
        "reopen split region value");
  Put(store.get(), "another", std::string(512, 'a'));
  Check(store->GetStats().file_bytes == original_size,
        "reopened split tail is reusable");

  store.reset();
  options.truncate = true;
  store = hashkv::Store::Open(options, &error);
  Check(store != nullptr, "coalesce open: " + error);
  Put(store.get(), "left", std::string(128, 'L'));
  Put(store.get(), "middle", std::string(128, 'M'));
  Put(store.get(), "right", std::string(128, 'R'));
  const auto adjacent_size = store->GetStats().file_bytes;
  CheckStatus(store->Erase("left", &error), hashkv::Status::kOk, error);
  CheckStatus(store->Erase("right", &error), hashkv::Status::kOk, error);
  Check(store->GetStats().free_regions == 2,
        "separated free regions stay distinct");
  CheckStatus(store->Erase("middle", &error), hashkv::Status::kOk, error);
  Check(store->GetStats().free_regions == 1,
        "free regions are coalesced on both sides");

  store.reset();
  options.truncate = false;
  store = hashkv::Store::Open(options, &error);
  Check(store != nullptr, "reopen coalesced region: " + error);
  Put(store.get(), "combined", std::string(400, 'C'));
  Check(store->GetStats().file_bytes == adjacent_size,
        "coalesced region is reusable after reopen");
  Check(Get(store.get(), "combined") == std::string(400, 'C'),
        "coalesced region value");
  store.reset();
  RemoveStoreFiles(path);
}

void TestMixedOperations() {
  const std::string path = TempPath();
  hashkv::Options options;
  options.path = path;
  options.bucket_count = 16;
  options.truncate = true;
  std::string error;
  auto store = hashkv::Store::Open(options, &error);
  Check(store != nullptr, "mixed operations open: " + error);
  std::map<std::string, std::string> expected;
  std::uint32_t seed = 42;
  auto random = [&seed] {
    seed = seed * 1664525U + 1013904223U;
    return seed;
  };

  for (int i = 0; i < 1000; ++i) {
    const std::string key = "mixed-" + std::to_string(random() % 50);
    const std::uint32_t choice = random() % 4;
    if (choice < 2) {
      const std::string value(random() % 2048,
                              static_cast<char>('a' + random() % 26));
      Put(store.get(), key, value);
      expected[key] = value;
    } else if (choice == 2) {
      const bool existed = expected.erase(key) != 0;
      CheckStatus(store->Erase(key, &error),
                  existed ? hashkv::Status::kOk
                          : hashkv::Status::kNotFound,
                  error);
    } else {
      std::string value;
      const auto it = expected.find(key);
      CheckStatus(store->Get(key, &value, &error),
                  it == expected.end() ? hashkv::Status::kNotFound
                                       : hashkv::Status::kOk,
                  error);
      if (it != expected.end()) {
        Check(value == it->second, "mixed lookup value");
      }
    }

    if (i % 125 == 124) {
      std::vector<std::pair<std::string, std::string>> rows;
      const std::vector<std::pair<std::string, std::string>> oracle(
          expected.begin(), expected.end());
      CheckStatus(store->Scan("", 100, &rows, &error), hashkv::Status::kOk,
                  error);
      Check(rows == oracle, "mixed scan before reopen");
      store.reset();
      options.truncate = false;
      store = hashkv::Store::Open(options, &error);
      Check(store != nullptr, "mixed reopen: " + error);
      CheckStatus(store->Scan("", 100, &rows, &error), hashkv::Status::kOk,
                  error);
      Check(rows == oracle, "mixed scan after reopen");
    }
  }
  store.reset();
  RemoveStoreFiles(path);
}

void TestConcurrentCommitOrder() {
  const std::string path = TempPath();
  hashkv::Options options;
  options.path = path;
  options.bucket_count = 64;
  options.scan_cache_slots = 64;
  options.truncate = true;
  std::string error;
  auto store = hashkv::Store::Open(options, &error);
  Check(store != nullptr, "group commit open: " + error);

  std::atomic<bool> start {false};
  std::atomic<int> writers_done {0};
  std::vector<std::thread> writers;
  for (int thread = 0; thread < 4; ++thread) {
    writers.emplace_back([&, thread] {
      while (!start.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      for (int i = 0; i < 100; ++i) {
        const std::string key = "group-" + std::to_string(thread) + "-" +
                                std::to_string(i);
        Put(store.get(), key, "initial");
        Put(store.get(), key, std::string(256, 'a' + thread));
        if (i % 2 == 0) {
          std::string local_error;
          CheckStatus(store->Erase(key, &local_error), hashkv::Status::kOk,
                      local_error);
        }
        Put(store.get(), "shared", std::to_string(thread) + "-" +
                                       std::to_string(i));
      }
      writers_done.fetch_add(1, std::memory_order_release);
    });
  }
  std::thread maintenance([&] {
    while (!start.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
    for (int i = 0; i < 8; ++i) {
      std::string local_error;
      const bool okay = i % 2 == 0 ? store->Checkpoint(&local_error)
                                   : store->Flush(&local_error);
      Check(okay, "concurrent checkpoint/flush: " + local_error);
    }
  });
  std::thread observer([&] {
    while (!start.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
    while (writers_done.load(std::memory_order_acquire) != 4) {
      std::string value;
      std::string local_error;
      const auto status = store->Get("shared", &value, &local_error);
      Check(status == hashkv::Status::kOk ||
                status == hashkv::Status::kNotFound,
            "concurrent get: " + local_error);
      std::vector<std::pair<std::string, std::string>> rows;
      CheckStatus(store->Scan("", 4, &rows, &local_error),
                  hashkv::Status::kOk, local_error);
      Check(store->GetStats().records <= 401,
            "concurrent stats record bound");
      std::this_thread::yield();
    }
  });
  start.store(true, std::memory_order_release);
  for (auto &writer : writers) {
    writer.join();
  }
  maintenance.join();
  observer.join();
  Check(store->GetStats().records == 201,
        "record count after concurrent checkpoint and writes");
  const std::string shared_before = Get(store.get(), "shared");
  store.reset();
  options.truncate = false;
  store = hashkv::Store::Open(options, &error);
  Check(store != nullptr, "group commit reopen: " + error);
  Check(Get(store.get(), "shared") == shared_before,
        "replay preserves concurrent commit order");
  for (int thread = 0; thread < 4; ++thread) {
    for (int i = 0; i < 100; ++i) {
      const std::string key = "group-" + std::to_string(thread) + "-" +
                              std::to_string(i);
      if (i % 2 == 0) {
        std::string value;
        CheckStatus(store->Get(key, &value, &error),
                    hashkv::Status::kNotFound, error);
      } else {
        Check(Get(store.get(), key) == std::string(256, 'a' + thread),
              "replay preserves batched update");
      }
    }
  }
  store.reset();
  RemoveStoreFiles(path);
}

}  // namespace

int main() {
  const std::string path = TempPath();
  hashkv::Options options;
  options.path = path;
  options.bucket_count = 1;  // Force une seule linked list.
  options.scan_cache_slots = 64;
  options.truncate = true;

  std::string error;
  auto invalid_options = options;
  invalid_options.scan_cache_slots = 3;
  Check(hashkv::Store::Open(invalid_options, &error) == nullptr,
        "reject non-power-of-two scan cache");
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
  Put(store.get(), "c", "shorter");
  CheckStatus(store->Scan("c", 1, &scan, &error), hashkv::Status::kOk,
              error);
  Check(scan.size() == 1 && scan[0].second == "shorter",
        "scan index after shrinking update");
  CheckStatus(store->Erase("bb", &error), hashkv::Status::kOk, error);
  CheckStatus(store->Scan("b", 4, &scan, &error), hashkv::Status::kOk,
              error);
  Check(scan.size() == 3 && scan[1].first == "c",
        "scan index after delete");

  const std::string long_key(1600, 'z');
  const std::string binary_key("x\0y", 3);
  Put(store.get(), long_key, "long-key-value");
  Put(store.get(), binary_key, std::string("binary\0value", 12));
  Put(store.get(), "prefetch-fit", std::string(1400, 'p'));
  Put(store.get(), "prefetch-fallback", std::string(2000, 'f'));
  Put(store.get(), "empty-value", "");
  Check(Get(store.get(), long_key) == "long-key-value",
        "long key fallback");
  Check(Get(store.get(), binary_key) == std::string("binary\0value", 12),
        "binary key and value");
  Check(Get(store.get(), "prefetch-fit") == std::string(1400, 'p'),
        "prefetched value");
  Check(Get(store.get(), "prefetch-fallback") == std::string(2000, 'f'),
        "large value fallback");
  Check(Get(store.get(), "empty-value").empty(), "empty value");

  Put(store.get(), "spare", std::string(2048, 'S'));
  CheckStatus(store->Erase("spare", &error), hashkv::Status::kOk, error);

  Check(store->Flush(&error), "flush: " + error);
  store.reset();
  options.truncate = false;
  store = hashkv::Store::Open(options, &error);
  Check(store != nullptr, "reopen: " + error);
  Check(Get(store.get(), "b") == "small", "persistent update");
  Check(Get(store.get(), "c") == "shorter", "persistent update after grow");
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
  RemoveStoreFiles(path);
  TestAllocator();
  TestMixedOperations();
  TestConcurrentCommitOrder();
  std::cout << "kv_store_test: OK" << std::endl;
}
