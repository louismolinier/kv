#ifndef HASHKV_KV_STORE_H_
#define HASHKV_KV_STORE_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace hashkv {

enum class Status { kOk, kNotFound, kInvalidArgument, kIOError, kCorrupt };

const char *StatusName(Status status);

struct Options {
  std::string path;
  std::size_t bucket_count = 65536;
  bool truncate = false;
};

struct Stats {
  std::uint64_t records = 0;
  std::uint64_t file_bytes = 0;
  std::uint64_t free_regions = 0;
  std::uint64_t free_bytes = 0;
  std::uint64_t reused_regions = 0;
  // Renseignés uniquement dans le binaire construit avec HASHKV_PROFILE.
  std::uint64_t read_calls = 0;
  std::uint64_t write_calls = 0;
  std::uint64_t read_ns = 0;
  std::uint64_t write_ns = 0;
  std::uint64_t find_ns = 0;
  std::uint64_t scan_ns = 0;
  std::uint64_t lock_wait_ns = 0;
};

class Store {
 public:
  static std::unique_ptr<Store> Open(const Options &options,
                                     std::string *error = nullptr);
  ~Store();

  Store(const Store &) = delete;
  Store &operator=(const Store &) = delete;

  Status Get(const std::string &key, std::string *value,
             std::string *error = nullptr);
  Status Put(const std::string &key, const std::string &value,
             std::string *error = nullptr);
  Status Erase(const std::string &key, std::string *error = nullptr);
  Status Scan(const std::string &start_key, std::size_t limit,
              std::vector<std::pair<std::string, std::string>> *records,
              std::string *error = nullptr);

  bool Flush(std::string *error = nullptr);
  Stats GetStats();
  const std::string &path() const;

 private:
  class Impl;
  explicit Store(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
};

}  // namespace hashkv

#endif  // HASHKV_KV_STORE_H_
