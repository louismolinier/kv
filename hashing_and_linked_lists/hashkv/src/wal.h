#ifndef HASHKV_WAL_H_
#define HASHKV_WAL_H_

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace hashkv {

enum class WalOperation : std::uint32_t { kPut = 1, kErase = 2 };

struct WalEntry {
  WalOperation operation;
  const std::string *key;
  const std::string *value;
};

class Wal {
 public:
  using Snapshot = std::function<bool(Wal *, std::string *)>;
  using Replay = std::function<bool(WalOperation, const std::string &,
                                    const std::string &, std::string *)>;

  static std::unique_ptr<Wal> OpenExisting(const std::string &path,
                                            std::uint64_t bucket_count,
                                            std::string *error);
  static std::unique_ptr<Wal> CreateAtomic(const std::string &path,
                                            std::uint64_t bucket_count,
                                            const Snapshot &snapshot,
                                            std::string *error);
  ~Wal();

  Wal(const Wal &) = delete;
  Wal &operator=(const Wal &) = delete;

  // Append confirme une opération seulement après fsync du journal.
  bool Append(WalOperation operation, const std::string &key,
              const std::string &value, std::string *error);
  bool AppendBatch(const std::vector<WalEntry> &entries,
                   std::string *error);
  // Réservé à CreateAtomic : un seul fsync publie toute la photographie.
  bool AppendSnapshot(WalOperation operation, const std::string &key,
                      const std::string &value, std::string *error);
  bool ValidateAndTrim(std::string *error);
  bool ReplayEntries(const Replay &replay, std::string *error);
  bool Sync(std::string *error);
  std::uint64_t bytes() const { return bytes_; }
#ifdef HASHKV_PROFILE
  std::uint64_t sync_calls() const { return sync_calls_; }
  std::uint64_t sync_ns() const { return sync_ns_; }
#endif

 private:
  Wal(int fd, std::string path, std::uint64_t bytes,
      bool temporary);
  bool EncodeFrame(WalOperation operation, const std::string &key,
                   const std::string &value, std::uint64_t sequence,
                   std::string *buffer, std::string *error);
  bool WriteFrame(WalOperation operation, const std::string &key,
                  const std::string &value, std::string *error);

  int fd_;
  std::string path_;
  std::uint64_t bytes_;
  std::uint64_t sequence_ = 0;
  bool temporary_;
  bool poisoned_ = false;
#ifdef HASHKV_PROFILE
  std::uint64_t sync_calls_ = 0;
  std::uint64_t sync_ns_ = 0;
#endif
};

}  // namespace hashkv

#endif  // HASHKV_WAL_H_
