#include "hashkv/kv_store.h"
#include "wal.h"

#include <array>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cerrno>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <fcntl.h>
#include <limits>
#include <map>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

namespace hashkv {
namespace {

constexpr char kFileMagic[8] = {'H', 'A', 'S', 'H', 'K', 'V', '0', '1'};
constexpr std::uint32_t kRecordMagic = 0x48564b31U;
constexpr std::uint32_t kActive = 1;
constexpr std::uint32_t kFree = 2;
constexpr std::size_t kGetPrefetchBytes = 1536;
constexpr std::size_t kMaxScanCacheSlots = 32768;
constexpr std::size_t kMaxCachedScanValueBytes = 4096;

struct FileHeader {
  char magic[8];
  std::uint64_t bucket_count;
  std::uint64_t reserved;
};

struct RecordHeader {
  std::uint32_t magic;
  std::uint32_t state;
  std::uint64_t next;
  std::uint64_t slot_size;
  std::uint32_t key_size;
  std::uint32_t value_size;
};

static_assert(sizeof(FileHeader) == 24, "unexpected file header size");
static_assert(sizeof(RecordHeader) == 32, "unexpected record header size");

#ifdef HASHKV_PROFILE
using Clock = std::chrono::steady_clock;

struct Timer {
  explicit Timer(std::atomic<std::uint64_t> *total)
      : total(total), start(Clock::now()) {}
  ~Timer() {
    total->fetch_add(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            Clock::now() - start)
            .count(),
        std::memory_order_relaxed);
  }
  std::atomic<std::uint64_t> *total;
  Clock::time_point start;
};

std::uint64_t ElapsedNs(Clock::time_point start) {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             Clock::now() - start)
      .count();
}
#endif

std::uint64_t Hash(const std::string &key) {
  std::uint64_t hash = 14695981039346656037ULL;
  for (char character : key) {
    hash ^= static_cast<unsigned char>(character);
    hash *= 1099511628211ULL;
  }
  return hash;
}

// Rebuilt from the WAL during Open, then discarded. A negative answer is
// certain: every live key has been added after its first successful Put.
// False positives merely take the ordinary on-disk Find path.
class ReplayFilter {
 public:
  bool MayContain(std::uint64_t hash) const {
    const std::uint64_t step = (hash >> 32) | 1ULL;
    for (std::uint64_t i = 0; i < 3; ++i) {
      const std::uint64_t bit = (hash + i * step) & (kBits - 1);
      if ((words_[bit >> 6] & (1ULL << (bit & 63))) == 0) {
        return false;
      }
    }
    return true;
  }

  void Add(std::uint64_t hash) {
    const std::uint64_t step = (hash >> 32) | 1ULL;
    for (std::uint64_t i = 0; i < 3; ++i) {
      const std::uint64_t bit = (hash + i * step) & (kBits - 1);
      words_[bit >> 6] |= 1ULL << (bit & 63);
    }
  }

 private:
  static constexpr std::uint64_t kBits = 1ULL << 19;
  std::array<std::uint64_t, kBits / 64> words_ {};
};

void SetError(std::string *error, const std::string &message) {
  if (error != nullptr) {
    *error = message;
  }
}

std::string SystemError(const std::string &operation) {
  return operation + ": " + std::strerror(errno);
}

}  // namespace

const char *StatusName(Status status) {
  switch (status) {
    case Status::kOk:
      return "ok";
    case Status::kNotFound:
      return "not found";
    case Status::kInvalidArgument:
      return "invalid argument";
    case Status::kIOError:
      return "I/O error";
    case Status::kCorrupt:
      return "corrupt";
  }
  return "unknown";
}

class Store::Impl {
 public:
  explicit Impl(Options options) : options_(std::move(options)) {}

  ~Impl() {
    if (fd_ >= 0) {
      ::close(fd_);
    }
  }

  bool Open(std::string *error) {
    if (options_.path.empty() || options_.bucket_count == 0) {
      SetError(error, "path and bucket_count are required");
      return false;
    }
    if (options_.scan_cache_slots > kMaxScanCacheSlots ||
        (options_.scan_cache_slots != 0 &&
         (options_.scan_cache_slots & (options_.scan_cache_slots - 1)) != 0)) {
      SetError(error, "scan_cache_slots must be 0 or a power of two <= 32768");
      return false;
    }

    fd_ = ::open(options_.path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
    if (fd_ < 0) {
      SetError(error, SystemError("open"));
      return false;
    }
    if (::flock(fd_, LOCK_EX | LOCK_NB) != 0) {
      SetError(error, SystemError("lock data file"));
      return false;
    }

    try {
      buckets_.assign(options_.bucket_count, 0);
      if (options_.scan_cache_slots != 0) {
        scan_cache_.reset(new ScanCacheEntry[options_.scan_cache_slots]);
      }
    } catch (const std::bad_alloc &) {
      SetError(error, "not enough memory for the bucket array or scan cache");
      return false;
    }
    const std::string wal_path = options_.path + ".wal";
    if (options_.truncate) {
      wal_ = Wal::CreateAtomic(wal_path, options_.bucket_count,
                               [](Wal *, std::string *) { return true; },
                               error);
      return wal_ != nullptr && ResetDataFile(error);
    }

    struct stat wal_info {};
    if (::stat(wal_path.c_str(), &wal_info) == 0) {
      wal_ = Wal::OpenExisting(wal_path, options_.bucket_count, error);
      if (!wal_ || !wal_->ValidateAndTrim(error) ||
          !ResetDataFile(error)) {
        return false;
      }
      ReplayFilter replay_filter;
      // Le WAL validé est la source de vérité : un record du fichier de
      // données peut avoir été interrompu au milieu de son pwrite.
      const bool replayed = wal_->ReplayEntries(
          [this, &replay_filter](WalOperation operation,
                                 const std::string &key,
                                 const std::string &value,
                                 std::string *error) {
            const std::uint64_t key_hash = Hash(key);
            Status status;
            if (operation == WalOperation::kPut) {
              status = ApplyPut(key, value, error,
                                !replay_filter.MayContain(key_hash));
              if (status == Status::kOk) {
                replay_filter.Add(key_hash);
              }
            } else {
              status = replay_filter.MayContain(key_hash)
                           ? ApplyErase(key, error)
                           : Status::kNotFound;
            }
            if (status != Status::kOk &&
                !(operation == WalOperation::kErase &&
                  status == Status::kNotFound)) {
              SetError(error, "WAL replay failed: " +
                                  std::string(StatusName(status)));
              return false;
            }
            return true;
          }, error);
      return replayed;
    }
    if (errno != ENOENT) {
      SetError(error, SystemError("stat WAL"));
      return false;
    }

    struct stat info {};
    if (::fstat(fd_, &info) != 0) {
      SetError(error, SystemError("fstat"));
      return false;
    }

    if (info.st_size == 0) {
      FileHeader header {};
      std::memcpy(header.magic, kFileMagic, sizeof(kFileMagic));
      header.bucket_count = options_.bucket_count;
      if (!WriteAt(0, &header, sizeof(header), error)) {
        return false;
      }
      file_size_ = sizeof(header);
    } else {
      FileHeader header {};
      if (!ReadAt(0, &header, sizeof(header), error) ||
          std::memcmp(header.magic, kFileMagic, sizeof(kFileMagic)) != 0 ||
          header.bucket_count != options_.bucket_count) {
        SetError(error, "invalid file header or bucket_count mismatch");
        return false;
      }
      file_size_ = static_cast<std::uint64_t>(info.st_size);
    }

    if (!RebuildIndexes(error)) {
      return false;
    }
    // Migration d'un ancien fichier sans WAL : publier une photographie
    // complète et atomique avant d'autoriser la prochaine écriture.
    wal_ = Wal::CreateAtomic(
        wal_path, options_.bucket_count,
        [this](Wal *writer, std::string *error) {
          return WriteSnapshot(writer, error);
        }, error);
    return wal_ != nullptr;
  }

  Status Get(const std::string &key, std::string *value, std::string *error) {
    if (value == nullptr) {
      return Status::kInvalidArgument;
    }
#ifdef HASHKV_PROFILE
    const auto before_lock = Clock::now();
#endif
    std::shared_lock<std::shared_mutex> lock(mutex_);
    if (needs_recovery_) {
      SetError(error, "data file needs WAL recovery; reopen the store");
      return Status::kIOError;
    }
#ifdef HASHKV_PROFILE
    profile_.lock_wait_ns.fetch_add(ElapsedNs(before_lock),
                                    std::memory_order_relaxed);
#endif
    Found found;
    Status status = Find(key, &found, error, value);
    if (status != Status::kOk) {
      return status;
    }
    return (found.value_loaded ||
            ReadValue(found.offset, found.header, value, error))
               ? Status::kOk
               : Status::kIOError;
  }

  Status Put(const std::string &key, const std::string &value,
             std::string *error) {
    if (key.size() > std::numeric_limits<std::uint32_t>::max() ||
        value.size() > std::numeric_limits<std::uint32_t>::max()) {
      SetError(error, "key or value is too large");
      return Status::kInvalidArgument;
    }
    return Submit(WalOperation::kPut, key, value, error);
  }

  Status Erase(const std::string &key, std::string *error) {
    if (key.size() > std::numeric_limits<std::uint32_t>::max()) {
      return Status::kNotFound;
    }
    static const std::string empty_value;
    return Submit(WalOperation::kErase, key, empty_value, error);
  }

  Status Scan(const std::string &start_key, std::size_t limit,
              std::vector<std::pair<std::string, std::string>> *records,
              std::string *error) {
    if (records == nullptr) {
      return Status::kInvalidArgument;
    }
#ifdef HASHKV_PROFILE
    const auto before_lock = Clock::now();
#endif
    std::shared_lock<std::shared_mutex> lock(mutex_);
    if (needs_recovery_) {
      SetError(error, "data file needs WAL recovery; reopen the store");
      return Status::kIOError;
    }
#ifdef HASHKV_PROFILE
    profile_.lock_wait_ns.fetch_add(ElapsedNs(before_lock),
                                    std::memory_order_relaxed);
    Timer scan_timer(&profile_.scan_ns);
#endif
    records->clear();
    if (limit == 0) {
      return Status::kOk;
    }
    if (!ordered_ready_) {
      lock.unlock();
#ifdef HASHKV_PROFILE
      const auto before_build_lock = Clock::now();
#endif
      std::unique_lock<std::shared_mutex> build_lock(mutex_);
#ifdef HASHKV_PROFILE
      profile_.lock_wait_ns.fetch_add(ElapsedNs(before_build_lock),
                                      std::memory_order_relaxed);
#endif
      if (!ordered_ready_) {
        const Status index_status = EnsureOrderedIndex(error);
        if (index_status != Status::kOk) {
          return index_status;
        }
      }
      build_lock.unlock();
      lock.lock();
    }
    for (auto it = ordered_.lower_bound(start_key);
         it != ordered_.end() && records->size() < limit; ++it) {
      const ScanEntry &entry = it->second;
      std::string value;
      if (!ReadScanValue(entry.offset,
                         entry.offset + sizeof(RecordHeader) +
                             it->first.size(),
                         entry.value_size, &value, error)) {
        return Status::kIOError;
      }
      records->emplace_back(it->first, std::move(value));
    }
    return Status::kOk;
  }

  bool Flush(std::string *error) {
    std::lock_guard<std::mutex> commit_lock(commit_mutex_);
    std::unique_lock<std::shared_mutex> lock(mutex_);
    if (needs_recovery_ || !wal_->Sync(error)) {
      if (needs_recovery_) {
        SetError(error, "data file needs WAL recovery; reopen the store");
      }
      return false;
    }
    if (::fsync(fd_) != 0) {
      SetError(error, SystemError("fsync"));
      return false;
    }
    return true;
  }

  bool Checkpoint(std::string *error) {
    std::lock_guard<std::mutex> commit_lock(commit_mutex_);
    std::unique_lock<std::shared_mutex> lock(mutex_);
    if (needs_recovery_) {
      SetError(error, "data file needs WAL recovery; reopen the store");
      return false;
    }
    auto replacement = Wal::CreateAtomic(
        options_.path + ".wal", options_.bucket_count,
        [this](Wal *writer, std::string *error) {
          return WriteSnapshot(writer, error);
        }, error);
    if (!replacement) {
      // Si rename a déjà eu lieu, l'ancien descripteur WAL n'est plus
      // forcément le fichier nommé. Ne plus accepter d'écritures.
      needs_recovery_ = true;
      return false;
    }
    wal_ = std::move(replacement);
    return true;
  }

  Stats GetStats() {
    std::lock_guard<std::mutex> commit_lock(commit_mutex_);
    std::shared_lock<std::shared_mutex> lock(mutex_);
    Stats stats;
    stats.records = record_count_;
    stats.file_bytes = file_size_;
    stats.wal_bytes = wal_ != nullptr ? wal_->bytes() : 0;
    stats.free_regions = free_regions_.size();
    stats.reused_regions = reused_regions_;
#ifdef HASHKV_PROFILE
    stats.read_calls = profile_.read_calls.load(std::memory_order_relaxed);
    stats.write_calls = profile_.write_calls.load(std::memory_order_relaxed);
    stats.read_ns = profile_.read_ns.load(std::memory_order_relaxed);
    stats.write_ns = profile_.write_ns.load(std::memory_order_relaxed);
    stats.find_ns = profile_.find_ns.load(std::memory_order_relaxed);
    stats.scan_ns = profile_.scan_ns.load(std::memory_order_relaxed);
    stats.scan_cache_hits =
        profile_.scan_cache_hits.load(std::memory_order_relaxed);
    stats.scan_cache_misses =
        profile_.scan_cache_misses.load(std::memory_order_relaxed);
    stats.lock_wait_ns = profile_.lock_wait_ns.load(std::memory_order_relaxed);
    if (wal_ != nullptr) {
      stats.wal_sync_calls = wal_->sync_calls();
      stats.wal_sync_ns = wal_->sync_ns();
    }
#endif
    for (const auto &region : free_regions_) {
      stats.free_bytes += region.first;
    }
    return stats;
  }

  const std::string &path() const { return options_.path; }

 private:
  struct ScanEntry {
    std::uint64_t offset;
    std::uint32_t value_size;
  };

  struct ScanCacheEntry {
    std::mutex mutex;
    std::uint64_t offset = 0;
    std::uint32_t value_size = 0;
    bool valid = false;
    std::string value;
  };

  struct Found {
    std::uint64_t offset = 0;
    std::uint64_t previous = 0;
    RecordHeader header {};
    bool value_loaded = false;
  };

  struct Pending {
    WalOperation operation;
    const std::string *key;
    const std::string *value;
    Status status = Status::kIOError;
    std::string error;
    bool done = false;
  };

  Status Submit(WalOperation operation, const std::string &key,
                const std::string &value, std::string *error) {
    Pending request {operation, &key, &value, Status::kIOError, "", false};
    std::unique_lock<std::mutex> queue_lock(queue_mutex_);
    pending_.push_back(&request);
    queue_cv_.notify_all();
    while (!request.done) {
      if (!batch_leader_ && !pending_.empty() &&
          pending_.front() == &request) {
        batch_leader_ = true;
        // Attendre seulement si plusieurs écrivains se présentent, ou si
        // un lot concurrent a été vu récemment. Après une série de lots
        // solitaires, revenir au chemin sans attente.
        if (pending_.size() > 1 || concurrent_recently_) {
          queue_cv_.wait_for(queue_lock, std::chrono::microseconds(50),
                             [this] { return pending_.size() >= 4; });
        }
        std::vector<Pending *> batch;
        while (!pending_.empty() && batch.size() < 32) {
          batch.push_back(pending_.front());
          pending_.pop_front();
        }
        if (batch.size() > 1) {
          concurrent_recently_ = true;
          singleton_batches_ = 0;
        } else if (concurrent_recently_ && ++singleton_batches_ >= 32) {
          concurrent_recently_ = false;
          singleton_batches_ = 0;
        }
        queue_lock.unlock();
        ProcessBatch(batch);
        queue_lock.lock();
        for (Pending *item : batch) {
          item->done = true;
        }
        batch_leader_ = false;
        queue_cv_.notify_all();
      } else {
        queue_cv_.wait(queue_lock);
      }
    }
    if (!request.error.empty()) {
      SetError(error, request.error);
    }
    return request.status;
  }

  void ProcessBatch(const std::vector<Pending *> &batch) {
    // WAL, checkpoint et Flush sont sérialisés. Tant que le WAL est en cours
    // de fsync, les index et le fichier de données restent inchangés : les
    // lecteurs peuvent donc poursuivre sous leur verrou partagé.
    std::lock_guard<std::mutex> commit_lock(commit_mutex_);
    if (needs_recovery_) {
      for (Pending *item : batch) {
        item->error = "data file needs WAL recovery; reopen the store";
      }
      return;
    }

    // Refuser avant le commit un lot qui ne pourrait même pas tenir dans le
    // fichier en supposant, de façon conservatrice, aucun slot réutilisé.
    std::uint64_t remaining = static_cast<std::uint64_t>(
                                  std::numeric_limits<off_t>::max()) -
                              file_size_;
    for (Pending *item : batch) {
      if (item->operation == WalOperation::kPut) {
        const std::uint64_t needed = sizeof(RecordHeader) +
                                     item->key->size() + item->value->size();
        if (needed > remaining) {
          for (Pending *rejected : batch) {
            rejected->status = Status::kInvalidArgument;
            rejected->error = "data file is too large";
          }
          return;
        }
        remaining -= needed;
      }
    }

    std::vector<WalEntry> entries;
    entries.reserve(batch.size());
    for (Pending *item : batch) {
      entries.push_back({item->operation, item->key, item->value});
    }
    std::string wal_error;
    if (!wal_->AppendBatch(entries, &wal_error)) {
      for (Pending *item : batch) {
        item->error = wal_error;
      }
      return;
    }
#ifdef HASHKV_PROFILE
    const auto before_lock = Clock::now();
#endif
    std::unique_lock<std::shared_mutex> lock(mutex_);
#ifdef HASHKV_PROFILE
    profile_.lock_wait_ns.fetch_add(ElapsedNs(before_lock),
                                    std::memory_order_relaxed);
#endif
    for (Pending *item : batch) {
      if (needs_recovery_) {
        item->error = "data file needs WAL recovery; reopen the store";
      } else {
        item->status = item->operation == WalOperation::kPut
                           ? ApplyPut(*item->key, *item->value, &item->error)
                           : ApplyErase(*item->key, &item->error);
      }
    }
  }

  Status ApplyPut(const std::string &key, const std::string &value,
                  std::string *error, bool key_known_absent = false) {
    Found found;
    const Status status = key_known_absent
                              ? Status::kNotFound
                              : Find(key, &found, error);
    if (status != Status::kOk && status != Status::kNotFound) {
      needs_recovery_ = true;
      return status;
    }
    const std::uint64_t needed = sizeof(RecordHeader) + key.size() + value.size();
    if (status == Status::kOk && needed <= found.header.slot_size) {
      found.header.value_size = static_cast<std::uint32_t>(value.size());
      if (!WriteRecord(found.offset, found.header, key, value, error)) {
        needs_recovery_ = true;
        return Status::kIOError;
      }
      if (ordered_ready_) {
        ordered_[key].value_size = found.header.value_size;
      }
      return Status::kOk;
    }

    std::uint64_t slot_size = needed;
    std::uint64_t new_offset = 0;
    if (!Allocate(needed, &new_offset, &slot_size, error)) {
      needs_recovery_ = true;
      return Status::kIOError;
    }
    RecordHeader new_header {kRecordMagic,
                             kActive,
                             status == Status::kOk ? found.header.next
                                                   : buckets_[Bucket(key)],
                             slot_size,
                             static_cast<std::uint32_t>(key.size()),
                             static_cast<std::uint32_t>(value.size())};
    if (!WriteRecord(new_offset, new_header, key, value, error)) {
      needs_recovery_ = true;
      return Status::kIOError;
    }
    if (status == Status::kOk) {
      if (found.previous == 0) {
        buckets_[Bucket(key)] = new_offset;
      } else if (!SetNext(found.previous, new_offset, error)) {
        needs_recovery_ = true;
        return Status::kIOError;
      }
      if (!MarkFree(found.offset, found.header.slot_size, error)) {
        needs_recovery_ = true;
        return Status::kIOError;
      }
    } else {
      buckets_[Bucket(key)] = new_offset;
      ++record_count_;
    }
    if (ordered_ready_) {
      ordered_[key] = {new_offset, new_header.value_size};
    }
    return Status::kOk;
  }

  Status ApplyErase(const std::string &key, std::string *error) {
    Found found;
    const Status status = Find(key, &found, error);
    if (status != Status::kOk) {
      if (status != Status::kNotFound) {
        needs_recovery_ = true;
      }
      return status;
    }
    const std::size_t bucket = Bucket(key);
    if (found.previous == 0) {
      buckets_[bucket] = found.header.next;
    } else if (!SetNext(found.previous, found.header.next, error)) {
      needs_recovery_ = true;
      return Status::kIOError;
    }
    if (!MarkFree(found.offset, found.header.slot_size, error)) {
      needs_recovery_ = true;
      return Status::kIOError;
    }
    --record_count_;
    if (ordered_ready_) {
      ordered_.erase(key);
    }
    return Status::kOk;
  }

  bool ResetDataFile(std::string *error) {
    if (::ftruncate(fd_, 0) != 0) {
      SetError(error, SystemError("truncate data file"));
      return false;
    }
    file_size_ = 0;
    record_count_ = 0;
    reused_regions_ = 0;
    std::fill(buckets_.begin(), buckets_.end(), 0);
    free_regions_.clear();
    free_by_offset_.clear();
    ordered_.clear();
    ordered_ready_ = false;
    FileHeader header {};
    std::memcpy(header.magic, kFileMagic, sizeof(kFileMagic));
    header.bucket_count = options_.bucket_count;
    if (!WriteAt(0, &header, sizeof(header), error)) {
      return false;
    }
    file_size_ = sizeof(header);
    return true;
  }

  bool WriteSnapshot(Wal *writer, std::string *error) {
    for (std::uint64_t head : buckets_) {
      for (std::uint64_t offset = head; offset != 0;) {
        RecordHeader header {};
        std::string key;
        std::string value;
        if (!ReadActiveHeader(offset, &header, error) ||
            !ReadKey(offset, header, &key, error) ||
            !ReadValue(offset, header, &value, error) ||
            !writer->AppendSnapshot(WalOperation::kPut, key, value,
                                    error)) {
          return false;
        }
        offset = header.next;
      }
    }
    return true;
  }

  Status EnsureOrderedIndex(std::string *error) {
    if (ordered_ready_) {
      return Status::kOk;
    }
    // L'index est créé au premier scan : les workloads de point lookup ne
    // paient ni son coût de construction ni sa consommation mémoire.
    for (std::uint64_t head : buckets_) {
      for (std::uint64_t offset = head; offset != 0;) {
        RecordHeader header {};
        if (!ReadActiveHeader(offset, &header, error)) {
          ordered_.clear();
          return Status::kCorrupt;
        }
        std::string key;
        if (!ReadKey(offset, header, &key, error)) {
          ordered_.clear();
          return Status::kIOError;
        }
        ordered_[std::move(key)] = {offset, header.value_size};
        offset = header.next;
      }
    }
    ordered_ready_ = true;
    return Status::kOk;
  }

  bool RebuildIndexes(std::string *error) {
    std::uint64_t offset = sizeof(FileHeader);
    while (offset < file_size_) {
      RecordHeader header {};
      if (!ReadAt(offset, &header, sizeof(header), error) ||
          !ValidHeader(offset, header)) {
        SetError(error, "invalid record at offset " + std::to_string(offset));
        return false;
      }
      if (header.state == kActive) {
        std::string key;
        if (!ReadKey(offset, header, &key, error)) {
          return false;
        }
        const std::size_t bucket = Bucket(key);
        header.next = buckets_[bucket];
        if (!WriteAt(offset, &header, sizeof(header), error)) {
          return false;
        }
        buckets_[bucket] = offset;
        ++record_count_;
      } else if (header.state == kFree) {
        // Les anciens fichiers peuvent contenir plusieurs régions libres
        // adjacentes. On les regroupe en RAM sans réécrire le fichier ici.
        if (!free_by_offset_.empty()) {
          auto last = std::prev(free_by_offset_.end());
          if (last->first + last->second == offset) {
            free_regions_.erase({last->second, last->first});
            last->second += header.slot_size;
            free_regions_.emplace(last->second, last->first);
          } else {
            AddFree(offset, header.slot_size);
          }
        } else {
          AddFree(offset, header.slot_size);
        }
      } else {
        SetError(error, "unknown record state");
        return false;
      }
      offset += header.slot_size;
    }
    return offset == file_size_;
  }

  Status Find(const std::string &key, Found *found, std::string *error,
              std::string *prefetched_value = nullptr) {
#ifdef HASHKV_PROFILE
    Timer find_timer(&profile_.find_ns);
#endif
    std::uint64_t previous = 0;
    std::uint64_t offset = buckets_[Bucket(key)];
    std::array<char, kGetPrefetchBytes> first_bytes {};
    while (offset != 0) {
      RecordHeader header {};
      bool key_matches = false;
      const bool short_key = key.size() <= first_bytes.size() - sizeof(header);
      const std::size_t prefix_size =
          short_key ? sizeof(header) + key.size() : 0;
      if (short_key && offset <= file_size_ &&
          prefix_size <= file_size_ - offset) {
        const std::size_t read_size = prefetched_value == nullptr
                                          ? prefix_size
                                          : static_cast<std::size_t>(
                                                std::min<std::uint64_t>(
                                                    first_bytes.size(),
                                                    file_size_ - offset));
        // Get précharge aussi la valeur courte ; les autres opérations
        // conservent la petite lecture en-tête + clé.
        if (!ReadAt(offset, first_bytes.data(), read_size, error)) {
          return Status::kIOError;
        }
        std::memcpy(&header, first_bytes.data(), sizeof(header));
        if (!ValidHeader(offset, header) || header.state != kActive) {
          SetError(error, "invalid active record in hash chain");
          return Status::kCorrupt;
        }
        key_matches = header.key_size == key.size() &&
                      std::memcmp(first_bytes.data() + sizeof(header),
                                  key.data(), key.size()) == 0;
        if (key_matches && prefetched_value != nullptr &&
            header.value_size <= read_size - prefix_size) {
          prefetched_value->assign(first_bytes.data() + prefix_size,
                                   header.value_size);
          found->value_loaded = true;
        }
      } else {
        if (!ReadActiveHeader(offset, &header, error)) {
          return Status::kCorrupt;
        }
        std::string stored_key;
        if (header.key_size == key.size() &&
            !ReadKey(offset, header, &stored_key, error)) {
          return Status::kIOError;
        }
        key_matches = header.key_size == key.size() && stored_key == key;
      }
      if (key_matches) {
        found->offset = offset;
        found->previous = previous;
        found->header = header;
        return Status::kOk;
      }
      previous = offset;
      offset = header.next;
    }
    return Status::kNotFound;
  }

  std::size_t Bucket(const std::string &key) const {
    return Hash(key) % buckets_.size();
  }

  void AddFree(std::uint64_t offset, std::uint64_t size) {
    free_regions_.emplace(size, offset);
    free_by_offset_.emplace(offset, size);
  }

  void RemoveFree(std::uint64_t offset, std::uint64_t size) {
    free_regions_.erase({size, offset});
    free_by_offset_.erase(offset);
  }

  bool Allocate(std::uint64_t needed, std::uint64_t *offset,
                std::uint64_t *slot_size, std::string *error) {
    const auto best = free_regions_.lower_bound({needed, 0});
    if (best != free_regions_.end()) {
      const std::uint64_t free_offset = best->second;
      const std::uint64_t free_size = best->first;
      const std::uint64_t remainder = free_size - needed;
      if (remainder >= sizeof(RecordHeader)) {
        RecordHeader tail {kRecordMagic, kFree, 0, remainder, 0, 0};
        if (!WriteAt(free_offset + needed, &tail, sizeof(tail), error)) {
          return false;
        }
      }
      RemoveFree(free_offset, free_size);
      if (remainder >= sizeof(RecordHeader)) {
        AddFree(free_offset + needed, remainder);
      }
      *offset = free_offset;
      *slot_size = remainder >= sizeof(RecordHeader) ? needed : free_size;
      ++reused_regions_;
      return true;
    }
    if (needed > static_cast<std::uint64_t>(
                     std::numeric_limits<off_t>::max()) - file_size_) {
      SetError(error, "data file is too large");
      return false;
    }
    *offset = file_size_;
    file_size_ += needed;
    return true;
  }

  bool MarkFree(std::uint64_t offset, std::uint64_t size,
                std::string *error) {
    InvalidateScanCache(offset);
    std::uint64_t start = offset;
    std::uint64_t end = offset + size;
    auto next = free_by_offset_.lower_bound(offset);
    auto first = next;
    while (first != free_by_offset_.begin()) {
      auto previous = std::prev(first);
      if (previous->first + previous->second != start) {
        break;
      }
      start = previous->first;
      first = previous;
    }
    auto after = next;
    while (after != free_by_offset_.end() && after->first == end) {
      end += after->second;
      ++after;
    }

    RecordHeader header {kRecordMagic, kFree, 0, end - start, 0, 0};
    if (!WriteAt(start, &header, sizeof(header), error)) {
      return false;
    }
    for (auto it = first; it != after;) {
      free_regions_.erase({it->second, it->first});
      it = free_by_offset_.erase(it);
    }
    AddFree(start, end - start);
    return true;
  }

  bool SetNext(std::uint64_t offset, std::uint64_t next,
               std::string *error) {
    RecordHeader header {};
    if (!ReadActiveHeader(offset, &header, error)) {
      return false;
    }
    header.next = next;
    return WriteAt(offset, &header, sizeof(header), error);
  }

  bool WriteRecord(std::uint64_t offset, const RecordHeader &header,
                   const std::string &key, const std::string &value,
                   std::string *error) {
    InvalidateScanCache(offset);
    // Une écriture système par record au lieu de trois petites écritures.
    std::string record(sizeof(header) + key.size() + value.size(), '\0');
    std::memcpy(record.data(), &header, sizeof(header));
    std::memcpy(record.data() + sizeof(header), key.data(), key.size());
    std::memcpy(record.data() + sizeof(header) + key.size(), value.data(),
                value.size());
    return WriteAt(offset, record.data(), record.size(), error);
  }

  bool ReadActiveHeader(std::uint64_t offset, RecordHeader *header,
                        std::string *error) {
    return ReadAt(offset, header, sizeof(*header), error) &&
           ValidHeader(offset, *header) && header->state == kActive;
  }

  bool ValidHeader(std::uint64_t offset, const RecordHeader &header) const {
    const std::uint64_t payload =
        static_cast<std::uint64_t>(header.key_size) + header.value_size;
    return header.magic == kRecordMagic &&
           header.slot_size >= sizeof(RecordHeader) &&
           offset <= file_size_ && header.slot_size <= file_size_ - offset &&
           (header.state != kActive ||
            payload <= header.slot_size - sizeof(RecordHeader));
  }

  bool ReadKey(std::uint64_t offset, const RecordHeader &header,
               std::string *key, std::string *error) {
    key->assign(header.key_size, '\0');
    return ReadAt(offset + sizeof(header), key->data(), key->size(), error);
  }

  bool ReadValue(std::uint64_t offset, const RecordHeader &header,
                 std::string *value, std::string *error) {
    value->assign(header.value_size, '\0');
    return ReadAt(offset + sizeof(header) + header.key_size, value->data(),
                  value->size(), error);
  }

  std::size_t ScanCacheSlot(std::uint64_t offset) const {
    return ((offset * 11400714819323198485ULL) >> 32) &
           (options_.scan_cache_slots - 1);
  }

  void InvalidateScanCache(std::uint64_t offset) {
    if (!scan_cache_) {
      return;
    }
    ScanCacheEntry &slot = scan_cache_[ScanCacheSlot(offset)];
    std::lock_guard<std::mutex> lock(slot.mutex);
    if (slot.valid && slot.offset == offset) {
      slot.valid = false;
      slot.value.clear();
    }
  }

  bool ReadScanValue(std::uint64_t record_offset,
                     std::uint64_t value_offset, std::uint32_t value_size,
                     std::string *value, std::string *error) {
    if (scan_cache_ && value_size <= kMaxCachedScanValueBytes) {
      ScanCacheEntry &slot = scan_cache_[ScanCacheSlot(record_offset)];
      {
        std::lock_guard<std::mutex> lock(slot.mutex);
        if (slot.valid && slot.offset == record_offset &&
            slot.value_size == value_size) {
          *value = slot.value;
#ifdef HASHKV_PROFILE
          ++profile_.scan_cache_hits;
#endif
          return true;
        }
      }
#ifdef HASHKV_PROFILE
      ++profile_.scan_cache_misses;
#endif
      value->assign(value_size, '\0');
      if (!ReadAt(value_offset, value->data(), value->size(), error)) {
        return false;
      }
      std::lock_guard<std::mutex> lock(slot.mutex);
      slot.offset = record_offset;
      slot.value_size = value_size;
      slot.value = *value;
      slot.valid = true;
      return true;
    }
    value->assign(value_size, '\0');
    return ReadAt(value_offset, value->data(), value->size(), error);
  }

  bool ReadAt(std::uint64_t offset, void *data, std::size_t size,
              std::string *error) {
#ifdef HASHKV_PROFILE
    Timer read_timer(&profile_.read_ns);
    ++profile_.read_calls;
#endif
    auto *cursor = static_cast<char *>(data);
    while (size != 0) {
      const ssize_t read = ::pread(fd_, cursor, size, static_cast<off_t>(offset));
      if (read < 0 && errno == EINTR) {
        continue;
      }
      if (read <= 0) {
        SetError(error, read == 0 ? "unexpected end of file"
                                  : SystemError("pread"));
        return false;
      }
      cursor += read;
      offset += static_cast<std::uint64_t>(read);
      size -= static_cast<std::size_t>(read);
    }
    return true;
  }

  bool WriteAt(std::uint64_t offset, const void *data, std::size_t size,
               std::string *error) {
#ifdef HASHKV_PROFILE
    Timer write_timer(&profile_.write_ns);
    ++profile_.write_calls;
#endif
    const auto *cursor = static_cast<const char *>(data);
    while (size != 0) {
      const ssize_t written =
          ::pwrite(fd_, cursor, size, static_cast<off_t>(offset));
      if (written < 0 && errno == EINTR) {
        continue;
      }
      if (written <= 0) {
        SetError(error, SystemError("pwrite"));
        return false;
      }
      cursor += written;
      offset += static_cast<std::uint64_t>(written);
      size -= static_cast<std::size_t>(written);
    }
    return true;
  }

  Options options_;
  int fd_ = -1;
  std::unique_ptr<Wal> wal_;
  bool needs_recovery_ = false;
  std::uint64_t file_size_ = 0;
  std::uint64_t record_count_ = 0;
  std::uint64_t reused_regions_ = 0;
  std::vector<std::uint64_t> buckets_;
  std::set<std::pair<std::uint64_t, std::uint64_t>> free_regions_;
  std::map<std::uint64_t, std::uint64_t> free_by_offset_;
  std::map<std::string, ScanEntry> ordered_;
  std::unique_ptr<ScanCacheEntry[]> scan_cache_;
  bool ordered_ready_ = false;
  std::shared_mutex mutex_;
  // Toujours pris avant mutex_ quand les deux sont nécessaires.
  std::mutex commit_mutex_;
  std::mutex queue_mutex_;
  std::condition_variable queue_cv_;
  std::deque<Pending *> pending_;
  bool batch_leader_ = false;
  bool concurrent_recently_ = false;
  std::uint32_t singleton_batches_ = 0;
#ifdef HASHKV_PROFILE
  struct ProfileCounters {
    std::atomic<std::uint64_t> read_calls {0};
    std::atomic<std::uint64_t> write_calls {0};
    std::atomic<std::uint64_t> read_ns {0};
    std::atomic<std::uint64_t> write_ns {0};
    std::atomic<std::uint64_t> find_ns {0};
    std::atomic<std::uint64_t> scan_ns {0};
    std::atomic<std::uint64_t> scan_cache_hits {0};
    std::atomic<std::uint64_t> scan_cache_misses {0};
    std::atomic<std::uint64_t> lock_wait_ns {0};
  } profile_;
#endif
};

std::unique_ptr<Store> Store::Open(const Options &options, std::string *error) {
  std::unique_ptr<Impl> impl(new Impl(options));
  if (!impl->Open(error)) {
    return nullptr;
  }
  return std::unique_ptr<Store>(new Store(std::move(impl)));
}

Store::Store(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
Store::~Store() = default;

Status Store::Get(const std::string &key, std::string *value,
                  std::string *error) {
  return impl_->Get(key, value, error);
}

Status Store::Put(const std::string &key, const std::string &value,
                  std::string *error) {
  return impl_->Put(key, value, error);
}

Status Store::Erase(const std::string &key, std::string *error) {
  return impl_->Erase(key, error);
}

Status Store::Scan(
    const std::string &start_key, std::size_t limit,
    std::vector<std::pair<std::string, std::string>> *records,
    std::string *error) {
  return impl_->Scan(start_key, limit, records, error);
}

bool Store::Flush(std::string *error) { return impl_->Flush(error); }
bool Store::Checkpoint(std::string *error) {
  return impl_->Checkpoint(error);
}
Stats Store::GetStats() { return impl_->GetStats(); }
const std::string &Store::path() const { return impl_->path(); }

}  // namespace hashkv
