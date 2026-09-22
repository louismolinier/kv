#include "hashkv/kv_store.h"

#include <array>
#include <atomic>
#ifdef HASHKV_PROFILE
#include <chrono>
#endif
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <limits>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <sys/stat.h>
#include <unistd.h>

namespace hashkv {
namespace {

constexpr char kFileMagic[8] = {'H', 'A', 'S', 'H', 'K', 'V', '0', '1'};
constexpr std::uint32_t kRecordMagic = 0x48564b31U;
constexpr std::uint32_t kActive = 1;
constexpr std::uint32_t kFree = 2;

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

    fd_ = ::open(options_.path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
    if (fd_ < 0) {
      SetError(error, SystemError("open"));
      return false;
    }
    if (options_.truncate && ::ftruncate(fd_, 0) != 0) {
      SetError(error, SystemError("truncate"));
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

    try {
      buckets_.assign(options_.bucket_count, 0);
    } catch (const std::bad_alloc &) {
      SetError(error, "not enough memory for the bucket array");
      return false;
    }
    return RebuildIndexes(error);
  }

  Status Get(const std::string &key, std::string *value, std::string *error) {
    if (value == nullptr) {
      return Status::kInvalidArgument;
    }
#ifdef HASHKV_PROFILE
    const auto before_lock = Clock::now();
#endif
    std::shared_lock<std::shared_mutex> lock(mutex_);
#ifdef HASHKV_PROFILE
    profile_.lock_wait_ns.fetch_add(ElapsedNs(before_lock),
                                    std::memory_order_relaxed);
#endif
    Found found;
    Status status = Find(key, &found, error);
    if (status != Status::kOk) {
      return status;
    }
    return ReadValue(found.offset, found.header, value, error)
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

#ifdef HASHKV_PROFILE
    const auto before_lock = Clock::now();
#endif
    std::unique_lock<std::shared_mutex> lock(mutex_);
#ifdef HASHKV_PROFILE
    profile_.lock_wait_ns.fetch_add(ElapsedNs(before_lock),
                                    std::memory_order_relaxed);
#endif
    Found found;
    Status status = Find(key, &found, error);
    if (status != Status::kOk && status != Status::kNotFound) {
      return status;
    }

    const std::uint64_t needed = sizeof(RecordHeader) + key.size() + value.size();
    if (status == Status::kOk && needed <= found.header.slot_size) {
      found.header.value_size = static_cast<std::uint32_t>(value.size());
      return WriteRecord(found.offset, found.header, key, value, error)
                 ? Status::kOk
                 : Status::kIOError;
    }

    std::uint64_t slot_size = needed;
    const std::uint64_t new_offset = Allocate(needed, &slot_size);
    RecordHeader new_header {kRecordMagic,
                             kActive,
                             status == Status::kOk ? found.header.next
                                                   : buckets_[Bucket(key)],
                             slot_size,
                             static_cast<std::uint32_t>(key.size()),
                             static_cast<std::uint32_t>(value.size())};
    if (!WriteRecord(new_offset, new_header, key, value, error)) {
      return Status::kIOError;
    }

    if (status == Status::kOk) {
      if (found.previous == 0) {
        buckets_[Bucket(key)] = new_offset;
      } else if (!SetNext(found.previous, new_offset, error)) {
        return Status::kIOError;
      }
      if (!MarkFree(found.offset, found.header.slot_size, error)) {
        return Status::kIOError;
      }
    } else {
      buckets_[Bucket(key)] = new_offset;
      ++record_count_;
    }
    if (ordered_ready_) {
      ordered_[key] = new_offset;
    }
    return Status::kOk;
  }

  Status Erase(const std::string &key, std::string *error) {
#ifdef HASHKV_PROFILE
    const auto before_lock = Clock::now();
#endif
    std::unique_lock<std::shared_mutex> lock(mutex_);
#ifdef HASHKV_PROFILE
    profile_.lock_wait_ns.fetch_add(ElapsedNs(before_lock),
                                    std::memory_order_relaxed);
#endif
    Found found;
    Status status = Find(key, &found, error);
    if (status != Status::kOk) {
      return status;
    }

    const std::size_t bucket = Bucket(key);
    if (found.previous == 0) {
      buckets_[bucket] = found.header.next;
    } else if (!SetNext(found.previous, found.header.next, error)) {
      return Status::kIOError;
    }
    if (!MarkFree(found.offset, found.header.slot_size, error)) {
      return Status::kIOError;
    }
    --record_count_;
    if (ordered_ready_) {
      ordered_.erase(key);
    }
    return Status::kOk;
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
      RecordHeader header {};
      if (!ReadActiveHeader(it->second, &header, error)) {
        return Status::kCorrupt;
      }
      std::string value;
      if (!ReadValue(it->second, header, &value, error)) {
        return Status::kIOError;
      }
      records->emplace_back(it->first, std::move(value));
    }
    return Status::kOk;
  }

  bool Flush(std::string *error) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    if (::fsync(fd_) != 0) {
      SetError(error, SystemError("fsync"));
      return false;
    }
    return true;
  }

  Stats GetStats() {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    Stats stats;
    stats.records = record_count_;
    stats.file_bytes = file_size_;
    stats.free_regions = free_regions_.size();
    stats.reused_regions = reused_regions_;
#ifdef HASHKV_PROFILE
    stats.read_calls = profile_.read_calls.load(std::memory_order_relaxed);
    stats.write_calls = profile_.write_calls.load(std::memory_order_relaxed);
    stats.read_ns = profile_.read_ns.load(std::memory_order_relaxed);
    stats.write_ns = profile_.write_ns.load(std::memory_order_relaxed);
    stats.find_ns = profile_.find_ns.load(std::memory_order_relaxed);
    stats.scan_ns = profile_.scan_ns.load(std::memory_order_relaxed);
    stats.lock_wait_ns = profile_.lock_wait_ns.load(std::memory_order_relaxed);
#endif
    for (const auto &region : free_regions_) {
      stats.free_bytes += region.first;
    }
    return stats;
  }

  const std::string &path() const { return options_.path; }

 private:
  struct Found {
    std::uint64_t offset = 0;
    std::uint64_t previous = 0;
    RecordHeader header {};
  };

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
        ordered_[std::move(key)] = offset;
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
        free_regions_.emplace(header.slot_size, offset);
      } else {
        SetError(error, "unknown record state");
        return false;
      }
      offset += header.slot_size;
    }
    return offset == file_size_;
  }

  Status Find(const std::string &key, Found *found, std::string *error) {
#ifdef HASHKV_PROFILE
    Timer find_timer(&profile_.find_ns);
#endif
    std::uint64_t previous = 0;
    std::uint64_t offset = buckets_[Bucket(key)];
    std::array<char, 256> first_bytes {};
    while (offset != 0) {
      RecordHeader header {};
      bool key_matches = false;
      const bool short_key = key.size() <= first_bytes.size() - sizeof(header);
      const std::size_t read_size = short_key ? sizeof(header) + key.size() : 0;
      if (short_key && offset <= file_size_ &&
          read_size <= file_size_ - offset) {
        // Les clés YCSB tiennent ici : un seul pread pour l'en-tête et la clé.
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

  std::uint64_t Allocate(std::uint64_t needed, std::uint64_t *slot_size) {
    const auto best = free_regions_.lower_bound(needed);
    if (best != free_regions_.end()) {
      const std::uint64_t offset = best->second;
      *slot_size = best->first;
      free_regions_.erase(best);
      ++reused_regions_;
      return offset;
    }
    const std::uint64_t offset = file_size_;
    file_size_ += needed;
    return offset;
  }

  bool MarkFree(std::uint64_t offset, std::uint64_t size,
                std::string *error) {
    RecordHeader header {kRecordMagic, kFree, 0, size, 0, 0};
    if (!WriteAt(offset, &header, sizeof(header), error)) {
      return false;
    }
    free_regions_.emplace(size, offset);
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
  std::uint64_t file_size_ = 0;
  std::uint64_t record_count_ = 0;
  std::uint64_t reused_regions_ = 0;
  std::vector<std::uint64_t> buckets_;
  std::multimap<std::uint64_t, std::uint64_t> free_regions_;
  std::map<std::string, std::uint64_t> ordered_;
  bool ordered_ready_ = false;
  std::shared_mutex mutex_;
#ifdef HASHKV_PROFILE
  struct ProfileCounters {
    std::atomic<std::uint64_t> read_calls {0};
    std::atomic<std::uint64_t> write_calls {0};
    std::atomic<std::uint64_t> read_ns {0};
    std::atomic<std::uint64_t> write_ns {0};
    std::atomic<std::uint64_t> find_ns {0};
    std::atomic<std::uint64_t> scan_ns {0};
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
Stats Store::GetStats() { return impl_->GetStats(); }
const std::string &Store::path() const { return impl_->path(); }

}  // namespace hashkv
