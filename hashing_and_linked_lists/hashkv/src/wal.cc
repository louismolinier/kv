#include "wal.h"

#include <algorithm>
#include <array>
#ifdef HASHKV_PROFILE
#include <chrono>
#endif
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <limits>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

namespace hashkv {
namespace {

constexpr char kWalMagic[8] = {'H', 'K', 'W', 'A', 'L', '0', '0', '1'};
constexpr std::uint32_t kFrameMagic = 0x484b5746U;
constexpr std::uint64_t kHashBasis = 14695981039346656037ULL;
constexpr std::uint64_t kHashPrime = 1099511628211ULL;

struct WalHeader {
  char magic[8];
  std::uint64_t bucket_count;
  std::uint64_t reserved;
};

struct FrameHeader {
  std::uint32_t magic;
  std::uint32_t operation;
  std::uint32_t key_size;
  std::uint32_t value_size;
  std::uint64_t sequence;
  std::uint64_t checksum;
};

static_assert(sizeof(WalHeader) == 24, "unexpected WAL header size");
static_assert(sizeof(FrameHeader) == 32, "unexpected WAL frame size");

void SetError(std::string *error, const std::string &message) {
  if (error != nullptr) {
    *error = message;
  }
}

std::string SystemError(const std::string &operation) {
  return operation + ": " + std::strerror(errno);
}

std::uint64_t HashBytes(std::uint64_t hash, const void *data,
                        std::size_t size) {
  const auto *bytes = static_cast<const unsigned char *>(data);
  for (std::size_t i = 0; i < size; ++i) {
    hash ^= bytes[i];
    hash *= kHashPrime;
  }
  return hash;
}

bool ReadAt(int fd, std::uint64_t offset, void *data, std::size_t size,
            std::string *error) {
  auto *cursor = static_cast<char *>(data);
  while (size != 0) {
    const ssize_t count = ::pread(fd, cursor, size, static_cast<off_t>(offset));
    if (count < 0 && errno == EINTR) {
      continue;
    }
    if (count <= 0) {
      SetError(error, count == 0 ? "unexpected end of WAL"
                                  : SystemError("WAL pread"));
      return false;
    }
    cursor += count;
    offset += static_cast<std::uint64_t>(count);
    size -= static_cast<std::size_t>(count);
  }
  return true;
}

// Validation and replay both walk the WAL sequentially. Buffering those walks
// avoids one pread per header, key and value without changing the disk format.
class BufferedReader {
 public:
  BufferedReader(int fd, std::uint64_t begin, std::uint64_t end)
      : fd_(fd), offset_(begin), end_(end) {}

  std::uint64_t offset() const { return offset_; }

  bool Read(void *data, std::size_t size, std::string *error) {
    if (size > end_ - offset_) {
      SetError(error, "unexpected end of WAL");
      return false;
    }
    auto *output = static_cast<char *>(data);
    while (size != 0) {
      if (position_ == available_ && !Fill(error)) {
        return false;
      }
      const std::size_t chunk = std::min(size, available_ - position_);
      std::memcpy(output, buffer_.data() + position_, chunk);
      output += chunk;
      size -= chunk;
      position_ += chunk;
      offset_ += chunk;
    }
    return true;
  }

  bool Hash(std::uint64_t *hash, std::uint64_t size,
            std::string *error) {
    if (size > end_ - offset_) {
      SetError(error, "unexpected end of WAL");
      return false;
    }
    while (size != 0) {
      if (position_ == available_ && !Fill(error)) {
        return false;
      }
      const std::size_t chunk = static_cast<std::size_t>(
          std::min<std::uint64_t>(size, available_ - position_));
      *hash = HashBytes(*hash, buffer_.data() + position_, chunk);
      size -= chunk;
      position_ += chunk;
      offset_ += chunk;
    }
    return true;
  }

 private:
  bool Fill(std::string *error) {
    position_ = 0;
    available_ = static_cast<std::size_t>(
        std::min<std::uint64_t>(buffer_.size(), end_ - offset_));
    if (available_ == 0) {
      SetError(error, "unexpected end of WAL");
      return false;
    }
    return ReadAt(fd_, offset_, buffer_.data(), available_, error);
  }

  int fd_;
  std::uint64_t offset_;
  std::uint64_t end_;
  std::array<char, 64 * 1024> buffer_ {};
  std::size_t position_ = 0;
  std::size_t available_ = 0;
};

bool WriteAt(int fd, std::uint64_t offset, const void *data,
             std::size_t size, std::string *error) {
  const auto *cursor = static_cast<const char *>(data);
  while (size != 0) {
    const ssize_t count = ::pwrite(fd, cursor, size,
                                   static_cast<off_t>(offset));
    if (count < 0 && errno == EINTR) {
      continue;
    }
    if (count <= 0) {
      SetError(error, SystemError("WAL pwrite"));
      return false;
    }
    cursor += count;
    offset += static_cast<std::uint64_t>(count);
    size -= static_cast<std::size_t>(count);
  }
  return true;
}

bool SyncDirectory(const std::string &path, std::string *error) {
  const std::size_t slash = path.find_last_of('/');
  const std::string directory =
      slash == std::string::npos ? "." : path.substr(0, slash + 1);
  const int fd = ::open(directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (fd < 0) {
    SetError(error, SystemError("open WAL directory"));
    return false;
  }
  const int result = ::fsync(fd);
  if (result != 0) {
    SetError(error, SystemError("fsync WAL directory"));
  }
  ::close(fd);
  return result == 0;
}

bool ValidOperation(const FrameHeader &header) {
  return header.operation == static_cast<std::uint32_t>(WalOperation::kPut) ||
         (header.operation == static_cast<std::uint32_t>(WalOperation::kErase) &&
          header.value_size == 0);
}

}  // namespace

Wal::Wal(int fd, std::string path, std::uint64_t bytes, bool temporary)
    : fd_(fd), path_(std::move(path)), bytes_(bytes), temporary_(temporary) {}

Wal::~Wal() {
  ::close(fd_);
  if (temporary_) {
    ::unlink(path_.c_str());
  }
}

std::unique_ptr<Wal> Wal::OpenExisting(const std::string &path,
                                        std::uint64_t bucket_count,
                                        std::string *error) {
  const int fd = ::open(path.c_str(), O_RDWR | O_CLOEXEC);
  if (fd < 0) {
    SetError(error, SystemError("open WAL"));
    return nullptr;
  }
  struct stat info {};
  WalHeader header {};
  if (::fstat(fd, &info) != 0) {
    SetError(error, SystemError("fstat WAL"));
    ::close(fd);
    return nullptr;
  }
  if (info.st_size < static_cast<off_t>(sizeof(header)) ||
      !ReadAt(fd, 0, &header, sizeof(header), error)) {
    if (info.st_size < static_cast<off_t>(sizeof(header))) {
      SetError(error, "incomplete WAL header");
    }
    ::close(fd);
    return nullptr;
  }
  if (std::memcmp(header.magic, kWalMagic, sizeof(kWalMagic)) != 0 ||
      header.bucket_count != bucket_count) {
    SetError(error, "invalid WAL header or bucket_count mismatch");
    ::close(fd);
    return nullptr;
  }
  return std::unique_ptr<Wal>(new Wal(fd, path,
                                      static_cast<std::uint64_t>(info.st_size),
                                      false));
}

std::unique_ptr<Wal> Wal::CreateAtomic(const std::string &path,
                                        std::uint64_t bucket_count,
                                        const Snapshot &snapshot,
                                        std::string *error) {
  std::string pattern = path + ".tmp.XXXXXX";
  std::vector<char> buffer(pattern.begin(), pattern.end());
  buffer.push_back('\0');
  const int fd = ::mkstemp(buffer.data());
  if (fd < 0) {
    SetError(error, SystemError("create WAL snapshot"));
    return nullptr;
  }
  ::fcntl(fd, F_SETFD, FD_CLOEXEC);
  std::unique_ptr<Wal> wal(new Wal(fd, buffer.data(), sizeof(WalHeader), true));
  WalHeader header {};
  std::memcpy(header.magic, kWalMagic, sizeof(kWalMagic));
  header.bucket_count = bucket_count;
  if (!WriteAt(fd, 0, &header, sizeof(header), error) ||
      !snapshot(wal.get(), error) || !wal->Sync(error)) {
    return nullptr;
  }
  if (::rename(wal->path_.c_str(), path.c_str()) != 0) {
    SetError(error, SystemError("publish WAL snapshot"));
    return nullptr;
  }
  wal->path_ = path;
  wal->temporary_ = false;
  if (!SyncDirectory(path, error)) {
    return nullptr;
  }
  return wal;
}

bool Wal::EncodeFrame(WalOperation operation, const std::string &key,
                      const std::string &value, std::uint64_t sequence,
                      std::string *buffer, std::string *error) {
  if (key.size() > std::numeric_limits<std::uint32_t>::max() ||
      value.size() > std::numeric_limits<std::uint32_t>::max()) {
    SetError(error, "WAL key or value is too large");
    return false;
  }
  if (sequence == 0) {
    SetError(error, "WAL sequence is exhausted");
    return false;
  }
  FrameHeader header {kFrameMagic,
                      static_cast<std::uint32_t>(operation),
                      static_cast<std::uint32_t>(key.size()),
                      static_cast<std::uint32_t>(value.size()),
                      sequence,
                      0};
  std::uint64_t checksum = HashBytes(kHashBasis, &header,
                                      sizeof(header) - sizeof(header.checksum));
  checksum = HashBytes(checksum, key.data(), key.size());
  header.checksum = HashBytes(checksum, value.data(), value.size());
  buffer->append(reinterpret_cast<const char *>(&header), sizeof(header));
  buffer->append(key);
  buffer->append(value);
  return true;
}

bool Wal::WriteFrame(WalOperation operation, const std::string &key,
                     const std::string &value, std::string *error) {
  const std::uint64_t frame_size = sizeof(FrameHeader) + key.size() +
                                   value.size();
  if (frame_size > static_cast<std::uint64_t>(
                       std::numeric_limits<off_t>::max()) - bytes_) {
    SetError(error, "WAL is too large");
    return false;
  }
  std::string frame;
  frame.reserve(static_cast<std::size_t>(frame_size));
  if (!EncodeFrame(operation, key, value, sequence_ + 1, &frame, error) ||
      !WriteAt(fd_, bytes_, frame.data(), frame.size(), error)) {
    return false;
  }
  bytes_ += frame_size;
  ++sequence_;
  return true;
}

bool Wal::AppendSnapshot(WalOperation operation, const std::string &key,
                         const std::string &value, std::string *error) {
  if (!temporary_) {
    SetError(error, "snapshot append is only valid before WAL publication");
    return false;
  }
  return WriteFrame(operation, key, value, error);
}

bool Wal::Append(WalOperation operation, const std::string &key,
                 const std::string &value, std::string *error) {
  return AppendBatch({{operation, &key, &value}}, error);
}

bool Wal::AppendBatch(const std::vector<WalEntry> &entries,
                      std::string *error) {
  if (entries.empty()) {
    return true;
  }
  if (poisoned_) {
    SetError(error, "WAL is poisoned by a prior I/O error");
    return false;
  }
  if (entries.size() > std::numeric_limits<std::uint64_t>::max() -
                           sequence_) {
    SetError(error, "WAL sequence is exhausted");
    return false;
  }
  std::uint64_t batch_bytes = 0;
  for (const WalEntry &entry : entries) {
    if (entry.key->size() > std::numeric_limits<std::uint32_t>::max() ||
        entry.value->size() > std::numeric_limits<std::uint32_t>::max()) {
      SetError(error, "WAL key or value is too large");
      return false;
    }
    const std::uint64_t frame_size = sizeof(FrameHeader) +
                                     entry.key->size() + entry.value->size();
    if (frame_size > static_cast<std::uint64_t>(
                         std::numeric_limits<off_t>::max()) - bytes_ -
                         batch_bytes) {
      SetError(error, "WAL is too large");
      return false;
    }
    batch_bytes += frame_size;
  }
  std::string frames;
  frames.reserve(static_cast<std::size_t>(batch_bytes));
  for (std::size_t i = 0; i < entries.size(); ++i) {
    const WalEntry &entry = entries[i];
    if (!EncodeFrame(entry.operation, *entry.key, *entry.value,
                     sequence_ + i + 1, &frames, error)) {
      return false;
    }
  }
  const std::uint64_t previous_bytes = bytes_;
  const std::uint64_t previous_sequence = sequence_;
  if (WriteAt(fd_, bytes_, frames.data(), frames.size(), error)) {
    bytes_ += batch_bytes;
    sequence_ += entries.size();
  }
  if (bytes_ != previous_bytes && Sync(error)) {
#ifdef HASHKV_TEST_CRASH
    if (::getenv("HASHKV_TEST_CRASH_AFTER_SYNC") != nullptr) {
      ::_exit(86);
    }
#endif
    return true;
  }
  bytes_ = previous_bytes;
  sequence_ = previous_sequence;
  if (::ftruncate(fd_, static_cast<off_t>(bytes_)) != 0 ||
      ::fsync(fd_) != 0) {
    poisoned_ = true;
  }
  return false;
}

bool Wal::Sync(std::string *error) {
#ifdef HASHKV_PROFILE
  const auto start = std::chrono::steady_clock::now();
#endif
  const int result = ::fsync(fd_);
#ifdef HASHKV_PROFILE
  ++sync_calls_;
  sync_ns_ += std::chrono::duration_cast<std::chrono::nanoseconds>(
                  std::chrono::steady_clock::now() - start)
                  .count();
#endif
  if (result != 0) {
    SetError(error, SystemError("fsync WAL"));
    return false;
  }
  return true;
}

bool Wal::ValidateAndTrim(std::string *error) {
  std::uint64_t offset = sizeof(WalHeader);
  std::uint64_t sequence = 0;
  BufferedReader reader(fd_, offset, bytes_);
  while (offset < bytes_) {
    if (bytes_ - offset < sizeof(FrameHeader)) {
      break;  // En-tête final interrompu.
    }
    FrameHeader header {};
    if (!reader.Read(&header, sizeof(header), error)) {
      return false;
    }
    if (header.magic != kFrameMagic || !ValidOperation(header) ||
        header.sequence != sequence + 1) {
      SetError(error, "invalid WAL frame at offset " +
                          std::to_string(offset));
      return false;
    }
    const std::uint64_t payload =
        static_cast<std::uint64_t>(header.key_size) + header.value_size;
    if (payload > bytes_ - offset - sizeof(header)) {
      break;  // Payload final interrompu.
    }
    std::uint64_t checksum = HashBytes(kHashBasis, &header,
                                      sizeof(header) - sizeof(header.checksum));
    if (!reader.Hash(&checksum, payload, error)) {
      return false;
    }
    const std::uint64_t cursor = reader.offset();
    if (checksum != header.checksum) {
      if (cursor != bytes_) {
        SetError(error, "WAL checksum mismatch before the final frame");
        return false;
      }
      break;  // Une dernière frame complète, mais déchirée.
    }
    offset = cursor;
    ++sequence;
  }
  if (offset != bytes_) {
    if (::ftruncate(fd_, static_cast<off_t>(offset)) != 0) {
      SetError(error, SystemError("truncate incomplete WAL tail"));
      return false;
    }
    if (!Sync(error)) {
      return false;
    }
    bytes_ = offset;
  }
  sequence_ = sequence;
  return true;
}

bool Wal::ReplayEntries(const Replay &replay, std::string *error) {
  BufferedReader reader(fd_, sizeof(WalHeader), bytes_);
  std::string key;
  std::string value;
  while (reader.offset() < bytes_) {
    FrameHeader header {};
    if (!reader.Read(&header, sizeof(header), error)) {
      return false;
    }
    const std::uint64_t payload =
        static_cast<std::uint64_t>(header.key_size) + header.value_size;
    if (payload > bytes_ - reader.offset()) {
      SetError(error, "unexpected end of WAL");
      return false;
    }
    key.resize(header.key_size);
    value.resize(header.value_size);
    if (!reader.Read(key.data(), key.size(), error)) {
      return false;
    }
    if (!reader.Read(value.data(), value.size(), error)) {
      return false;
    }
    if (!replay(static_cast<WalOperation>(header.operation), key, value,
                error)) {
      return false;
    }
  }
  return true;
}

}  // namespace hashkv
