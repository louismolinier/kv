#include "hashkv/kv_store.h"

#include <array>
#include <cstdint>
#include <functional>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "core/db.h"
#include "core/db_factory.h"
#include "utils/utils.h"

namespace {

void Append32(std::uint32_t value, std::string *out) {
  for (int shift = 0; shift < 32; shift += 8) {
    out->push_back(static_cast<char>((value >> shift) & 0xffU));
  }
}

bool Read32(const std::string &input, std::size_t *cursor,
            std::uint32_t *value) {
  if (*cursor > input.size() || input.size() - *cursor < 4) {
    return false;
  }
  *value = 0;
  for (int shift = 0; shift < 32; shift += 8) {
    *value |= static_cast<std::uint32_t>(
                  static_cast<unsigned char>(input[*cursor + shift / 8]))
              << shift;
  }
  *cursor += 4;
  return true;
}

}  // namespace

namespace ycsbc {

class HashKVDB : public DB {
 public:
  ~HashKVDB() override { Cleanup(); }

  void Init() override {
    std::lock_guard<std::mutex> lock(open_mutex_);
    if (store_) {
      return;
    }

    const std::string path =
        props_->GetProperty("hashkv.path", "/tmp/hashkv-first-draft.data");
    print_stats_ = utils::StrToBool(
        props_->GetProperty("hashkv.stats", "true"));

    store_ = shared_store_.lock();
    if (store_) {
      if (store_->path() != path) {
        throw utils::Exception("HashKV threads must use the same path");
      }
      return;
    }

    hashkv::Options options;
    options.path = path;
    options.truncate = utils::StrToBool(
        props_->GetProperty("hashkv.destroy", "false"));
    try {
      options.bucket_count = static_cast<std::size_t>(std::stoull(
          props_->GetProperty("hashkv.buckets", "65536")));
      options.scan_cache_slots = static_cast<std::size_t>(std::stoull(
          props_->GetProperty("hashkv.scan_cache_slots", "0")));
    } catch (const std::exception &) {
      throw utils::Exception(
          "hashkv.buckets and hashkv.scan_cache_slots must be integers");
    }

    std::string error;
    std::unique_ptr<hashkv::Store> opened =
        hashkv::Store::Open(options, &error);
    if (!opened) {
      throw utils::Exception("HashKV open: " + error);
    }
    store_ = std::shared_ptr<hashkv::Store>(std::move(opened));
    shared_store_ = store_;
  }

  void Cleanup() override {
    std::lock_guard<std::mutex> lock(open_mutex_);
    if (!store_) {
      return;
    }
    if (print_stats_ && store_.use_count() == 1) {
      const hashkv::Stats stats = store_->GetStats();
      std::cout << "HashKV records: " << stats.records << '\n'
                << "HashKV file bytes: " << stats.file_bytes << '\n'
                << "HashKV WAL bytes: " << stats.wal_bytes << '\n'
                << "HashKV free regions: " << stats.free_regions << '\n'
                << "HashKV free bytes: " << stats.free_bytes << '\n'
                << "HashKV reused regions: " << stats.reused_regions
                << std::endl;
#ifdef HASHKV_PROFILE
      std::cout << "HashKV profile read calls: " << stats.read_calls << '\n'
                << "HashKV profile write calls: " << stats.write_calls << '\n'
                << "HashKV profile read ms: " << stats.read_ns / 1e6 << '\n'
                << "HashKV profile write ms: " << stats.write_ns / 1e6 << '\n'
                << "HashKV profile find ms: " << stats.find_ns / 1e6 << '\n'
                << "HashKV profile scan ms: " << stats.scan_ns / 1e6 << '\n'
                << "HashKV profile scan cache hits: "
                << stats.scan_cache_hits << '\n'
                << "HashKV profile scan cache misses: "
                << stats.scan_cache_misses << '\n'
                << "HashKV profile lock wait ms: " << stats.lock_wait_ns / 1e6
                << '\n'
                << "HashKV profile WAL sync calls: " << stats.wal_sync_calls
                << '\n'
                << "HashKV profile WAL sync ms: " << stats.wal_sync_ns / 1e6
                << std::endl;
#endif
    }
    store_.reset();
  }

  Status Read(const std::string &table, const std::string &key,
              const std::vector<std::string> *fields,
              std::vector<Field> &result) override {
    std::string row;
    std::string error;
    hashkv::Status status = store_->Get(Key(table, key), &row, &error);
    if (status != hashkv::Status::kOk) {
      return Translate(status, error);
    }
    std::vector<Field> decoded;
    DecodeOrThrow(row, &decoded);
    Select(decoded, fields, &result);
    return kOK;
  }

  Status Scan(const std::string &table, const std::string &key,
              int record_count, const std::vector<std::string> *fields,
              std::vector<std::vector<Field>> &result) override {
    if (record_count <= 0) {
      return kOK;
    }
    const std::string prefix = Key(table, "");
    std::vector<std::pair<std::string, std::string>> rows;
    std::string error;
    hashkv::Status status = store_->Scan(
        Key(table, key), static_cast<std::size_t>(record_count), &rows, &error);
    if (status != hashkv::Status::kOk) {
      return Translate(status, error);
    }
    for (const auto &row : rows) {
      if (row.first.compare(0, prefix.size(), prefix) != 0) {
        break;
      }
      std::vector<Field> decoded;
      DecodeOrThrow(row.second, &decoded);
      result.emplace_back();
      Select(decoded, fields, &result.back());
    }
    return kOK;
  }

  Status Update(const std::string &table, const std::string &key,
                std::vector<Field> &values) override {
    const std::string storage_key = Key(table, key);
    std::lock_guard<std::mutex> lock(UpdateMutex(storage_key));
    std::string row;
    std::string error;
    hashkv::Status status = store_->Get(storage_key, &row, &error);
    if (status != hashkv::Status::kOk) {
      return Translate(status, error);
    }

    std::vector<Field> current;
    DecodeOrThrow(row, &current);
    for (const Field &replacement : values) {
      bool replaced = false;
      for (Field &field : current) {
        if (field.name == replacement.name) {
          field.value = replacement.value;
          replaced = true;
          break;
        }
      }
      if (!replaced) {
        current.push_back(replacement);
      }
    }
    row = Encode(current);
    return Translate(store_->Put(storage_key, row, &error), error);
  }

  Status Insert(const std::string &table, const std::string &key,
                std::vector<Field> &values) override {
    const std::string storage_key = Key(table, key);
    std::lock_guard<std::mutex> lock(UpdateMutex(storage_key));
    std::string error;
    return Translate(store_->Put(storage_key, Encode(values), &error),
                     error);
  }

  Status Delete(const std::string &table, const std::string &key) override {
    const std::string storage_key = Key(table, key);
    std::lock_guard<std::mutex> lock(UpdateMutex(storage_key));
    std::string error;
    return Translate(store_->Erase(storage_key, &error), error);
  }

 private:
  static std::mutex &UpdateMutex(const std::string &key) {
    return update_mutexes_[std::hash<std::string> {}(key) %
                           update_mutexes_.size()];
  }

  static std::string Key(const std::string &table, const std::string &key) {
    return table + '\0' + key;
  }

  static std::string Encode(const std::vector<Field> &fields) {
    if (fields.size() > std::numeric_limits<std::uint32_t>::max()) {
      throw utils::Exception("too many YCSB fields");
    }
    std::string row;
    Append32(static_cast<std::uint32_t>(fields.size()), &row);
    for (const Field &field : fields) {
      if (field.name.size() > std::numeric_limits<std::uint32_t>::max() ||
          field.value.size() > std::numeric_limits<std::uint32_t>::max()) {
        throw utils::Exception("YCSB field is too large");
      }
      Append32(static_cast<std::uint32_t>(field.name.size()), &row);
      Append32(static_cast<std::uint32_t>(field.value.size()), &row);
      row += field.name;
      row += field.value;
    }
    return row;
  }

  static bool Decode(const std::string &row, std::vector<Field> *fields) {
    fields->clear();
    std::size_t cursor = 0;
    std::uint32_t count = 0;
    if (!Read32(row, &cursor, &count) || count > (row.size() - cursor) / 8) {
      return false;
    }
    for (std::uint32_t i = 0; i < count; ++i) {
      std::uint32_t name_size = 0;
      std::uint32_t value_size = 0;
      if (!Read32(row, &cursor, &name_size) ||
          !Read32(row, &cursor, &value_size) ||
          static_cast<std::uint64_t>(name_size) + value_size >
              row.size() - cursor) {
        return false;
      }
      Field field;
      field.name.assign(row.data() + cursor, name_size);
      cursor += name_size;
      field.value.assign(row.data() + cursor, value_size);
      cursor += value_size;
      fields->push_back(std::move(field));
    }
    return cursor == row.size();
  }

  static void DecodeOrThrow(const std::string &row,
                            std::vector<Field> *fields) {
    if (!Decode(row, fields)) {
      throw utils::Exception("malformed HashKV YCSB row");
    }
  }

  static void Select(const std::vector<Field> &all,
                     const std::vector<std::string> *wanted,
                     std::vector<Field> *result) {
    if (wanted == nullptr) {
      result->insert(result->end(), all.begin(), all.end());
      return;
    }
    for (const std::string &name : *wanted) {
      for (const Field &field : all) {
        if (field.name == name) {
          result->push_back(field);
          break;
        }
      }
    }
  }

  static Status Translate(hashkv::Status status, const std::string &error) {
    if (status == hashkv::Status::kOk) {
      return kOK;
    }
    if (status == hashkv::Status::kNotFound) {
      return kNotFound;
    }
    throw utils::Exception(std::string("HashKV ") +
                           hashkv::StatusName(status) + ": " + error);
  }

  static std::mutex open_mutex_;
  static std::array<std::mutex, 256> update_mutexes_;
  static std::weak_ptr<hashkv::Store> shared_store_;

  std::shared_ptr<hashkv::Store> store_;
  bool print_stats_ = true;
};

std::mutex HashKVDB::open_mutex_;
std::array<std::mutex, 256> HashKVDB::update_mutexes_;
std::weak_ptr<hashkv::Store> HashKVDB::shared_store_;

DB *NewHashKVDB() { return new HashKVDB; }
const bool registered = DBFactory::RegisterDB("hashkv", NewHashKVDB);

}  // namespace ycsbc
