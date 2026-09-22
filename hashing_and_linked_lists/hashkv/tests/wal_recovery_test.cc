#include "hashkv/kv_store.h"
#include "../src/wal.h"

#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include <fcntl.h>

namespace {

void Check(bool condition, const std::string &message) {
  if (!condition) {
    std::cerr << "FAIL: " << message << std::endl;
    std::exit(1);
  }
}

void Put(hashkv::Store *store, const std::string &key,
         const std::string &value) {
  std::string error;
  Check(store->Put(key, value, &error) == hashkv::Status::kOk,
        "put " + key + ": " + error);
}

void CheckContents(hashkv::Store *store) {
  std::string error;
  std::string value;
  Check(store->Get("a", &value, &error) == hashkv::Status::kOk &&
            value == "updated", "recovered a");
  Check(store->Get("b", &value, &error) == hashkv::Status::kNotFound,
        "recovered deletion");
  Check(store->Get("c", &value, &error) == hashkv::Status::kOk &&
            value == std::string(2048, 'C'), "recovered c");
  Check(store->GetStats().records == 2, "recovered record count");
}

void CheckBufferedWalBoundaries() {
  char path[] = "/tmp/hashkv-wal-boundaries-XXXXXX";
  const int initial_fd = ::mkstemp(path);
  Check(initial_fd >= 0, "mkstemp buffered WAL");
  ::close(initial_fd);
  Check(::unlink(path) == 0, "remove buffered WAL placeholder");

  std::vector<std::pair<std::string, std::string>> entries;
  for (int i = 0; i < 120; ++i) {
    entries.emplace_back("key-" + std::to_string(i),
                         std::string(1000 + i % 17, static_cast<char>(i)));
  }
  entries.emplace_back("large", std::string(70000, 'L'));
  entries.emplace_back("empty", "");

  std::string error;
  auto wal = hashkv::Wal::CreateAtomic(
      path, 8,
      [&entries](hashkv::Wal *snapshot, std::string *snapshot_error) {
        for (const auto &entry : entries) {
          if (!snapshot->AppendSnapshot(hashkv::WalOperation::kPut,
                                        entry.first, entry.second,
                                        snapshot_error)) {
            return false;
          }
        }
        return true;
      },
      &error);
  Check(wal != nullptr, "create buffered WAL: " + error);
  const std::uint64_t valid_bytes = wal->bytes();
  wal.reset();

  const int tail_fd = ::open(path, O_WRONLY | O_APPEND);
  Check(tail_fd >= 0, "open buffered WAL tail");
  const char torn[] = "tail";
  Check(::write(tail_fd, torn, sizeof(torn)) == sizeof(torn),
        "append buffered WAL tail");
  ::close(tail_fd);

  wal = hashkv::Wal::OpenExisting(path, 8, &error);
  Check(wal != nullptr && wal->ValidateAndTrim(&error),
        "validate buffered WAL: " + error);
  Check(wal->bytes() == valid_bytes, "trim buffered WAL tail");
  std::size_t replayed = 0;
  Check(wal->ReplayEntries(
            [&entries, &replayed](hashkv::WalOperation operation,
                                  const std::string &key,
                                  const std::string &value,
                                  std::string *) {
              Check(replayed < entries.size() &&
                        operation == hashkv::WalOperation::kPut &&
                        key == entries[replayed].first &&
                        value == entries[replayed].second,
                    "replay WAL frame across buffer boundary");
              ++replayed;
              return true;
            },
            &error),
        "replay buffered WAL: " + error);
  Check(replayed == entries.size(), "replay all buffered WAL frames");
  wal.reset();
  Check(::unlink(path) == 0, "remove buffered WAL test file");
}

}  // namespace

int main() {
  CheckBufferedWalBoundaries();
  char path[] = "/tmp/hashkv-wal-test-XXXXXX";
  const int initial_fd = ::mkstemp(path);
  Check(initial_fd >= 0, "mkstemp");
  ::close(initial_fd);
  const std::string wal_path = std::string(path) + ".wal";

  hashkv::Options options;
  options.path = path;
  options.bucket_count = 8;
  options.truncate = true;
  std::string error;
  auto store = hashkv::Store::Open(options, &error);
  Check(store != nullptr, "open: " + error);
  Put(store.get(), "a", "initial");
  Put(store.get(), "b", "deleted");
  Put(store.get(), "a", "updated");
  Check(store->Erase("b", &error) == hashkv::Status::kOk, "erase b");
  Put(store.get(), "c", std::string(2048, 'C'));
  const auto before_checkpoint = store->GetStats().wal_bytes;
  Check(store->Checkpoint(&error), "checkpoint: " + error);
  const auto wal_bytes = store->GetStats().wal_bytes;
  Check(wal_bytes > 24 && wal_bytes < before_checkpoint,
        "checkpoint retains only live records");

  // Deux écrivains ne doivent pas conserver des offsets de WAL divergents.
  auto second = hashkv::Store::Open(options, &error);
  Check(second == nullptr, "exclusive data file lock");
  store.reset();

  // Simule un record et un en-tête de fichier de données interrompus.
  const int data_fd = ::open(path, O_RDWR);
  Check(data_fd >= 0, "open data for corruption");
  Check(::ftruncate(data_fd, 5) == 0, "truncate interrupted data file");
  ::close(data_fd);
  options.truncate = false;
  store = hashkv::Store::Open(options, &error);
  Check(store != nullptr, "recover interrupted data file: " + error);
  CheckContents(store.get());
  Check(store->GetStats().wal_bytes == wal_bytes, "WAL kept after replay");
  store.reset();

  // Une frame de journal incomplète n'est jamais considérée comme validée.
  const int wal_fd = ::open(wal_path.c_str(), O_WRONLY | O_APPEND);
  Check(wal_fd >= 0, "open WAL tail");
  const char torn[] = "partial";
  Check(::write(wal_fd, torn, sizeof(torn)) == sizeof(torn),
        "append incomplete WAL header");
  ::close(wal_fd);
  store = hashkv::Store::Open(options, &error);
  Check(store != nullptr, "recover incomplete WAL tail: " + error);
  CheckContents(store.get());
  Check(store->GetStats().wal_bytes == wal_bytes,
        "incomplete WAL tail discarded");
  store.reset();

  struct PartialFrame {
    std::uint32_t magic;
    std::uint32_t operation;
    std::uint32_t key_size;
    std::uint32_t value_size;
    std::uint64_t sequence;
    std::uint64_t checksum;
  };
  static_assert(sizeof(PartialFrame) == 32, "WAL test frame size");
  const PartialFrame partial {0x484b5746U, 1, 3, 100, 3, 0};
  const int payload_fd = ::open(wal_path.c_str(), O_WRONLY | O_APPEND);
  Check(payload_fd >= 0, "open WAL partial payload");
  Check(::write(payload_fd, &partial, sizeof(partial)) == sizeof(partial) &&
            ::write(payload_fd, "key", 3) == 3,
        "append incomplete WAL payload");
  ::close(payload_fd);
  store = hashkv::Store::Open(options, &error);
  Check(store != nullptr, "recover incomplete WAL payload: " + error);
  CheckContents(store.get());
  Check(store->GetStats().wal_bytes == wal_bytes,
        "incomplete WAL payload discarded");
  store.reset();

  // Migration d'un ancien fichier valide dépourvu de journal.
  Check(::unlink(wal_path.c_str()) == 0, "remove WAL for migration test");
  store = hashkv::Store::Open(options, &error);
  Check(store != nullptr, "migrate existing data file: " + error);
  CheckContents(store.get());
  store.reset();

  // Le processus meurt exactement après fsync du WAL, avant tout pwrite du
  // record : l'opération doit être rejouée au prochain Open.
  const pid_t child = ::fork();
  Check(child >= 0, "fork crash simulation");
  if (child == 0) {
    ::setenv("HASHKV_TEST_CRASH_AFTER_SYNC", "1", 1);
    std::string child_error;
    auto child_store = hashkv::Store::Open(options, &child_error);
    if (child_store) {
      child_store->Put("crash", "logged-before-data", &child_error);
    }
    ::_exit(1);
  }
  int child_status = 0;
  Check(::waitpid(child, &child_status, 0) == child &&
            WIFEXITED(child_status) && WEXITSTATUS(child_status) == 86,
        "child stops after WAL sync");
  store = hashkv::Store::Open(options, &error);
  Check(store != nullptr, "recover synced WAL operation: " + error);
  std::string crash_value;
  Check(store->Get("crash", &crash_value, &error) == hashkv::Status::kOk &&
            crash_value == "logged-before-data",
        "replay committed operation after process crash");
  store.reset();

  // Le même arrêt après fsync doit rejouer toutes les frames d'un lot,
  // y compris deux modifications successives de la même clé.
  const pid_t batch_child = ::fork();
  Check(batch_child >= 0, "fork batch crash simulation");
  if (batch_child == 0) {
    std::string child_error;
    auto wal = hashkv::Wal::OpenExisting(wal_path, options.bucket_count,
                                        &child_error);
    if (!wal || !wal->ValidateAndTrim(&child_error)) {
      ::_exit(1);
    }
    const std::string key = "batch-crash";
    const std::string first = "first";
    const std::string second = "second";
    const std::string other_key = "batch-other";
    const std::string other_value = "persisted";
    const std::vector<hashkv::WalEntry> entries {
        {hashkv::WalOperation::kPut, &key, &first},
        {hashkv::WalOperation::kPut, &key, &second},
        {hashkv::WalOperation::kPut, &other_key, &other_value}};
    ::setenv("HASHKV_TEST_CRASH_AFTER_SYNC", "1", 1);
    wal->AppendBatch(entries, &child_error);
    ::_exit(1);
  }
  Check(::waitpid(batch_child, &child_status, 0) == batch_child &&
            WIFEXITED(child_status) && WEXITSTATUS(child_status) == 86,
        "child stops after batch WAL sync");
  store = hashkv::Store::Open(options, &error);
  Check(store != nullptr, "recover synced WAL batch: " + error);
  std::string batch_value;
  Check(store->Get("batch-crash", &batch_value, &error) ==
                hashkv::Status::kOk && batch_value == "second",
        "replay ordered batch update");
  Check(store->Get("batch-other", &batch_value, &error) ==
                hashkv::Status::kOk && batch_value == "persisted",
        "replay every batched operation");
  store.reset();

  // Une corruption au milieu d'un WAL déjà publié est signalée, sans
  // effacer le fichier de données avant validation.
  struct stat before {};
  Check(::stat(path, &before) == 0, "stat data before bad WAL");
  const int corrupt_fd = ::open(wal_path.c_str(), O_RDWR);
  Check(corrupt_fd >= 0, "open WAL for corruption");
  char byte = 0;
  Check(::pread(corrupt_fd, &byte, 1, 56) == 1, "read WAL payload byte");
  byte ^= 1;
  Check(::pwrite(corrupt_fd, &byte, 1, 56) == 1, "corrupt WAL payload byte");
  ::close(corrupt_fd);
  store = hashkv::Store::Open(options, &error);
  Check(store == nullptr && error.find("checksum") != std::string::npos,
        "reject middle WAL corruption");
  struct stat after {};
  Check(::stat(path, &after) == 0 && before.st_size == after.st_size,
        "bad WAL does not truncate data file");

  ::unlink(path);
  ::unlink(wal_path.c_str());
  std::cout << "wal_recovery_test: OK" << std::endl;
}
