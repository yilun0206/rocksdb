// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cstdio>
#include <string>
#include <vector>
#include <thread>
#include <fcntl.h>
#include <time.h>
#include <unistd.h>
#include <sys/mman.h>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

#define NUM_KEYS  (50 << 20)
#define NR_PUTS_THREADS	16
#define NR_DBS	(1 << 3)
#define DB_IDX_MASK (NR_DBS - 1)

using namespace rocksdb;

struct KeyStruct {
  uint32_t u32[8];
};

std::string kDBPath = "/mnt/cache_test/";
DB *dbs[NR_DBS];

void init_dbs() {
  Status s;
  int nr_threads_per_db = 16 / NR_DBS;

  if (!nr_threads_per_db) {
    nr_threads_per_db = 1;
  }

  Options options;
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options.IncreaseParallelism(nr_threads_per_db);
  options.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options.create_if_missing = true;
  // control the total amount of write buffer to 32GB
  uint64_t buffer_size = (32UL << 30) / NR_DBS;
  options.db_write_buffer_size = buffer_size;
  printf("db_write_buffer_size: %lu\n", options.db_write_buffer_size);

  for (int i = 0; i < NR_DBS; i++) {
    std::string db_path = kDBPath + std::to_string(i);
    s = DB::Open(options, db_path, &dbs[i]);
    assert(s.ok());
  }
}

void delete_dbs() {
  for (int i = 0; i < NR_DBS; i++) {
    delete(dbs[i]);
  }
}

struct KeyStruct *init_key_array() {
  void *mmap_addr = mmap(NULL, NUM_KEYS * sizeof(struct KeyStruct),
                         PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);

  if (mmap_addr == MAP_FAILED) {
     printf("Fail to allocate the array for keys.\n");
     exit(-1);
  }
  return static_cast<struct KeyStruct *>(mmap_addr);
}

inline DB* find_db(struct KeyStruct *key) {
  int idx = (key->u32[0]) & DB_IDX_MASK;
  // printf("idx = %d\n", idx);
  return dbs[idx];
}

uint64_t count_usecs(struct timespec &before, struct timespec &after) {
  uint64_t us;
  us = 1000000UL * (after.tv_sec - before.tv_sec) + (after.tv_nsec - before.tv_nsec)/1000;
  return us;
}

void put_keys(int random_fd, WriteOptions opts, struct KeyStruct *keys, int nr_keys, int tid) {
  char value_buf[4096];
  Status s;
  DB *db;

  for (int i = 0; i < nr_keys; i++) {
    read(random_fd, (char *)&keys[i], sizeof(struct KeyStruct));
    read(random_fd, value_buf, 4096);
    Slice key = Slice(reinterpret_cast<char *>(&keys[i]), sizeof(struct KeyStruct));
    Slice value = Slice(value_buf, 4096);
    db = find_db(&keys[i]);

    s = db->Put(opts, key, value);
    assert(s.ok());
    if (i % 10000 == 0) {
      printf("Tid: %d has put %d keys.\n", tid, i);
    }
  }
}

int main() {
  DB* db;
  int random_fd;
  int chunk_size = NUM_KEYS/NR_PUTS_THREADS;
  Status s;

  printf("chunk_size = %d keys.\n", chunk_size);
  // Init DBs
  init_dbs();
  struct KeyStruct *keys = init_key_array();

  random_fd = open("/dev/urandom", O_RDONLY);
  assert(random_fd > 0);

  WriteOptions write_options = WriteOptions();
  write_options.disableWAL = true;

  struct timespec before_put, after_put, before_get, after_get;

  // Put key-values
  printf("Starting putting %d of <K, V> pairs\n", NUM_KEYS);
  std::vector<std::thread> threads;
  clock_gettime(CLOCK_MONOTONIC, &before_put);
  for (int i = 0; i < NR_PUTS_THREADS; i++) {
    threads.emplace_back(&put_keys, random_fd, write_options, &keys[i * chunk_size], chunk_size, i);
  }

  for (auto &thx : threads) {
    thx.join();
  }
  clock_gettime(CLOCK_MONOTONIC, &after_put);
  printf("Finishing putting <K, V> in %lu usecs.\n", count_usecs(before_put, after_put));

  std::vector<Slice> test_keys;
  std::vector<uint32_t> test_key_idxs;

  for (int i = 0; i < 10000; i++) {
    uint32_t idx;
    std::string value;

    read(random_fd, (char *)&idx, 4);
    idx = idx % NUM_KEYS;
    test_key_idxs.push_back(idx);
    test_keys.push_back(Slice(reinterpret_cast<char *>(&keys[idx]), sizeof(struct KeyStruct)));
  } 
  std::string value;

  printf("Starting getting <K, V> pairs.\n");
  clock_gettime(CLOCK_MONOTONIC, &before_get);
  for (int i = 0; i < test_keys.size(); i++) {
    db = find_db(&keys[test_key_idxs[i]]); 
    s = db->Get(ReadOptions(), test_keys[i], &value);
    assert(s.ok());
  } 
  clock_gettime(CLOCK_MONOTONIC, &after_get);
  printf("Getting %lu <K, V> in %lu usecs.\n", test_keys.size(), count_usecs(before_get, after_get));

  return 0;
}
