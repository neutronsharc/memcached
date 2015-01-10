#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

#include "debug.h"
#include "storage_rocksdb.h"

/* Open and prepare RocksDB */
RocksDB* InitRocksDB(char *dbpath) {
  RocksDB *db = malloc(sizeof(RocksDB));
  if (!db) {
    err("failed to open rocksdb: %s\n", dbpath);
    return NULL;
  }
  memset(db, 0, sizeof(RocksDB));
  strncpy(db->dbpath, dbpath, 1024);

  // Prepare DB options.
  db->options = rocksdb_options_create();
  rocksdb_options_set_create_if_missing(db->options, 1);
  long cpus = sysconf(_SC_NPROCESSORS_ONLN);
  rocksdb_options_increase_parallelism(db->options, (int)(cpus));
  dbg("Use %ld cpu cores\n", cpus);
  rocksdb_options_optimize_level_style_compaction(db->options, 0);

  // Prepare write options.
  db->write_options = rocksdb_writeoptions_create();

  // Prepare read options.
  db->read_options = rocksdb_readoptions_create();

  // open DB
  char *err = NULL;
  db->db = rocksdb_open(db->options, dbpath, &err);
  assert(!err);
  dbg("successfully opened db %s\n", db->dbpath);

  return db;
}

void CloseRocksDB(RocksDB* db) {
  dbg("will close RocksDB %s\n", db->dbpath);
  rocksdb_writeoptions_destroy(db->write_options);
  rocksdb_readoptions_destroy(db->read_options);
  rocksdb_options_destroy(db->options);
  rocksdb_close(db->db);
  free(db);
}

/* Write given object to RocksDB. */
/* Return: number of bytes written. */
int Put(RocksDB *db, item *it) {
  return 0;
}

/* Read given object from RocksDB. */
/* Return: item */
item* Get(RocksDB *db, char *key, int keylen) {
  return NULL;
}

/* Delete object with given name from from RocksDB. */
/* Return: 0 on success, non-0 otherwise. */
int Delete(RocksDB *db, char *key, int keylen) {
  return 0;
}

