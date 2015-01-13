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
  // Disable compression, since user data is already compressed.
  rocksdb_options_set_compression(db->options, 0);

  long cpus = sysconf(_SC_NPROCESSORS_ONLN);
  rocksdb_options_increase_parallelism(db->options, (int)(cpus));
  dbg("Use %ld cpu cores\n", cpus);
  rocksdb_options_optimize_level_style_compaction(db->options, 0);

  // Prepare write options.
  db->write_options = rocksdb_writeoptions_create();
  // Disable WAL log since we are doing caching, and crash in middle of write
  // isn't a problem.
  rocksdb_writeoptions_disable_WAL(db->write_options, 1);

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


/**
 *  Write given object to RocksDB.
 *  Return: 0 on success, none-0 otherwise.
 */
int Put(RocksDB *db, char* key, int keylen, char* value, int vlen) {
  char *err = NULL;
  rocksdb_put(db->db,
              db->write_options,
              key,
              keylen,
              value,
              vlen,
              &err);
  if (err) {
    err("Put() object %s failed: %s\n", key, err);
    return -1;
  }
  return 0;
}

/* Read given object from RocksDB. */
/* Return: malloced memory containing the data. */
char* Get(RocksDB *db, char *key, int keylen, size_t *vlen) {
  char *err = NULL;
  char *value = rocksdb_get(db->db,
                            db->read_options,
                            key,
                            keylen,
                            vlen,
                            &err);
  if (err) {
    err("Get() obj %s failed: %s\n", key, err);
    return NULL;
  }
  return value;
}

/**
 *  Delete object with given name from from RocksDB.
 *  Return: object size in bytes if the obj is found and deleted.
 *          -1 otherwise.
 */
int Delete(RocksDB *db, char *key, int keylen) {
  size_t vlen = 0;
  char* value = Get(db, key, keylen, &vlen);
  if (!value) {
    dbg("object %s not exist, returning right away...\n", key);
    return -1;
  }
  free(value);

  char *err = NULL;
  rocksdb_delete(db->db,
                 db->write_options,
                 key,
                 keylen,
                 &err);
  if (err) {
    err("Delete() object %s failed: %s\n", key, err);
    return -1;
  }
  return (int)vlen;
}

