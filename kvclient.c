#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

#include "debug.h"
#include "kvclient.h"
#include "kvinterface.h"

/* Open and prepare RocksDB */
RocksDB* InitRocksDB(char *dbpath) {
  RocksDB *db = malloc(sizeof(RocksDB));
  if (!db) {
    err("failed to open rocksdb: %s\n", dbpath);
    return NULL;
  }
  memset(db, 0, sizeof(RocksDB));
  strncpy(db->dbpath, dbpath, 1024);

  // step 1: prepare DB options.
  db->options = rocksdb_options_create();
  rocksdb_options_set_create_if_missing(db->options, 1);

  // 0 to disable compression, since user data is already compressed.
  rocksdb_options_set_compression(db->options, 0);

  // DB will have up to this many open files at a time.  Must be within rlimit
  // of ths system.
  rocksdb_options_set_max_open_files(db->options, 4000);

  // config write buffer.
  rocksdb_options_set_write_buffer_size(db->options, 1024L * 1024);
  rocksdb_options_set_max_write_buffer_number(db->options, 32);

  long cpus = sysconf(_SC_NPROCESSORS_ONLN);
  rocksdb_options_increase_parallelism(db->options, (int)(cpus));
  dbg("Use %ld cpu cores\n", cpus);

  // compaction style.
  rocksdb_options_optimize_level_style_compaction(db->options, 0);

  // step 2: prepare write options.
  db->write_options = rocksdb_writeoptions_create();

  // Disable WAL log since we are doing caching, and crash in middle of write
  // isn't a problem.
  rocksdb_writeoptions_disable_WAL(db->write_options, 1);

  // step 3: prepare read options.
  db->read_options = rocksdb_readoptions_create();

  // step 4: open DB
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


// dbpath contains multiple dir-names, separated by ",". DB will open multiple
// shards across these dirs.
void* OpenKVStore(char* dbpath, int numShards, int cacheMB) {
  return OpenDB(dbpath, numShards, cacheMB);
}

void CloseKVStore(void* handler) {
  CloseDB(handler);
}

/**
 * Use KV interface to put an obj.
 * Return:
 *    0 on success, non-0 otherwise.
 */
int KVPut(void* dbHandler, item *it) {
  KVRequest request;
  request.type = PUT;
  request.key = ITEM_key(it);
  request.keylen = it->nkey;
  request.value = ITEM_data(it);
  request.vlen = it->nbytes;

  int ret = KVRunCommand(dbHandler, &request, 1);
  if (ret == 1) {
    return 0;
  } else {
    return -1;
  }
}

/**
 * multi-get.
 * Input:
 *    requests:  a list of KV request
 *    resultItems: an array of item*. Should be at least the same size as
 *                 requests list. Items that are successfully retrieved
 *                 at stored in this array.
 * Return:
 *    Number of items successfully retrieved from kv store.
 */
int KVGet(void* dbHandler,
          KVRequest* requests,
          int numRequests,
          item** resultItems) {
  item* it;
  int validItems = 0;

  dbg("will fetch %d objs...\n", numRequests);
  KVRunCommand(dbHandler, requests, numRequests);

  for (int i = 0; i < numRequests; i++) {
    KVRequest *p = requests + i;
    if (p->retcode != SUCCESS) {
      dbg("get key %s failed, retcode %d\n", p->key, p->retcode);
      continue;
    }
    int flags = 0;
    // TODO: parse exptime from data.
    int exptime = 1000;
    it = item_alloc(p->key, p->keylen, flags, exptime, (int)(p->vlen));
    if (it) {
      memcpy(ITEM_data(it), p->value, p->vlen);
      dbg("fetched an item, nkey %d, vlen %ld, size = %ld\n", p->keylen, p->vlen, ITEM_ntotal(it));
      //dump_item(it);
      free(p->value);
      resultItems[validItems] = it;
      validItems++;
    } else {
      err("failed to alloc memory for item %s, size %ld\n", p->key, p->vlen);
      free(p->value);
    }
  }

  return validItems;
}

/**
 * Delete object.
 * Return:
 *    the object's size if it exists.  < 0 if the obj does not exist.
 */
int KVDelete(void* dbHandler, char* key, int keylen) {
  KVRequest getrqst;
  getrqst.type = GET;
  getrqst.key = key;
  getrqst.keylen = keylen;
  item *its[1];
  int numItems = KVGet(dbHandler, &getrqst, 1, its);
  if (numItems != 1) {
    dbg("unable to find key %s\n", key);
    return -1;
  }

  KVRequest request;
  request.type = DELETE;
  request.key = key;
  request.keylen = keylen;

  KVRunCommand(dbHandler, &request, 1);
  dbg("delete key %s ret %d\n", key, request.retcode);
  int vlen = (int)its[0]->nbytes;
  item_remove(its[0]);
  return request.retcode == SUCCESS ? vlen : -1;
}
