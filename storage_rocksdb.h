#ifndef __STORAGE_ROCKSDB__
#define __STORAGE_ROCKSDB__

#include "rocksdb/c.h"

typedef struct RocksDB RocksDB;

struct  RocksDB {
    rocksdb_t *db;
    rocksdb_options_t *options;
    rocksdb_writeoptions_t *write_options;
    rocksdb_readoptions_t *read_options;

    // TODO: use a macro to define lenth.
    char dbpath[1024];  // path to DB directory.

    unsigned long read_bytes;
    unsigned long write_bytes;
};

extern RocksDB* InitRocksDB(char *dbname);
extern void CloseRocksDB(RocksDB* db);
extern int Put(RocksDB *db, char* key, int keylen, char *value, int vlen);
extern char* Get(RocksDB *db, char *key, int keylen, size_t *vlen);
extern int Delete(RocksDB *db, char *key, int keylen);


#endif  // __STORAGE_ROCKSDB__
