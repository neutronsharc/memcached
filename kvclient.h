#ifndef __KVCLIENT_H__
#define __KVCLIENT_H__

#include "memcached.h"
#include "hcderrno.h"
#include "kvinterface.h"

// Below are routines to access KV store.
//void* OpenKVStore(char* dbpath, int numIOThreqds, int cacheMB);

void* OpenKVStore(char* db_paths[],
                  int number_paths,
                  size_t storage_sizes[],
                  size_t memory_for_index,
                  size_t memory_for_datacache);

void CloseKVStore(void* handler);

int KVPut(void* handler, item *it);

int KVGet(void* handler,
          KVRequest* requests,
          int num_requests,
          item** result_items);

int KVDelete(void* handler, char* key, int keylen);

size_t GetNumberOfRecords(void* handler);

size_t GetDataSize(void* handler);

size_t GetMemoryUsage(void* handler);

#endif  // __KVCLIENT_H__
