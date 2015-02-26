#ifndef __KVCLIENT_H__
#define __KVCLIENT_H__

#include "kvinterface.h"
#include "memcached.h"

// Below are routines to access KV store.
void* OpenKVStore(char* dbpath, int numIOThreqds, int cacheMB);

void CloseKVStore(void* handler);

int KVPut(void* dbHandler, item *it);

int KVGet(void* dbHandler,
          KVRequest* requests,
          int numRequests,
          item** resultItems);

int KVDelete(void* dbHandler, char* key, int keylen);

size_t GetNumberOfRecords(void* dbHandler);

size_t GetDataSize(void* dbHandler);

#endif  // __KVCLIENT_H__
