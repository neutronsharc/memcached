#include <assert.h>
#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <pthread.h>
#include <sched.h>
#include <unistd.h>
#include <string.h>

#include "debug.h"
#include "kvclient.h"

void* OpenKVStore(char* paths[],
                  int number_paths,
                  size_t storage_sizes[],
                  size_t memory_for_index,
                  size_t memory_for_datacache) {
  return OpenSSDCache(paths,
                      number_paths,
                      storage_sizes,
                      memory_for_index,
                      memory_for_datacache);
}

void CloseKVStore(void* handler) {
  CloseSSDCache(handler);
}

/**
 * Use KV interface to put an obj.
 * Return:
 *    0 on success, non-0 otherwise.
 */
int KVPut(void* handler, item *it) {
  dbg("will call KVStore put: %s(%d), exptime %d.\n",
      ITEM_key(it), it->nkey, it->exptime);
  //return PutItem(handler, (void*)it);
  return Put(handler,
             ITEM_key(it),
             it->nkey,
             ITEM_data(it),
             it->nbytes,
             it->exptime);
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
int KVGet(void* handler,
          KVRequest* requests,
          int num_requests,
          item** result_items) {
  int valid_items = 0;

  for (int i = 0; i < num_requests; i++) {
    void* data;
    size_t data_size;
    KVRequest *p = requests + i;
    int ret = Get(handler, p->key, p->keylen, &data, &data_size);

    if (ret != HCD_OK) {
      continue;
    }

    int flags = 0;
    // KVstore has examed the exptime. We don't care about exp-time here.
    item* it = item_alloc((char*)p->key, p->keylen, flags, 0, (int)(data_size));
    if (it) {
      memcpy(ITEM_data(it), data, data_size);
      dbg("fetched an item: key %s(%d), data-size %ld, item-total-size = %ld\n",
          (char*)p->key,
          p->keylen,
          data_size,
          ITEM_ntotal(it));
      ReleaseMemory(handler, data);
      result_items[valid_items] = it;
      valid_items++;
    } else {
      err("failed to alloc memory for item %s(%d), data size %ld\n",
          p->key,
          p->keylen,
          data_size);
      ReleaseMemory(handler, data);
    }
  }

  return valid_items;
}

/**
 * Delete object.
 * Return:
 *    0 if the obj exists and is deleted.
 *    1 if the obj not exists before delete.
 */
int KVDelete(void* handler, char* key, int keylen) {
  return Delete(handler, key, keylen);
}

size_t GetDataSize(void* handler) {
  KVRequest rqst;
  memset(&rqst, 0, sizeof(rqst));
  rqst.type = GET_DATA_SIZE;

  KVRunCommand(handler, &rqst, 1);
  return rqst.vlen;
}

size_t GetNumberOfRecords(void* handler) {
  KVRequest rqst;
  memset(&rqst, 0, sizeof(rqst));
  rqst.type = GET_NUMBER_RECORDS;

  KVRunCommand(handler, &rqst, 1);
  return rqst.vlen;
}

size_t GetMemoryUsage(void* handler) {
  KVRequest rqst;
  memset(&rqst, 0, sizeof(rqst));
  rqst.type = GET_MEMORY_USAGE;

  KVRunCommand(handler, &rqst, 1);
  return rqst.vlen;
}
