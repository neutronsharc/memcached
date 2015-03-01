#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

#include "debug.h"
#include "kvclient.h"


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
  memset(&request, 0, sizeof(request));
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

  ////////////////
#if 0
  for (int i = 0; i < numRequests; i++) {
    KVRequest *p = requests + i;
    if (p->retcode != SUCCESS) {
      dbg("get key %s failed, retcode %d\n", p->key, p->retcode);
      continue;
    }
    ReleaseMemory(p->value);
  }
  return 0;
#endif

  for (int i = 0; i < numRequests; i++) {
    KVRequest *p = requests + i;
    if (p->retcode != SUCCESS) {
      dbg("get key %s failed, retcode %d\n", p->key, p->retcode);
      continue;
    }
    int flags = 0;
    // TODO: parse exptime from data.
    int exptime = 1000;
    it = item_alloc((char*)p->key, p->keylen, flags, exptime, (int)(p->vlen));
    if (it) {
      memcpy(ITEM_data(it), p->value, p->vlen);
      dbg("fetched an item, nkey %d, vlen %ld, size = %ld\n", p->keylen, p->vlen, ITEM_ntotal(it));
      //dump_item(it);
      //free(p->value);
      ReleaseMemory(p->value);
      resultItems[validItems] = it;
      validItems++;
    } else {
      err("failed to alloc memory for item %s, size %ld\n", p->key, p->vlen);
      //free(p->value);
      ReleaseMemory(p->value);
    }
  }

  //////////
#if 0
  for (int i = 0; i < validItems; i++) {
    it = resultItems[i];
    item_remove(it);
  }
  validItems = 0;
#endif
  //////////////////////

  return validItems;
}

/**
 * Delete object.
 * Return:
 *    the object's size if it exists.  < 0 if the obj does not exist.
 */
int KVDelete(void* dbHandler, char* key, int keylen) {
  KVRequest getrqst;
  memset(&getrqst, 0, sizeof(getrqst));
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
  memset(&request, 0, sizeof(request));
  request.type = DELETE;
  request.key = key;
  request.keylen = keylen;

  KVRunCommand(dbHandler, &request, 1);
  dbg("delete key %s ret %d\n", key, request.retcode);
  int vlen = (int)its[0]->nbytes;
  item_remove(its[0]);
  return request.retcode == SUCCESS ? vlen : -1;
}

size_t GetDataSize(void* dbHandler) {
  KVRequest rqst;
  memset(&rqst, 0, sizeof(rqst));
  rqst.type = GET_DATA_SIZE;

  KVRunCommand(dbHandler, &rqst, 1);
  return rqst.vlen;
}

size_t GetNumberOfRecords(void* dbHandler) {
  KVRequest rqst;
  memset(&rqst, 0, sizeof(rqst));
  rqst.type = GET_NUMBER_RECORDS;

  KVRunCommand(dbHandler, &rqst, 1);
  return rqst.vlen;
}

size_t GetMemoryUsage(void* dbHandler) {
  KVRequest rqst;
  memset(&rqst, 0, sizeof(rqst));
  rqst.type = GET_MEMORY_USAGE;

  KVRunCommand(dbHandler, &rqst, 1);
  return rqst.vlen;
}
