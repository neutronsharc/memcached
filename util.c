#include <stdio.h>
#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <stdarg.h>

#include "memcached.h"

/* Avoid warnings on solaris, where isspace() is an index into an array, and gcc uses signed chars */
#define xisspace(c) isspace((unsigned char)c)

bool safe_strtoull(const char *str, uint64_t *out) {
    assert(out != NULL);
    errno = 0;
    *out = 0;
    char *endptr;
    unsigned long long ull = strtoull(str, &endptr, 10);
    if ((errno == ERANGE) || (str == endptr)) {
        return false;
    }

    if (xisspace(*endptr) || (*endptr == '\0' && endptr != str)) {
        if ((long long) ull < 0) {
            /* only check for negative signs in the uncommon case when
             * the unsigned number is so big that it's negative as a
             * signed number. */
            if (strchr(str, '-') != NULL) {
                return false;
            }
        }
        *out = ull;
        return true;
    }
    return false;
}

bool safe_strtoll(const char *str, int64_t *out) {
    assert(out != NULL);
    errno = 0;
    *out = 0;
    char *endptr;
    long long ll = strtoll(str, &endptr, 10);
    if ((errno == ERANGE) || (str == endptr)) {
        return false;
    }

    if (xisspace(*endptr) || (*endptr == '\0' && endptr != str)) {
        *out = ll;
        return true;
    }
    return false;
}

bool safe_strtoul(const char *str, uint32_t *out) {
    char *endptr = NULL;
    unsigned long l = 0;
    assert(out);
    assert(str);
    *out = 0;
    errno = 0;

    l = strtoul(str, &endptr, 10);
    if ((errno == ERANGE) || (str == endptr)) {
        return false;
    }

    if (xisspace(*endptr) || (*endptr == '\0' && endptr != str)) {
        if ((long) l < 0) {
            /* only check for negative signs in the uncommon case when
             * the unsigned number is so big that it's negative as a
             * signed number. */
            if (strchr(str, '-') != NULL) {
                return false;
            }
        }
        *out = l;
        return true;
    }

    return false;
}

bool safe_strtol(const char *str, int32_t *out) {
    assert(out != NULL);
    errno = 0;
    *out = 0;
    char *endptr;
    long l = strtol(str, &endptr, 10);
    if ((errno == ERANGE) || (str == endptr)) {
        return false;
    }

    if (xisspace(*endptr) || (*endptr == '\0' && endptr != str)) {
        *out = l;
        return true;
    }
    return false;
}

void vperror(const char *fmt, ...) {
    int old_errno = errno;
    char buf[1024];
    va_list ap;

    va_start(ap, fmt);
    if (vsnprintf(buf, sizeof(buf), fmt, ap) == -1) {
        buf[sizeof(buf) - 1] = '\0';
    }
    va_end(ap);

    errno = old_errno;

    perror(buf);
}

// Split an input string into sub-strings according to "delimiters".
// The sub-strings are put in substrs[] array.
//
// Return:  number of sub-strings found in input. -1 if more sub-strings
//    than the substrs[] array size.
int SplitString(char *input,
                const char *delimiters,
                char* substrs[],
                int substrs_size) {
  int num_substrs = 0;
  char *pch;

  pch = strtok(input, delimiters);
  while (pch) {
    substrs[num_substrs] = pch;
    num_substrs++;
    if (num_substrs >= substrs_size) {
      printf("sub str number %d > substrs size %d\n", num_substrs, substrs_size);
      return -1;
    }
    pch = strtok(NULL, delimiters);
  }
  for (int i = 0; i < num_substrs; i++) {
    printf("\t%s\n", substrs[i]);
  }
  return num_substrs;
}

// Convert a string in form of 123K/M/G to its decimal value.
size_t KMGToValue(char *strValue) {
  char *rptr = NULL;
  size_t val = strtoul(strValue, &rptr, 10);
  if (*rptr == 0) {
    // the input string is like "1345".
  } else if (*(rptr + 1) == 0) {
    // the input string is like "1345K".
    switch (*rptr) {
      case 'k':
      case 'K':
        val *= 1024L;
        break;
      case 'm':
      case 'M':
        val *= (1024L * 1024);
        break;
      case 'g':
      case 'G':
        val *= (1024L * 1024 * 1024);
        break;
      default:
        fprintf(stderr, "invalid input format: %s\n", strValue);
        return 0;
    }
  } else {
    fprintf(stderr, "invalid input format: %s\n", strValue);
    return 0;
  }

  return val;
}


#ifndef HAVE_HTONLL
static uint64_t mc_swap64(uint64_t in) {
#ifdef ENDIAN_LITTLE
    /* Little endian, flip the bytes around until someone makes a faster/better
    * way to do this. */
    int64_t rv = 0;
    int i = 0;
     for(i = 0; i<8; i++) {
        rv = (rv << 8) | (in & 0xff);
        in >>= 8;
     }
    return rv;
#else
    /* big-endian machines don't need byte swapping */
    return in;
#endif
}

uint64_t ntohll(uint64_t val) {
   return mc_swap64(val);
}

uint64_t htonll(uint64_t val) {
   return mc_swap64(val);
}
#endif

