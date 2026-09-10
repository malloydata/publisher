// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// ADBC driver shim for the DuckDB `snowflake` extension.
//
// Installed AS `libadbc_driver_snowflake.so` beside the extension, with the
// real driver renamed to `libadbc_driver_snowflake.real.so` in the same
// directory. Every ADBC entry point is forwarded to the real driver unchanged,
// except that after each successful StatementNew the shim sets the two
// statement options the extension has no way to set:
//
//   adbc.rpc.result_queue_size               <- ADBC_RESULT_QUEUE_SIZE
//   adbc.snowflake.rpc.prefetch_concurrency  <- ADBC_PREFETCH_CONCURRENCY
//
// An unset or empty variable sets nothing for that option, so with neither
// set the shim is a pure pass-through. Why this exists, the measurements, and
// the condition for deleting it are in README.md next to this file.
#define _GNU_SOURCE
#include <dlfcn.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "adbc.h"

#define REAL_DRIVER_BASENAME "libadbc_driver_snowflake.real.so"
#define LOG_PREFIX "[adbc-shim] "

static AdbcStatusCode (*real_StatementNew)(struct AdbcConnection*, struct AdbcStatement*,
                                           struct AdbcError*);
static AdbcStatusCode (*real_StatementSetOption)(struct AdbcStatement*, const char*, const char*,
                                                 struct AdbcError*);
static int debug_statements;

// Resolve the real driver: ADBC_REAL_DRIVER if set, else REAL_DRIVER_BASENAME
// in the directory this shim was loaded from.
static int real_driver_path(char* out, size_t out_len) {
  const char* override = getenv("ADBC_REAL_DRIVER");
  if (override && *override) {
    snprintf(out, out_len, "%s", override);
    return 0;
  }
  Dl_info info;
  if (!dladdr((void*)&real_driver_path, &info) || !info.dli_fname) {
    fprintf(stderr, LOG_PREFIX "cannot locate own path via dladdr\n");
    return -1;
  }
  const char* slash = strrchr(info.dli_fname, '/');
  size_t dir_len = slash ? (size_t)(slash - info.dli_fname + 1) : 0;
  if (dir_len + strlen(REAL_DRIVER_BASENAME) + 1 > out_len) return -1;
  memcpy(out, info.dli_fname, dir_len);
  strcpy(out + dir_len, REAL_DRIVER_BASENAME);
  return 0;
}

static void set_option(struct AdbcStatement* stmt, const char* key, const char* env,
                       struct AdbcError* err) {
  const char* value = getenv(env);
  if (!value || !*value) return;
  AdbcStatusCode status = real_StatementSetOption(stmt, key, value, err);
  if (status != ADBC_STATUS_OK) {
    fprintf(stderr, LOG_PREFIX "%s=%s rejected by driver (status %d)%s%s\n", key, value,
            (int)status, (err && err->message) ? ": " : "", (err && err->message) ? err->message : "");
    if (err && err->release) err->release(err);
  } else if (debug_statements) {
    fprintf(stderr, LOG_PREFIX "%s=%s applied\n", key, value);
  }
}

static AdbcStatusCode shim_StatementNew(struct AdbcConnection* conn, struct AdbcStatement* stmt,
                                        struct AdbcError* err) {
  AdbcStatusCode status = real_StatementNew(conn, stmt, err);
  if (status != ADBC_STATUS_OK) return status;
  // A rejected option is logged, not fatal: the statement still works, it is
  // just unbounded, which is the behaviour without the shim.
  struct AdbcError opt_err;
  memset(&opt_err, 0, sizeof(opt_err));
  set_option(stmt, "adbc.rpc.result_queue_size", "ADBC_RESULT_QUEUE_SIZE", &opt_err);
  memset(&opt_err, 0, sizeof(opt_err));
  set_option(stmt, "adbc.snowflake.rpc.prefetch_concurrency", "ADBC_PREFETCH_CONCURRENCY",
             &opt_err);
  return ADBC_STATUS_OK;
}

AdbcStatusCode AdbcDriverInit(int version, void* raw_driver, struct AdbcError* error) {
  char path[PATH_MAX];
  if (real_driver_path(path, sizeof(path)) != 0) return ADBC_STATUS_INTERNAL;
  void* handle = dlopen(path, RTLD_NOW | RTLD_GLOBAL);
  if (!handle) {
    fprintf(stderr, LOG_PREFIX "dlopen(%s) failed: %s\n", path, dlerror());
    return ADBC_STATUS_INTERNAL;
  }
  AdbcDriverInitFunc real_init = (AdbcDriverInitFunc)dlsym(handle, "AdbcDriverInit");
  if (!real_init) {
    fprintf(stderr, LOG_PREFIX "%s exports no AdbcDriverInit\n", path);
    return ADBC_STATUS_INTERNAL;
  }
  AdbcStatusCode status = real_init(version, raw_driver, error);
  if (status != ADBC_STATUS_OK) return status;

  struct AdbcDriver* driver = (struct AdbcDriver*)raw_driver;
  real_StatementNew = driver->StatementNew;
  real_StatementSetOption = driver->StatementSetOption;
  if (!real_StatementNew || !real_StatementSetOption) {
    fprintf(stderr, LOG_PREFIX "driver table lacks StatementNew/StatementSetOption; passing through\n");
    return ADBC_STATUS_OK;
  }
  driver->StatementNew = shim_StatementNew;

  const char* dbg = getenv("ADBC_SHIM_DEBUG");
  debug_statements = dbg && *dbg;
  const char* q = getenv("ADBC_RESULT_QUEUE_SIZE");
  const char* p = getenv("ADBC_PREFETCH_CONCURRENCY");
  fprintf(stderr, LOG_PREFIX "wrapping %s (adbc %d); result_queue_size=%s prefetch_concurrency=%s\n",
          path, version, (q && *q) ? q : "(unset)", (p && *p) ? p : "(unset)");
  return ADBC_STATUS_OK;
}

// The ADBC driver manager may derive the entry point from the library name.
AdbcStatusCode AdbcDriverSnowflakeInit(int version, void* raw_driver, struct AdbcError* error) {
  return AdbcDriverInit(version, raw_driver, error);
}
