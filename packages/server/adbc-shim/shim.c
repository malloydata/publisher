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
// and before each DatabaseInit it sets the client application name every
// session reports, which the extension has no way to set either:
//
//   adbc.snowflake.sql.client_option.app_name <- PUBLISHER_SNOWFLAKE_APPLICATION_NAME
//
// An unset or empty variable sets nothing for that option, so with none of
// them set the shim is a pure pass-through. Why this exists, the measurements,
// and the condition for deleting it are in README.md next to this file.
//
// Lifetime: the extension opens one ADBC database per pooled Snowflake client,
// and the driver manager runs AdbcDriverInit at each AdbcDatabaseInit — so the
// statics below are re-initialised and the "wrapping" line is logged once PER
// DRIVER LOAD, i.e. once per client the extension creates, not once per
// process. Repeats of that line are expected, not a fault.
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "adbc.h"

#define REAL_DRIVER_BASENAME "libadbc_driver_snowflake.real.so"
#define LOG_PREFIX "[adbc-shim] "

// Process-global, rewritten on EVERY AdbcDriverInit (once per pooled client) with
// the same values, since one process loads one real driver: inits serialise on
// the loader lock, so the unguarded writes are benign. The assumption that would
// break this is two different real drivers in one process — then the last writer
// wins for both. Not a case the extension can produce today.
static AdbcStatusCode (*real_StatementNew)(struct AdbcConnection*, struct AdbcStatement*,
                                           struct AdbcError*);
static AdbcStatusCode (*real_StatementSetOption)(struct AdbcStatement*, const char*, const char*,
                                                 struct AdbcError*);
static AdbcStatusCode (*real_DatabaseInit)(struct AdbcDatabase*, struct AdbcError*);
static AdbcStatusCode (*real_DatabaseSetOption)(struct AdbcDatabase*, const char*, const char*,
                                                struct AdbcError*);
static int debug_statements;

#define APPLICATION_ENV "PUBLISHER_SNOWFLAKE_APPLICATION_NAME"
#define APPLICATION_OPTION "adbc.snowflake.sql.client_option.app_name"

// The driver rejects anything that is not a positive integer, but it does so per
// statement and only in a log line; validating here keeps the startup line honest
// (an operator typo such as 0 must not print as if the bound were on).
static int is_positive_integer(const char* s) {
  if (!s || !*s) return 0;
  for (const char* p = s; *p; ++p)
    if (*p < '0' || *p > '9') return 0;
  // strtol saturates to LONG_MAX on overflow rather than failing, so a 30-digit
  // value would pass a plain "> 0" check, be forwarded verbatim, and then be
  // rejected by the driver per statement — exactly the mismatch this function
  // exists to prevent. The driver stores the value in a C int, so cap there.
  errno = 0;
  char* end = NULL;
  long v = strtol(s, &end, 10);
  if (errno == ERANGE || !end || *end != '\0') return 0;
  return v > 0 && v <= INT_MAX;
}

// snowflake-sdk's rule for a client application name, which the server checks
// at boot (config.ts, getSnowflakeApplicationName): a letter, then 1 to 50 letters,
// digits, '.', '-' or '_'. The Go driver accepts anything; checking here keeps
// both paths reporting the same names, and a name the server refused cannot
// reach this process anyway, since that refusal fails startup.
static int is_application_name(const char* s) {
  if (!s) return 0;
  if (!((*s >= 'A' && *s <= 'Z') || (*s >= 'a' && *s <= 'z'))) return 0;
  size_t rest = 0;
  for (const char* p = s + 1; *p; ++p, ++rest) {
    char c = *p;
    int ok = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
             c == '.' || c == '-' || c == '_';
    if (!ok) return 0;
  }
  return rest >= 1 && rest <= 50;
}

// Exported for selftest.c, which cannot see the statics above.
int AdbcShimIsPositiveInteger(const char* s) { return is_positive_integer(s); }
int AdbcShimIsApplicationName(const char* s) { return is_application_name(s); }
void* AdbcShimCapturedStatementNew(void) { return (void*)real_StatementNew; }
void* AdbcShimCapturedDatabaseInit(void) { return (void*)real_DatabaseInit; }

// The value to apply for an env var, or NULL when unset/empty/invalid.
static const char* option_value(const char* env) {
  const char* v = getenv(env);
  if (!v || !*v) return NULL;
  return is_positive_integer(v) ? v : NULL;
}

// How the startup line describes an env var.
static const char* describe(const char* env, char* buf, size_t len) {
  const char* v = getenv(env);
  if (!v || !*v) return "(unset)";
  if (is_positive_integer(v)) return v;
  snprintf(buf, len, "(invalid \"%s\", ignored)", v);
  return buf;
}

// The application name to apply, or NULL when unset/empty/invalid.
static const char* application_value(void) {
  const char* v = getenv(APPLICATION_ENV);
  if (!v || !*v) return NULL;
  return is_application_name(v) ? v : NULL;
}

// How the startup line describes the application name.
static const char* describe_application(char* buf, size_t len) {
  const char* v = getenv(APPLICATION_ENV);
  if (!v || !*v) return "(unset)";
  if (is_application_name(v)) return v;
  snprintf(buf, len, "(invalid \"%.60s\", ignored)", v);
  return buf;
}

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
  const char* value = option_value(env);
  if (!value) return;
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

// The driver manager replays the extension's database options through
// DatabaseSetOption and then calls DatabaseInit, which is where the Go driver
// builds its session config; an option set here is the last one set. The
// extension sets no app_name, so nothing overrides this one. The driver puts
// its own "[ADBC][Go-<version>]" in front of the name.
static AdbcStatusCode shim_DatabaseInit(struct AdbcDatabase* db, struct AdbcError* err) {
  const char* application = application_value();
  if (application) {
    // Rejected is logged, not fatal: the session still opens, reporting the
    // driver's default name, which is the behaviour without the shim.
    struct AdbcError opt_err;
    memset(&opt_err, 0, sizeof(opt_err));
    AdbcStatusCode status = real_DatabaseSetOption(db, APPLICATION_OPTION, application, &opt_err);
    if (status != ADBC_STATUS_OK) {
      fprintf(stderr, LOG_PREFIX "%s=%s rejected by driver (status %d)%s%s\n", APPLICATION_OPTION,
              application, (int)status, opt_err.message ? ": " : "",
              opt_err.message ? opt_err.message : "");
      if (opt_err.release) opt_err.release(&opt_err);
    } else if (debug_statements) {
      fprintf(stderr, LOG_PREFIX "%s=%s applied\n", APPLICATION_OPTION, application);
    }
  }
  return real_DatabaseInit(db, err);
}

AdbcStatusCode AdbcDriverInit(int version, void* raw_driver, struct AdbcError* error) {
  char path[PATH_MAX];
  if (real_driver_path(path, sizeof(path)) != 0) return ADBC_STATUS_INTERNAL;
  // RTLD_LOCAL (the default): the shim reaches the driver only through this
  // handle, so its exported Go-runtime symbols stay out of the namespace DuckDB,
  // Node and every other extension share. The handle is deliberately never
  // dlclose'd — a Go c-shared library cannot be unloaded cleanly — so the driver
  // stays mapped for the life of the process even when the manager releases the
  // shim. That pins memory the upstream layout would also have pinned once loaded.
  void* handle = dlopen(path, RTLD_NOW);
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
  // Self-wrap guard. The real driver overwrites every slot today, but a manager
  // that reuses one AdbcDriver across inits with a driver that preserves populated
  // slots would hand us our own wrappers here — capturing one as "real" makes it
  // call itself forever. Both slots are wrapped together, so either one already
  // wrapped means nothing to do.
  if (driver->StatementNew == shim_StatementNew || driver->DatabaseInit == shim_DatabaseInit)
    return ADBC_STATUS_OK;
  real_StatementNew = driver->StatementNew;
  real_StatementSetOption = driver->StatementSetOption;
  if (real_StatementNew && real_StatementSetOption) {
    driver->StatementNew = shim_StatementNew;
  } else {
    fprintf(stderr, LOG_PREFIX "driver table lacks StatementNew/StatementSetOption; statements pass through\n");
  }
  real_DatabaseInit = driver->DatabaseInit;
  real_DatabaseSetOption = driver->DatabaseSetOption;
  if (real_DatabaseInit && real_DatabaseSetOption) {
    driver->DatabaseInit = shim_DatabaseInit;
  } else {
    fprintf(stderr, LOG_PREFIX "driver table lacks DatabaseInit/DatabaseSetOption; databases pass through\n");
  }

  const char* dbg = getenv("ADBC_SHIM_DEBUG");
  debug_statements = dbg && *dbg;
  const char* q = getenv("ADBC_RESULT_QUEUE_SIZE");
  const char* p = getenv("ADBC_PREFETCH_CONCURRENCY");
  const char* a = getenv(APPLICATION_ENV);
  // Quiet when the features are off: with every variable unset there is nothing to
  // report, and this runs once per pooled client. Anything set — valid or not —
  // is worth a line, because that line is the operator's acceptance check.
  if ((q && *q) || (p && *p) || (a && *a) || debug_statements) {
    char qbuf[128], pbuf[128], abuf[128];
    fprintf(stderr,
            LOG_PREFIX "wrapping %s (adbc %d); result_queue_size=%s prefetch_concurrency=%s "
                       "application=%s\n",
            path, version, describe("ADBC_RESULT_QUEUE_SIZE", qbuf, sizeof(qbuf)),
            describe("ADBC_PREFETCH_CONCURRENCY", pbuf, sizeof(pbuf)),
            describe_application(abuf, sizeof(abuf)));
  }
  return ADBC_STATUS_OK;
}

// The ADBC driver manager may derive the entry point from the library name.
AdbcStatusCode AdbcDriverSnowflakeInit(int version, void* raw_driver, struct AdbcError* error) {
  return AdbcDriverInit(version, raw_driver, error);
}
