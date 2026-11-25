// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Build-time check for the shim, run in the Dockerfile's adbc-driver stage. No
// network, no credentials. Exit 0 on success. Four things it proves:
//   1. dlopen'd as the driver manager would, AdbcDriverInit populates the table
//      and the StatementNew and DatabaseInit slots resolve into the SHIM (exact
//      basename, so the real driver's longer name cannot satisfy it);
//   2. a second init on the same table does not re-wrap (the self-wrap guard);
//   3. the env-var validators accept exactly the positive integers a C int
//      holds, and exactly the application names snowflake-sdk accepts;
//   4. PUBLISHER_SNOWFLAKE_APPLICATION reaches the database before init: against
//      the stub driver (ADBC_REAL_DRIVER set), the app_name DatabaseInit saw for
//      a valid, an invalid and an unset value; against the real driver, the
//      app_name the driver reports back afterwards, with its "[ADBC]" prefix.
#define _GNU_SOURCE
#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "adbc.h"

static const char* basename_of(const char* p) {
  const char* s = strrchr(p, '/');
  return s ? s + 1 : p;
}

#define APP_NAME_OPTION "adbc.snowflake.sql.client_option.app_name"

// Opens one database through the (wrapped) table with PUBLISHER_SNOWFLAKE_APPLICATION
// set to `value` (NULL to unset), runs DatabaseInit, and releases it. Returns the
// DatabaseInit status. Account and user are set because the real driver's init
// may want them; nothing connects until a connection is opened, which never is.
static AdbcStatusCode open_database(struct AdbcDriver* driver, const char* value,
                                    char* reported, size_t reported_len) {
  if (value) setenv("PUBLISHER_SNOWFLAKE_APPLICATION", value, 1);
  else unsetenv("PUBLISHER_SNOWFLAKE_APPLICATION");
  struct AdbcDatabase db;
  memset(&db, 0, sizeof(db));
  struct AdbcError err;
  memset(&err, 0, sizeof(err));
  AdbcStatusCode s = driver->DatabaseNew(&db, &err);
  if (s != ADBC_STATUS_OK) return s;
  driver->DatabaseSetOption(&db, "adbc.snowflake.sql.account", "selftest", &err);
  driver->DatabaseSetOption(&db, "username", "selftest", &err);
  s = driver->DatabaseInit(&db, &err);
  if (s != ADBC_STATUS_OK) {
    fprintf(stderr, "DatabaseInit: %s\n", err.message ? err.message : "(no message)");
    if (err.release) err.release(&err);
  }
  if (reported && reported_len) {
    reported[0] = '\0';
    size_t len = reported_len;
    if (s == ADBC_STATUS_OK && driver->DatabaseGetOption &&
        driver->DatabaseGetOption(&db, APP_NAME_OPTION, reported, &len, &err) != ADBC_STATUS_OK) {
      reported[0] = '\0';
    }
  }
  if (driver->DatabaseRelease) driver->DatabaseRelease(&db, &err);
  return s;
}

int main(int argc, char** argv) {
  if (argc != 2) {
    fprintf(stderr, "usage: %s <path-to-shim.so>\n", argv[0]);
    return 2;
  }
  void* h = dlopen(argv[1], RTLD_NOW);
  if (!h) {
    fprintf(stderr, "dlopen(%s): %s\n", argv[1], dlerror());
    return 1;
  }
  AdbcDriverInitFunc init = (AdbcDriverInitFunc)dlsym(h, "AdbcDriverInit");
  int (*validate)(const char*) = (int (*)(const char*))dlsym(h, "AdbcShimIsPositiveInteger");
  void* (*captured)(void) = (void* (*)(void))dlsym(h, "AdbcShimCapturedStatementNew");
  int (*validate_app)(const char*) = (int (*)(const char*))dlsym(h, "AdbcShimIsApplicationName");
  void* (*captured_db)(void) = (void* (*)(void))dlsym(h, "AdbcShimCapturedDatabaseInit");
  if (!init || !validate || !captured || !validate_app || !captured_db) {
    fprintf(stderr, "shim exports no AdbcDriverInit / AdbcShimIsPositiveInteger / AdbcShimCapturedStatementNew"
                    " / AdbcShimIsApplicationName / AdbcShimCapturedDatabaseInit\n");
    return 1;
  }

  // 1. init wraps StatementNew, and the wrapper lives in the shim itself.
  struct AdbcDriver driver;
  memset(&driver, 0, sizeof(driver));
  struct AdbcError err;
  memset(&err, 0, sizeof(err));
  AdbcStatusCode s = init(ADBC_VERSION_1_1_0, &driver, &err);
  if (s != ADBC_STATUS_OK) {
    fprintf(stderr, "AdbcDriverInit failed: status %d %s\n", (int)s, err.message ? err.message : "");
    return 1;
  }
  if (!driver.StatementNew || !driver.StatementSetOption || !driver.DatabaseNew) {
    fprintf(stderr, "driver table incomplete after init\n");
    return 1;
  }
  Dl_info info;
  if (!dladdr((void*)driver.StatementNew, &info) || !info.dli_fname ||
      strcmp(basename_of(info.dli_fname), "libadbc_driver_snowflake.so") != 0) {
    fprintf(stderr, "StatementNew does not resolve into the shim (%s)\n", info.dli_fname ? info.dli_fname : "?");
    return 1;
  }
  if (!driver.DatabaseInit || !dladdr((void*)driver.DatabaseInit, &info) || !info.dli_fname ||
      strcmp(basename_of(info.dli_fname), "libadbc_driver_snowflake.so") != 0) {
    fprintf(stderr, "DatabaseInit does not resolve into the shim (%s)\n", info.dli_fname ? info.dli_fname : "?");
    return 1;
  }
  Dl_info real_info;
  if (!dladdr((void*)driver.DatabaseNew, &real_info) || !real_info.dli_fname ||
      strcmp(basename_of(real_info.dli_fname), "libadbc_driver_snowflake.real.so") != 0) {
    fprintf(stderr, "DatabaseNew does not resolve into the real driver (%s)\n",
            real_info.dli_fname ? real_info.dli_fname : "?");
    return 1;
  }

  // 2. a second init on the same, already-wrapped table must be a no-op — and
  //    in particular must NOT capture the wrapper as the "real" StatementNew,
  //    which is the state that recurses forever on the first statement. The
  //    real driver overwrites the slot on init today, so simulate a driver that
  //    preserves it: re-run init with the slot still holding the wrapper.
  void* wrapped = (void*)driver.StatementNew;
  void* real_before = captured();
  void* wrapped_db = (void*)driver.DatabaseInit;
  void* real_db_before = captured_db();
  if (!real_before || real_before == wrapped || !real_db_before || real_db_before == wrapped_db) {
    fprintf(stderr, "captured real StatementNew/DatabaseInit is missing or is the wrapper\n");
    return 1;
  }
  s = init(ADBC_VERSION_1_1_0, &driver, &err);
  if (s != ADBC_STATUS_OK || (void*)driver.StatementNew != wrapped || captured() != real_before ||
      (void*)driver.DatabaseInit != wrapped_db || captured_db() != real_db_before) {
    fprintf(stderr, "second init re-wrapped the table (status %d, captured %p -> %p, db %p -> %p)\n", (int)s,
            real_before, captured(), real_db_before, captured_db());
    return 1;
  }

  // 3. validator matrix. The overflow cases are the point: strtol saturates.
  const char* accept[] = {"1", "5", "100", "2147483647"};
  const char* reject[] = {"", "0", "-1", "abc", " 5", "5 ", "5x", "+5", "1e3", "01x",
                          "2147483648", "9223372036854775807", "9223372036854775808",
                          "18446744073709551616", "99999999999999999999999"};
  for (size_t i = 0; i < sizeof(accept) / sizeof(*accept); ++i)
    if (!validate(accept[i])) { fprintf(stderr, "validator rejected \"%s\"\n", accept[i]); return 1; }
  for (size_t i = 0; i < sizeof(reject) / sizeof(*reject); ++i)
    if (validate(reject[i])) { fprintf(stderr, "validator accepted \"%s\"\n", reject[i]); return 1; }

  // 3b. application names: snowflake-sdk's rule, which the server checks at boot
  //     (snowflake_connection.spec.ts pins the server's check to snowflake-sdk's).
  const char* app_accept[] = {"Credible_Credible", "DagsterLabs_Dagster", "Ab", "a.b-c_d",
                              "Abbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"};
  const char* app_reject[] = {"", "A", "1Credible", "_Credible", "Credible Data", "Credible/Data",
                              "Abbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"};
  for (size_t i = 0; i < sizeof(app_accept) / sizeof(*app_accept); ++i)
    if (!validate_app(app_accept[i])) { fprintf(stderr, "application validator rejected \"%s\"\n", app_accept[i]); return 1; }
  for (size_t i = 0; i < sizeof(app_reject) / sizeof(*app_reject); ++i)
    if (validate_app(app_reject[i])) { fprintf(stderr, "application validator accepted \"%s\"\n", app_reject[i]); return 1; }

  // 4. the application name reaches the database before init.
  const char* stub_path = getenv("ADBC_REAL_DRIVER");
  char reported[256];
  if (stub_path && *stub_path) {
    // Against the stub: read what DatabaseInit saw. RTLD_NOLOAD returns the
    // handle the shim already opened.
    void* stub = dlopen(stub_path, RTLD_NOW | RTLD_NOLOAD);
    const char* (*seen)(void) = stub ? (const char* (*)(void))dlsym(stub, "AdbcStubAppNameAtInit") : NULL;
    int (*inits)(void) = stub ? (int (*)(void))dlsym(stub, "AdbcStubDatabaseInits") : NULL;
    if (!seen || !inits) { fprintf(stderr, "stub driver exports no AdbcStubAppNameAtInit\n"); return 1; }
    struct { const char* value; const char* expect; } cases[] = {
        {"Credible_Credible", "Credible_Credible"},
        {"Credible Data", ""},  // invalid: not applied, logged at driver load
        {NULL, ""},             // unset: pass-through
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(*cases); ++i) {
      int before = inits();
      if (open_database(&driver, cases[i].value, NULL, 0) != ADBC_STATUS_OK || inits() != before + 1) {
        fprintf(stderr, "stub DatabaseInit did not run for %s\n", cases[i].value ? cases[i].value : "(unset)");
        return 1;
      }
      if (strcmp(seen(), cases[i].expect) != 0) {
        fprintf(stderr, "app_name at init for %s was \"%s\", expected \"%s\"\n",
                cases[i].value ? cases[i].value : "(unset)", seen(), cases[i].expect);
        return 1;
      }
    }
  } else {
    // Against the real driver: the name it will report, prefix and all. A
    // driver that cannot init offline is reported, not hidden: it means this
    // check no longer runs and needs a different way in.
    if (open_database(&driver, "Credible_Credible", reported, sizeof(reported)) != ADBC_STATUS_OK) {
      fprintf(stderr, "real driver DatabaseInit failed offline\n");
      return 1;
    }
    size_t n = strlen(reported), want = strlen("Credible_Credible");
    if (strncmp(reported, "[ADBC]", 6) != 0 || n < want ||
        strcmp(reported + n - want, "Credible_Credible") != 0) {
      fprintf(stderr, "real driver reports app_name \"%s\", expected \"[ADBC]...Credible_Credible\"\n", reported);
      return 1;
    }
    printf("real driver reports app_name \"%s\"\n", reported);
  }
  unsetenv("PUBLISHER_SNOWFLAKE_APPLICATION");

  printf("adbc-shim selftest ok: StatementNew and DatabaseInit wrapped in the shim, DatabaseNew in the real "
         "driver, re-init is a no-op, validator matrices hold, application name reaches the database\n");
  return 0;
}
