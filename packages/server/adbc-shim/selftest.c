// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Build-time check for the shim, run in the Dockerfile's adbc-driver stage. No
// network, no credentials. Exit 0 on success. Three things it proves:
//   1. dlopen'd as the driver manager would, AdbcDriverInit populates the table
//      and the StatementNew slot resolves into the SHIM (exact basename, so the
//      real driver's longer name cannot satisfy it);
//   2. a second init on the same table does not re-wrap (the self-wrap guard);
//   3. the env-var validator accepts exactly the positive integers a C int holds.
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
  if (!init || !validate || !captured) {
    fprintf(stderr, "shim exports no AdbcDriverInit / AdbcShimIsPositiveInteger / AdbcShimCapturedStatementNew\n");
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
  if (!real_before || real_before == wrapped) {
    fprintf(stderr, "captured real StatementNew is missing or is the wrapper\n");
    return 1;
  }
  s = init(ADBC_VERSION_1_1_0, &driver, &err);
  if (s != ADBC_STATUS_OK || (void*)driver.StatementNew != wrapped || captured() != real_before) {
    fprintf(stderr, "second init re-wrapped the table (status %d, captured %p -> %p)\n", (int)s,
            real_before, captured());
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

  printf("adbc-shim selftest ok: StatementNew wrapped in the shim, DatabaseNew in the real driver, "
         "re-init is a no-op, validator matrix holds\n");
  return 0;
}
