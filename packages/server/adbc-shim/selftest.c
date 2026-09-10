// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Build-time check for the shim: dlopen it as the driver manager would, run
// AdbcDriverInit, and confirm the table came back populated with the shim's
// StatementNew in place. No network, no credentials. Exit 0 on success.
#define _GNU_SOURCE
#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "adbc.h"

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
  if (!init) {
    fprintf(stderr, "shim exports no AdbcDriverInit\n");
    return 1;
  }
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
  // The shim's StatementNew must be the one in the table, i.e. resolve into the shim, not the real driver.
  Dl_info info;
  if (!dladdr((void*)driver.StatementNew, &info) || !info.dli_fname || !strstr(info.dli_fname, "libadbc_driver_snowflake.so")) {
    fprintf(stderr, "StatementNew does not resolve into the shim (%s)\n", info.dli_fname ? info.dli_fname : "?");
    return 1;
  }
  printf("adbc-shim selftest ok: StatementNew wrapped, real driver loaded\n");
  return 0;
}
