// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Test fixture for selftest.c: an ADBC "driver" that fills only EMPTY slots of
// the AdbcDriver table and preserves whatever is already there. The real
// Snowflake driver overwrites every slot on init, so against it a second
// AdbcDriverInit on an already-wrapped table can never hand the shim its own
// StatementNew back — which is exactly the case the shim's self-wrap guard
// exists for. This stub makes that case reachable at build time: run selftest
// with ADBC_REAL_DRIVER pointing at a copy of this file named
// libadbc_driver_snowflake.real.so, and a shim without the guard captures itself
// on the second init and fails the test. Never installed in the image.
#include <string.h>

#include "adbc.h"

static AdbcStatusCode stub_DatabaseNew(struct AdbcDatabase* db, struct AdbcError* err) {
  (void)db; (void)err;
  return ADBC_STATUS_OK;
}
static AdbcStatusCode stub_StatementNew(struct AdbcConnection* c, struct AdbcStatement* s,
                                        struct AdbcError* err) {
  (void)c; (void)s; (void)err;
  return ADBC_STATUS_OK;
}
static AdbcStatusCode stub_StatementSetOption(struct AdbcStatement* s, const char* k, const char* v,
                                              struct AdbcError* err) {
  (void)s; (void)k; (void)v; (void)err;
  return ADBC_STATUS_OK;
}

AdbcStatusCode AdbcDriverInit(int version, void* raw_driver, struct AdbcError* error) {
  (void)version; (void)error;
  struct AdbcDriver* d = (struct AdbcDriver*)raw_driver;
  if (!d->DatabaseNew) d->DatabaseNew = stub_DatabaseNew;
  if (!d->StatementNew) d->StatementNew = stub_StatementNew;
  if (!d->StatementSetOption) d->StatementSetOption = stub_StatementSetOption;
  return ADBC_STATUS_OK;
}
