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
//
// It also records the app_name option a database holds when DatabaseInit
// runs, which is the one thing selftest.c cannot observe on the real driver
// without a Snowflake account: that the shim sets it before init.
#include <stdio.h>
#include <string.h>

#include "adbc.h"

#define APP_NAME_OPTION "adbc.snowflake.sql.client_option.app_name"

// One database at a time is all selftest.c opens, so plain statics will do.
static char app_name_set[128];
static char app_name_at_init[128];
static int inits;

static AdbcStatusCode stub_DatabaseNew(struct AdbcDatabase* db, struct AdbcError* err) {
  (void)db; (void)err;
  app_name_set[0] = '\0';
  return ADBC_STATUS_OK;
}
static AdbcStatusCode stub_DatabaseSetOption(struct AdbcDatabase* db, const char* k, const char* v,
                                             struct AdbcError* err) {
  (void)db; (void)err;
  if (k && v && strcmp(k, APP_NAME_OPTION) == 0) snprintf(app_name_set, sizeof(app_name_set), "%s", v);
  return ADBC_STATUS_OK;
}
static AdbcStatusCode stub_DatabaseInit(struct AdbcDatabase* db, struct AdbcError* err) {
  (void)db; (void)err;
  snprintf(app_name_at_init, sizeof(app_name_at_init), "%s", app_name_set);
  ++inits;
  return ADBC_STATUS_OK;
}

// For selftest.c: the app_name the last DatabaseInit saw ("" for none), and how
// many inits have run, so a test can tell "not set" from "init never ran".
const char* AdbcStubAppNameAtInit(void) { return app_name_at_init; }
int AdbcStubDatabaseInits(void) { return inits; }
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
  if (!d->DatabaseSetOption) d->DatabaseSetOption = stub_DatabaseSetOption;
  if (!d->DatabaseInit) d->DatabaseInit = stub_DatabaseInit;
  if (!d->StatementNew) d->StatementNew = stub_StatementNew;
  if (!d->StatementSetOption) d->StatementSetOption = stub_StatementSetOption;
  return ADBC_STATUS_OK;
}
