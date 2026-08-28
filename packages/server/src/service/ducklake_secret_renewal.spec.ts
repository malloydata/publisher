// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { shouldRenewStorageSecret } from "./connection";

/**
 * The predicate DuckLakeConnection.runSQL actually calls -- not a copy of it. A rule
 * mirrored into a spec passes whether or not the caller uses it, which is how a test
 * comes to look like coverage without being any.
 */
describe("expired-credential retry decision", () => {
   const expired = new Error("ExpiredToken: The provided token has expired.");
   const wrongKey = new Error("InvalidAccessKeyId: no such key");

   it("renews on an expired credential when a renewer is registered", () => {
      expect(
         shouldRenewStorageSecret({
            error: expired,
            hasRenewer: true,
            alreadyRenewing: false,
         }),
      ).toBe(true);
   });

   it("does not renew for a credential that was never valid", () => {
      // Retrying a wrong key would hide a misconfiguration behind a silent second
      // attempt -- the failure mode this tier has spent its whole design avoiding.
      expect(
         shouldRenewStorageSecret({
            error: wrongKey,
            hasRenewer: true,
            alreadyRenewing: false,
         }),
      ).toBe(false);
   });

   it("does not renew a key-pair connection, which registers no renewer", () => {
      // Only a chain secret stores credentials that age out. A key pair does not
      // expire, so a renewer is never registered for one.
      expect(
         shouldRenewStorageSecret({
            error: expired,
            hasRenewer: false,
            alreadyRenewing: false,
         }),
      ).toBe(false);
   });

   it("does not renew while a renewal is already in flight", () => {
      // The renewal issues its own statement through the same runSQL, so without
      // this the failure of that statement would recurse.
      expect(
         shouldRenewStorageSecret({
            error: expired,
            hasRenewer: true,
            alreadyRenewing: true,
         }),
      ).toBe(false);
   });
});
