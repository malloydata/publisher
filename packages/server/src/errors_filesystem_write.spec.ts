// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { internalErrorToHttpError, PackageNotFoundError } from "./errors";
import {
   deserializeError,
   serializeError,
} from "./package_load/package_load_pool";

// The image runs as uid 1000 (#1273), so a mount the server cannot write is a
// deployment fault an operator fixes with a chown, not a server bug. Every write
// site in the server -- an environment's .temp_ download dir, publisher.db, a
// package swap, a README or publisher.json -- surfaces the failure as a Node
// errno error. These pin the contract at the one place every REST and MCP
// failure passes through, so a write site nobody thought of is covered too.
//
// The assertions are deliberately loose about wording: the response must say
// WHICH errno it was, and must not be the generic body.

const GENERIC_INTERNAL_MESSAGE = "Internal server error.";

function errnoError(
   code: string,
   syscall: string,
   path: string,
): NodeJS.ErrnoException {
   const description: Record<string, string> = {
      EACCES: "permission denied",
      EPERM: "operation not permitted",
      EROFS: "read-only file system",
   };
   const error = new Error(
      `${code}: ${description[code]}, ${syscall} '${path}'`,
   ) as NodeJS.ErrnoException;
   error.code = code;
   error.syscall = syscall;
   error.path = path;
   error.errno = code === "EACCES" ? -13 : code === "EPERM" ? -1 : -30;
   return error;
}

describe("internalErrorToHttpError: a filesystem write the server cannot make", () => {
   const cases: Array<[string, string, string]> = [
      // S2: a publisher_data volume written by a root-run 0.8.x.
      [
         "EACCES",
         "mkdir",
         "/publisher/publisher_data/local/.temp_0123456789abcdef",
      ],
      // A runtime install staging into a root-owned environment directory.
      ["EACCES", "mkdir", "/publisher/publisher_data/local/.staging-tiny"],
      // A package mount bound read-only, written to by a package swap.
      ["EROFS", "rm", "/publisher/publisher_data/local/tiny"],
      // A chown/chmod/rename the kernel refuses outright.
      ["EPERM", "rename", "/publisher/publisher_data/local/tiny"],
   ];

   for (const [code, syscall, path] of cases) {
      it(`${code} on ${syscall} is not answered with the generic body`, () => {
         const { status, json } = internalErrorToHttpError(
            errnoError(code, syscall, path),
         );
         expect(status).toBeGreaterThanOrEqual(500);
         expect(json.message).not.toBe(GENERIC_INTERNAL_MESSAGE);
         expect(json.message).toContain(code);
      });
   }

   it("recognizes the errno when a write site wraps it in a cause", () => {
      const wrapped = new Error("Failed to install package tiny", {
         cause: errnoError("EACCES", "mkdir", "/tmp/packages/tiny"),
      });
      const { json } = internalErrorToHttpError(wrapped);
      expect(json.message).not.toBe(GENERIC_INTERNAL_MESSAGE);
      expect(json.message).toContain("EACCES");
   });

   it("recognizes the errno after it crosses the package-load worker boundary", () => {
      // Package loads run in a worker thread and their errors cross as a
      // serialized shape, through structured clone, like postMessage does.
      const original = errnoError(
         "EACCES",
         "open",
         "/publisher/publisher_data/local/tiny/model.malloy",
      );
      const crossed = deserializeError(
         structuredClone(serializeError(original)),
      );
      const { json } = internalErrorToHttpError(crossed);
      expect(json.message).not.toBe(GENERIC_INTERNAL_MESSAGE);
      expect(json.message).toContain("EACCES");
   });

   it("answers a not-found wrap around a refused access with the errno, not 404", () => {
      // A 404 "does not exist" sends the operator looking for a missing file
      // when the file is there and only its permissions are wrong.
      const wrapped = new PackageNotFoundError(
         "Failed to mount local directory: /tmp/packages/tiny",
         { cause: errnoError("EACCES", "mkdir", "/tmp/packages/tiny") },
      );
      const { status, json } = internalErrorToHttpError(wrapped);
      expect(status).toBe(500);
      expect(json.message).toContain("EACCES");
   });

   it("still withholds an unrelated internal failure", () => {
      // The guard rail: the fix must not turn the fallback branch into a
      // pass-through. ENOENT is not a permissions problem and keeps the generic
      // body, as does a plain Error.
      const enoent = errnoError("EACCES", "open", "/x");
      enoent.code = "ENOENT";
      enoent.message = "ENOENT: no such file or directory, open '/x'";
      expect(internalErrorToHttpError(enoent).json.message).toBe(
         GENERIC_INTERNAL_MESSAGE,
      );
      expect(
         internalErrorToHttpError(new Error("boom at /srv/secret")).json
            .message,
      ).toBe(GENERIC_INTERNAL_MESSAGE);
   });

   it("does not take an errno from message text or from a code alone", () => {
      // Matching on text would pass through any failure whose message happens
      // to mention a permission problem, such as a driver error echoing the
      // caller's SQL. A `code` with no `syscall` is not a Node errno error.
      expect(
         internalErrorToHttpError(
            new Error("EACCES: permission denied, open '/srv/secret'"),
         ).json.message,
      ).toBe(GENERIC_INTERNAL_MESSAGE);
      const driverError = new Error("driver said no") as NodeJS.ErrnoException;
      driverError.code = "EACCES";
      expect(internalErrorToHttpError(driverError).json.message).toBe(
         GENERIC_INTERNAL_MESSAGE,
      );
   });
});
