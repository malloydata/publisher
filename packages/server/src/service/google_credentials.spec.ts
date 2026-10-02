// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "fs";
import * as os from "os";
import * as path from "path";
import { internalErrorToHttpError, ServerConfigurationError } from "../errors";
import { testConnectionConfig } from "./connection";
import { assertGoogleCredentialsIsNotADirectory } from "./google_credentials";

describe("GOOGLE_APPLICATION_CREDENTIALS that names a directory", () => {
   const saved = process.env.GOOGLE_APPLICATION_CREDENTIALS;
   let dir: string;

   beforeEach(() => {
      dir = mkdtempSync(path.join(os.tmpdir(), "google-creds-spec-"));
   });

   afterEach(() => {
      if (saved === undefined)
         delete process.env.GOOGLE_APPLICATION_CREDENTIALS;
      else process.env.GOOGLE_APPLICATION_CREDENTIALS = saved;
      rmSync(dir, { recursive: true, force: true });
   });

   it("is refused with a message that says it is a directory", () => {
      const keyPath = path.join(dir, "credentials.json");
      mkdirSync(keyPath);
      process.env.GOOGLE_APPLICATION_CREDENTIALS = keyPath;

      let thrown: unknown;
      try {
         assertGoogleCredentialsIsNotADirectory();
      } catch (error) {
         thrown = error;
      }
      expect(thrown).toBeInstanceOf(ServerConfigurationError);
      const { status, json } = internalErrorToHttpError(thrown as Error);
      expect(status).toBe(500);
      expect(json.message).toMatch(/directory/);
   });

   it("leaves a file, a missing path and an unset variable to google-auth", () => {
      const keyPath = path.join(dir, "credentials.json");
      writeFileSync(keyPath, "{}");
      for (const value of [keyPath, path.join(dir, "absent.json"), undefined]) {
         if (value === undefined)
            delete process.env.GOOGLE_APPLICATION_CREDENTIALS;
         else process.env.GOOGLE_APPLICATION_CREDENTIALS = value;
         expect(() => assertGoogleCredentialsIsNotADirectory()).not.toThrow();
      }
   });

   it("is named by a BigQuery connection test that relies on it", async () => {
      const keyPath = path.join(dir, "credentials.json");
      mkdirSync(keyPath);
      process.env.GOOGLE_APPLICATION_CREDENTIALS = keyPath;

      const result = await testConnectionConfig({
         name: "bq",
         type: "bigquery",
         bigqueryConnection: {},
      });
      expect(result.status).toBe("failed");
      expect(result.errorMessage).toMatch(/directory/);
   });
});
