import { describe, expect, it } from "bun:test";
import { redactSensitive } from "./logger";

describe("redactSensitive", () => {
   it("masks every known credential field at the top level", () => {
      const input = {
         password: "hunter2",
         connectionString: "postgresql://u:p@h/db",
         serviceAccountKeyJson: '{"private_key":"-----BEGIN..."}',
         privateKey: "-----BEGIN PRIVATE KEY-----",
         privateKeyPass: "passphrase",
         secret: "gcs-hmac-secret",
         secretAccessKey: "aws-secret",
         sessionToken: "aws-session",
         sasUrl: "https://a.blob.core.windows.net/c?sig=abc",
         clientSecret: "azure-client-secret",
         oauthClientSecret: "databricks-oauth-secret",
         peakaKey: "peaka-api-key",
         token: "databricks-pat",
         accessToken: "motherduck-token",
      };
      const out = redactSensitive(input) as Record<string, unknown>;
      for (const key of Object.keys(input)) {
         expect(out[key]).toBe("[REDACTED]");
      }
   });

   it("keeps non-secret fields intact", () => {
      const input = {
         name: "bigquery",
         type: "bigquery",
         defaultProjectId: "my-project",
         host: "db.example.com",
         port: 5432,
      };
      expect(redactSensitive(input)).toEqual(input);
   });

   it("redacts secrets nested inside the environment PATCH payload shape", () => {
      const input = {
         name: "demo___malloy-samples",
         connections: [
            {
               name: "bigquery",
               type: "bigquery",
               bigqueryConnection: {
                  defaultProjectId: "vscode-demo",
                  serviceAccountKeyJson: '{"private_key":"-----BEGIN..."}',
               },
            },
            {
               name: "warehouse",
               type: "snowflake",
               snowflakeConnection: {
                  account: "acct",
                  username: "svc",
                  password: "s3cret",
                  privateKey: "-----BEGIN PRIVATE KEY-----",
               },
            },
         ],
      };
      expect(redactSensitive(input)).toEqual({
         name: "demo___malloy-samples",
         connections: [
            {
               name: "bigquery",
               type: "bigquery",
               bigqueryConnection: {
                  defaultProjectId: "vscode-demo",
                  serviceAccountKeyJson: "[REDACTED]",
               },
            },
            {
               name: "warehouse",
               type: "snowflake",
               snowflakeConnection: {
                  account: "acct",
                  username: "svc",
                  password: "[REDACTED]",
                  privateKey: "[REDACTED]",
               },
            },
         ],
      });
   });

   it("recurses through arrays of secrets", () => {
      const out = redactSensitive([{ password: "a" }, { password: "b" }]);
      expect(out).toEqual([
         { password: "[REDACTED]" },
         { password: "[REDACTED]" },
      ]);
   });

   it("passes primitives through unchanged", () => {
      expect(redactSensitive("plain")).toBe("plain");
      expect(redactSensitive(42)).toBe(42);
      expect(redactSensitive(null)).toBeNull();
      expect(redactSensitive(undefined)).toBeUndefined();
   });

   it("preserves Date values instead of destructuring them", () => {
      const d = new Date("2026-07-08T00:00:00.000Z");
      const out = redactSensitive({ createdAt: d }) as { createdAt: Date };
      expect(out.createdAt).toBeInstanceOf(Date);
      expect(out.createdAt.getTime()).toBe(d.getTime());
   });

   it("does not mutate the input object", () => {
      const input = { password: "hunter2", nested: { token: "abc" } };
      redactSensitive(input);
      expect(input.password).toBe("hunter2");
      expect(input.nested.token).toBe("abc");
   });

   it("tolerates circular references", () => {
      const input: Record<string, unknown> = { password: "hunter2" };
      input.self = input;
      const out = redactSensitive(input) as Record<string, unknown>;
      expect(out.password).toBe("[REDACTED]");
      expect(out.self).toBe("[Circular]");
   });
});
