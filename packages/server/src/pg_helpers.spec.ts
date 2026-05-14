import { afterEach, describe, expect, it } from "bun:test";
import { ConnectionAuthError } from "./errors";
import {
   classifyPgError,
   handlePgAttachError,
   pgConnectTimeoutSeconds,
   redactPgSecrets,
   withPgConnectTimeout,
} from "./pg_helpers";

describe("pgConnectTimeoutSeconds", () => {
   const ORIGINAL_TIMEOUT = process.env.PG_CONNECT_TIMEOUT_SECONDS;

   afterEach(() => {
      if (ORIGINAL_TIMEOUT === undefined) {
         delete process.env.PG_CONNECT_TIMEOUT_SECONDS;
      } else {
         process.env.PG_CONNECT_TIMEOUT_SECONDS = ORIGINAL_TIMEOUT;
      }
   });

   it("defaults to 5 when env unset", () => {
      delete process.env.PG_CONNECT_TIMEOUT_SECONDS;
      expect(pgConnectTimeoutSeconds()).toBe(5);
   });

   it("honors PG_CONNECT_TIMEOUT_SECONDS override", () => {
      process.env.PG_CONNECT_TIMEOUT_SECONDS = "12";
      expect(pgConnectTimeoutSeconds()).toBe(12);
   });

   it("falls back to 5 when env value is invalid", () => {
      process.env.PG_CONNECT_TIMEOUT_SECONDS = "not-a-number";
      expect(pgConnectTimeoutSeconds()).toBe(5);
   });

   it("falls back to 5 when env value is zero or negative", () => {
      process.env.PG_CONNECT_TIMEOUT_SECONDS = "0";
      expect(pgConnectTimeoutSeconds()).toBe(5);
      process.env.PG_CONNECT_TIMEOUT_SECONDS = "-3";
      expect(pgConnectTimeoutSeconds()).toBe(5);
   });
});

describe("withPgConnectTimeout", () => {
   it("appends to keyword form when missing", () => {
      expect(withPgConnectTimeout("host=h dbname=d user=u password=p", 5)).toBe(
         "host=h dbname=d user=u password=p connect_timeout=5",
      );
   });

   it("appends to postgres: keyword form (DuckLake catalogUrl shape)", () => {
      expect(
         withPgConnectTimeout("postgres:host=h user=u password=p dbname=d", 5),
      ).toBe("postgres:host=h user=u password=p dbname=d connect_timeout=5");
   });

   it("does not override a user-supplied connect_timeout in keyword form", () => {
      expect(withPgConnectTimeout("host=h connect_timeout=30", 99)).toBe(
         "host=h connect_timeout=30",
      );
   });

   it("appends to URI form with no query", () => {
      expect(withPgConnectTimeout("postgresql://u:p@h:5432/d", 5)).toBe(
         "postgresql://u:p@h:5432/d?connect_timeout=5",
      );
   });

   it("appends to URI form with existing query", () => {
      expect(
         withPgConnectTimeout("postgresql://u:p@h/d?sslmode=require", 5),
      ).toBe("postgresql://u:p@h/d?sslmode=require&connect_timeout=5");
   });

   it("appends to URI with bare trailing ?", () => {
      expect(withPgConnectTimeout("postgresql://h/d?", 5)).toBe(
         "postgresql://h/d?connect_timeout=5",
      );
   });

   it("does not double-append when URI already has connect_timeout (?-style)", () => {
      expect(
         withPgConnectTimeout("postgresql://h/d?connect_timeout=10", 5),
      ).toBe("postgresql://h/d?connect_timeout=10");
   });

   it("does not double-append when URI already has connect_timeout (&-style)", () => {
      expect(
         withPgConnectTimeout(
            "postgresql://h/d?sslmode=require&connect_timeout=10",
            5,
         ),
      ).toBe("postgresql://h/d?sslmode=require&connect_timeout=10");
   });

   it("recognizes postgres:// (alternative scheme) as URI form", () => {
      expect(withPgConnectTimeout("postgres://u@h/d", 5)).toBe(
         "postgres://u@h/d?connect_timeout=5",
      );
   });
});

describe("redactPgSecrets", () => {
   it("redacts bare password values", () => {
      expect(redactPgSecrets("host=h password=hunter2 dbname=d")).toBe(
         "host=h password=*** dbname=d",
      );
   });

   it("redacts single-quoted password values", () => {
      expect(redactPgSecrets("host=h password='s3 cret' dbname=d")).toBe(
         "host=h password=*** dbname=d",
      );
   });

   it("leaves non-secret content alone", () => {
      expect(redactPgSecrets("user=alice dbname=billing")).toBe(
         "user=alice dbname=billing",
      );
   });
});

describe("classifyPgError", () => {
   it.each([
      'password authentication failed for user "alice"',
      "no pg_hba.conf entry for host",
      'role "alice" does not exist',
      'database "billing" does not exist',
      "permission denied for relation foo",
   ])("classifies '%s' as auth error", (msg) => {
      const result = classifyPgError(new Error(msg), "PG attach");
      expect(result).toBeInstanceOf(ConnectionAuthError);
      expect(result?.message).toContain("PG attach:");
   });

   it("returns undefined for unrelated errors", () => {
      expect(
         classifyPgError(
            new Error('relation "users" does not exist'),
            "PG attach",
         ),
      ).toBeUndefined();
      expect(
         classifyPgError(new Error("connection reset by peer"), "PG attach"),
      ).toBeUndefined();
   });

   it("returns undefined for non-Error values", () => {
      expect(
         classifyPgError("password authentication failed", "ctx"),
      ).toBeUndefined();
      expect(classifyPgError(undefined, "ctx")).toBeUndefined();
   });

   it("redacts embedded passwords in the wrapped message", () => {
      const result = classifyPgError(
         new Error(
            "password authentication failed: tried host=h password=hunter2",
         ),
         "DuckLake attach",
      );
      expect(result?.message).toContain("password=***");
      expect(result?.message).not.toContain("hunter2");
   });
});

describe("handlePgAttachError", () => {
   it("swallows 'already exists' errors", () => {
      const outcome = handlePgAttachError(
         new Error('database "db_x" already exists'),
         "ctx",
      );
      expect(outcome.action).toBe("swallow");
   });

   it("swallows 'already attached' errors", () => {
      const outcome = handlePgAttachError(
         new Error("DuckLake catalog db_x is already attached"),
         "ctx",
      );
      expect(outcome.action).toBe("swallow");
   });

   it("classifies libpq auth failures as ConnectionAuthError", () => {
      const outcome = handlePgAttachError(
         new Error('password authentication failed for user "alice"'),
         "PG attach db_x",
      );
      expect(outcome.action).toBe("throw");
      if (outcome.action === "throw") {
         expect(outcome.error).toBeInstanceOf(ConnectionAuthError);
         expect(outcome.error.message).toContain("PG attach db_x:");
      }
   });

   it("passes through unrelated Error instances unchanged", () => {
      const original = new Error("network unreachable");
      const outcome = handlePgAttachError(original, "ctx");
      expect(outcome.action).toBe("throw");
      if (outcome.action === "throw") {
         expect(outcome.error).toBe(original);
         expect(outcome.error).not.toBeInstanceOf(ConnectionAuthError);
      }
   });

   it("wraps non-Error throwables so callers always get an Error", () => {
      const outcome = handlePgAttachError("a string was thrown", "ctx");
      expect(outcome.action).toBe("throw");
      if (outcome.action === "throw") {
         expect(outcome.error).toBeInstanceOf(Error);
         expect(outcome.error.message).toBe("a string was thrown");
      }
   });

   it("prefers 'already attached' over auth classification when both keywords appear", () => {
      // Defensive: if a future DuckDB version emits a combined message,
      // 'already attached' wins so we don't bubble up a false auth failure
      // on what is actually a benign idempotent re-attach.
      const outcome = handlePgAttachError(
         new Error("already attached; permission denied tail"),
         "ctx",
      );
      expect(outcome.action).toBe("swallow");
   });
});
