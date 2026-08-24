// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, test } from "bun:test";
import {
   buildConnection,
   envVarReference,
   passwordEnvVar,
   renderEnvExample,
   resolveConnectionName,
   ownedFlags,
   validateTablePath,
   WAREHOUSE_TYPES,
   type ConnectionFlags,
} from "./connection";
import { ScaffoldError } from "./errors";

/** The minimum that builds for each dialect, so a test can vary one thing. */
const MINIMAL: Record<string, ConnectionFlags> = {
   postgres: {
      connection: "postgres",
      pgHost: "localhost",
      pgDatabase: "analytics",
      pgUser: "demo",
   },
   bigquery: {
      connection: "bigquery",
      bqProject: "my-project",
   },
   snowflake: {
      connection: "snowflake",
      sfAccount: "acme-east",
      sfUser: "demo",
      sfWarehouse: "COMPUTE_WH",
   },
};

describe("resolveConnectionName", () => {
   test("defaults to the warehouse type, which is always a legal name", () => {
      for (const type of WAREHOUSE_TYPES) {
         expect(resolveConnectionName(type)).toBe(type);
      }
   });

   test("refuses the reserved name, which fails the whole environment", () => {
      expect(() => resolveConnectionName("postgres", "duckdb")).toThrow(
         ScaffoldError,
      );
   });

   test("refuses a hyphen, which cred add connection skips", () => {
      // The trap this rule exists for: Publisher accepts `my-warehouse`
      // happily, so the failure only appears later, on the way to Credible,
      // as a connection that is simply not there. `cred` does log an error
      // when it skips one, but it carries on and exits successfully, so the
      // line is easy to lose in a batch that otherwise worked.
      expect(() => resolveConnectionName("postgres", "my-warehouse")).toThrow(
         /my_warehouse/,
      );
   });

   test("refuses a one-character name, which the `+` in the rule excludes", () => {
      expect(() => resolveConnectionName("postgres", "x")).toThrow(
         ScaffoldError,
      );
      expect(resolveConnectionName("postgres", "xy")).toBe("xy");
   });

   test("refuses a leading digit but allows a leading underscore", () => {
      expect(() => resolveConnectionName("postgres", "1warehouse")).toThrow(
         ScaffoldError,
      );
      expect(resolveConnectionName("postgres", "_warehouse")).toBe(
         "_warehouse",
      );
   });
});

describe("passwordEnvVar", () => {
   test("is uppercase, which is the only form Publisher substitutes", () => {
      // The server's pattern is /\$\{([A-Z_][A-Z0-9_]*)\}/g. A lowercase name
      // is not an error, it simply travels to the driver as literal characters,
      // so this is the assertion that keeps that failure unreachable.
      expect(passwordEnvVar("warehouse")).toBe("MALLOY_WAREHOUSE_PASSWORD");
      expect(passwordEnvVar("my_pg")).toBe("MALLOY_MY_PG_PASSWORD");
      for (const type of WAREHOUSE_TYPES) {
         expect(passwordEnvVar(type)).toMatch(/^[A-Z_][A-Z0-9_]*$/);
      }
   });

   test("is prefixed, so it cannot collide with POSTGRES_PASSWORD", () => {
      // Docker's own postgres image uses POSTGRES_PASSWORD; sharing that name
      // silently is how a connection picks up an unrelated credential.
      expect(passwordEnvVar("postgres")).not.toBe("POSTGRES_PASSWORD");
   });
});

describe("envVarReference", () => {
   test("renders the ${VAR} form the server looks for", () => {
      expect(envVarReference("MALLOY_PG_PASSWORD")).toBe(
         "${MALLOY_PG_PASSWORD}",
      );
   });
});

describe("validateTablePath", () => {
   test("accepts a schema-qualified name and a BigQuery-style path", () => {
      expect(validateTablePath("public.orders")).toBe("public.orders");
      expect(validateTablePath("my-project.ds.tbl")).toBe("my-project.ds.tbl");
   });

   test("returns undefined when no table was named", () => {
      expect(validateTablePath(undefined)).toBeUndefined();
   });

   test("refuses a quote, which would end the Malloy string early", () => {
      expect(() => validateTablePath("public.or'ders")).toThrow(ScaffoldError);
      expect(() => validateTablePath("a\\b")).toThrow(ScaffoldError);
   });

   test("refuses an empty value rather than scaffolding a stub silently", () => {
      expect(() => validateTablePath("")).toThrow(ScaffoldError);
   });
});

describe("buildConnection", () => {
   test("postgres: identity fields inline, password as a reference", () => {
      const built = buildConnection(MINIMAL.postgres);
      expect(built.entry).toEqual({
         name: "postgres",
         type: "postgres",
         postgresConnection: {
            host: "localhost",
            databaseName: "analytics",
            userName: "demo",
            port: 5432,
            password: "${MALLOY_POSTGRES_PASSWORD}",
         },
      });
      expect(built.envVars.map((v) => v.name)).toEqual([
         "MALLOY_POSTGRES_PASSWORD",
      ]);
   });

   test("no secret reaches the entry, for any dialect", () => {
      // The property that matters most in this file: whatever else changes,
      // nothing that looks like a credential value is ever written.
      for (const type of WAREHOUSE_TYPES) {
         const built = buildConnection(MINIMAL[type]);
         const serialized = JSON.stringify(built.entry);
         for (const variable of built.envVars) {
            expect(serialized).toContain(`\${${variable.name}}`);
         }
         // Every value is either a literal we were given or a ${VAR}; none of
         // the flags can carry a secret because no such flag exists.
         expect(serialized).not.toMatch(/BEGIN [A-Z ]*PRIVATE KEY/);
      }
   });

   test("postgres port defaults, and is validated when given", () => {
      expect(
         buildConnection({ ...MINIMAL.postgres, pgPort: "15432" }).entry
            .postgresConnection,
      ).toMatchObject({ port: 15432 });
      expect(() =>
         buildConnection({ ...MINIMAL.postgres, pgPort: "nope" }),
      ).toThrow(ScaffoldError);
      expect(() =>
         buildConnection({ ...MINIMAL.postgres, pgPort: "70000" }),
      ).toThrow(ScaffoldError);
   });

   test("bigquery writes no credential at all, falling through to ADC", () => {
      const built = buildConnection(MINIMAL.bigquery);
      expect(built.entry).toEqual({
         name: "bigquery",
         type: "bigquery",
         bigqueryConnection: { defaultProjectId: "my-project" },
      });
      expect(built.envVars).toEqual([]);
   });

   test("snowflake omits the optional fields that were not given", () => {
      const built = buildConnection(MINIMAL.snowflake);
      expect(built.entry.snowflakeConnection).toEqual({
         account: "acme-east",
         username: "demo",
         warehouse: "COMPUTE_WH",
         password: "${MALLOY_SNOWFLAKE_PASSWORD}",
      });
   });

   test("names the missing option rather than writing a broken entry", () => {
      expect(() =>
         buildConnection({ connection: "postgres", pgHost: "localhost" }),
      ).toThrow(/--pg-database/);
   });

   test("every dialect-specific flag is owned by exactly one dialect", () => {
      // The guard for the class of bug this table caused once: --pg-port was in
      // no dialect's list, so it was owned by nobody, passed the wrong-dialect
      // check, and was silently dropped. Any new pg/bq/sf flag added to
      // ConnectionFlags without a table entry fails here.
      const declared: (keyof ConnectionFlags)[] = [
         "pgHost",
         "pgPort",
         "pgDatabase",
         "pgUser",
         "bqProject",
         "bqLocation",
         "sfAccount",
         "sfUser",
         "sfWarehouse",
         "sfDatabase",
         "sfSchema",
         "sfRole",
      ];
      for (const flag of declared) {
         const owners = WAREHOUSE_TYPES.filter((type) =>
            ownedFlags(type).some((owned) => owned.from === flag),
         );
         expect({ flag, owners }).toEqual({ flag, owners: [owners[0]] });
      }
   });

   test("refuses --pg-port passed with a non-postgres dialect", () => {
      expect(() =>
         buildConnection({ ...MINIMAL.bigquery, pgPort: "5432" }),
      ).toThrow(/--pg-port is a postgres option/);
   });

   test("refuses a flag belonging to another dialect", () => {
      // Without this the host is dropped on the floor and the user finds out at
      // the first query, not at the command line.
      expect(() =>
         buildConnection({ ...MINIMAL.bigquery, pgHost: "localhost" }),
      ).toThrow(/--pg-host is a postgres option/);
   });

   test("refuses a type it cannot author", () => {
      expect(() => buildConnection({ connection: "oracle" })).toThrow(
         ScaffoldError,
      );
   });

   test("carries the validated table through", () => {
      expect(
         buildConnection({ ...MINIMAL.postgres, table: "public.orders" }).table,
      ).toBe("public.orders");
   });
});

describe("renderEnvExample", () => {
   test("cannot inject a line into .env.example through a user name", () => {
      // The user name is under no character rule, and this is the one place a
      // raw flag value reaches a line-oriented file rather than JSON.
      const built = buildConnection({
         connection: "postgres",
         pgHost: "localhost",
         pgDatabase: "analytics",
         pgUser: 'demo"\nMALLOY_INJECTED=surprise',
      });
      const rendered = renderEnvExample(built);
      expect(rendered).not.toMatch(/^MALLOY_INJECTED=/m);
   });

   test("lists every variable by name and none of them by value", () => {
      const built = buildConnection(MINIMAL.postgres);
      const rendered = renderEnvExample(built);
      expect(rendered).toContain("MALLOY_POSTGRES_PASSWORD=");
      // Nothing after the `=`: this file is committed.
      expect(rendered).toMatch(/MALLOY_POSTGRES_PASSWORD=\s*$/m);
   });
});
