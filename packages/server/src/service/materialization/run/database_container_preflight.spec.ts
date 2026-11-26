// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// F-03: a database build whose table's container does not exist stops before
// it runs any statement that writes (the staging DROP, the CTAS), with a
// deterministic ContainerNotFoundError; a container whose read fails stops
// nothing.

import type { Connection, PersistSource } from "@malloydata/malloy";
import { describe, expect, it } from "bun:test";
import { ContainerNotFoundError } from "../../../errors";
import { classifyBuildError } from "./build_failure_class";
import { DatabaseTier } from "./database_build_strategy";
import type { DatabaseBuildRequest } from "./tier";

/**
 * A Postgres connection that answers the container read with `n` schemas
 * (or fails it), records every statement, and fails every other one, so the
 * build stops at its first statement after the check.
 */
function postgres(answer: { n: number } | Error) {
   const statements: string[] = [];
   const connection = {
      dialectName: "postgres",
      runSQL: async (sql: string) => {
         statements.push(sql);
         if (sql.includes("pg_namespace")) {
            if (answer instanceof Error) throw answer;
            return { rows: [{ db: "app", n: answer.n }] };
         }
         throw new Error("stop: past the check");
      },
   } as unknown as Connection;
   return { connection, statements };
}

function request(
   connection: Connection,
   physicalTableName: string,
): DatabaseBuildRequest {
   return {
      persistSource: {
         name: "by_station",
         connectionName: "pg",
         dialectName: "postgres",
      } as unknown as PersistSource,
      instruction: {
         sourceEntityId: "00000000-0000-0000-0000-000000000001",
         physicalTableName,
      },
      connection,
      manifest: { update: () => {} },
      buildSQL: "SELECT 1 AS one",
      runOptions: {},
      upstreamReuse: {},
   } as unknown as DatabaseBuildRequest;
}

describe("F-03: the database build checks its table's container first", () => {
   it("throws ContainerNotFoundError before any DROP or CTAS", async () => {
      const { connection, statements } = postgres({ n: 0 });
      const err = await new DatabaseTier()
         .build(request(connection, "no_such_schema.by_station"))
         .catch((e) => e);
      expect(err).toBeInstanceOf(ContainerNotFoundError);
      expect(err.message).toContain('schema "no_such_schema"');
      expect(err.message).toContain("or change name=");
      expect(statements).toHaveLength(1);
      expect(statements.some((s) => /DROP|CREATE/i.test(s))).toBe(false);
      // Deterministic: a retry of the same build fails the same way.
      expect(classifyBuildError(err)).toBe(false);
   });

   it("goes on to the build when the container exists", async () => {
      const { connection, statements } = postgres({ n: 1 });
      const err = await new DatabaseTier()
         .build(request(connection, "analytics.by_station"))
         .catch((e) => e);
      expect(err.message).toBe("stop: past the check");
      expect(statements[1]).toMatch(/^DROP TABLE IF EXISTS/);
   });

   it("goes on to the build when the read fails (fail open)", async () => {
      const { connection, statements } = postgres(new Error("network down"));
      const err = await new DatabaseTier()
         .build(request(connection, "analytics.by_station"))
         .catch((e) => e);
      expect(err.message).toBe("stop: past the check");
      expect(statements[1]).toMatch(/^DROP TABLE IF EXISTS/);
   });

   it("reads nothing for a bare name, which lands in the default schema", async () => {
      const { connection, statements } = postgres({ n: 0 });
      await new DatabaseTier()
         .build(request(connection, "by_station"))
         .catch(() => undefined);
      expect(statements[0]).toMatch(/^DROP TABLE IF EXISTS/);
   });
});
