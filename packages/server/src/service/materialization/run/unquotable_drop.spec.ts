// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A run entry whose table name its dialect cannot quote is not dropped and
 * not owed: no statement can name it, and a retry would fail alike. The rest
 * of the sweep still drops.
 */

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import type { Connection as MalloyConnection } from "@malloydata/malloy";
import * as sinon from "sinon";
import { logger } from "../../../logger";
import { Reclaimer } from "./reclaimer";

let quiet: sinon.SinonStub[] = [];
beforeEach(() => {
   quiet = [sinon.stub(logger, "warn"), sinon.stub(logger, "info")];
});
afterEach(() => {
   for (const stub of quiet) stub.restore();
});

describe("a colocated drop of an unquotable name", () => {
   it("records nothing to drop for it, and drops the rest", async () => {
      const statements: string[] = [];
      const connection = {
         dialectName: "standardsql",
         runSQL: async (sql: string) => {
            statements.push(sql);
            return { rows: [] };
         },
      } as unknown as MalloyConnection;
      const reclaimer = new Reclaimer({
         repository: () => ({ listMaterializations: async () => [] }) as never,
         host: {} as never,
         lifecycle: {} as never,
         dropManifestEntries: async () => [],
      });
      const environment = {
         getEnvironmentName: () => "env",
         getEnvironmentPath: () => "/nonexistent",
         getApiConnection: () => ({}),
      } as never;
      const owed = await reclaimer.dropManifestEntries(
         environment,
         "env",
         "pkg",
         "run-1",
         "delete",
         [
            {
               sourceEntityId: "bad",
               physicalTableName: "ds.a\nb",
               connectionName: "bq",
            } as never,
            {
               sourceEntityId: "good",
               physicalTableName: "ds.t",
               connectionName: "bq",
            } as never,
         ],
         new Set(),
         async () => connection,
      );
      expect(owed).toEqual([]);
      const drops = statements.filter((s) => s.startsWith("DROP"));
      expect(drops.some((s) => s.includes("a\nb"))).toBe(false);
      expect(statements).toContain("DROP TABLE IF EXISTS `ds`.`t`");
      expect(
         (quiet[0].getCalls() ?? []).some((c) =>
            String(c.args[0]).includes("cannot be written as SQL"),
         ),
      ).toBe(true);
   });
});
