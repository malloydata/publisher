// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { Annotations, type PersistSource } from "@malloydata/malloy";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { components } from "../../../api";
import {
   packageTableSuffix,
   versionedTableName,
   versionTableSuffix,
} from "../../versions/materialization_scope";
import type { BuildInstruction, ManifestEntry } from "../domain/types";
import {
   colocatedNameViewSite,
   pointNameView,
   storageNameViewSite,
   type NameViewContext,
} from "./name_view";
import { nameViewComment } from "./name_view_sql";
import {
   assertTableOwnable,
   hasAuthorTableName,
   hasReservedTableSuffix,
   markOwnedTable,
   ownedElsewhere,
   ownedTableComment,
   ownedTableMarkSql,
   ownedTableProbeSql,
   packageRecordsName,
   parseOwnedTableComment,
   probeTableHolder,
   tableHolderOf,
   TableOwnershipError,
   TABLE_OWNED_ELSEWHERE,
   TABLE_OWNERSHIP_UNVERIFIED,
   uncarryUnownedTables,
} from "./table_ownership";

type ApiConnection = components["schemas"]["Connection"];

/**
 * Who owns a table an auto-run writes at the author's own `name=`: the mark,
 * the probe, the build's refusal, the carry-forward recheck and the drop
 * guard, against a DuckDB file and a DuckLake catalog. The end-to-end case
 * (two packages and two environments on one Postgres schema and one DuckLake
 * destination, built and queried) is in
 * tests/integration/materialization/cross_package_tables.integration.spec.ts.
 */

const A = "env1/pkg_a";
const B = "env1/pkg_b";

describe("the package's table suffix", () => {
   it("differs per package and per environment, and is stable", () => {
      const a = packageTableSuffix({
         environmentName: "e1",
         packageName: "p",
      });
      expect(a).toMatch(/^__p[0-9a-f]{8}$/);
      expect(
         packageTableSuffix({ environmentName: "e1", packageName: "p" }),
      ).toBe(a);
      expect(
         packageTableSuffix({ environmentName: "e2", packageName: "p" }),
      ).not.toBe(a);
      expect(
         packageTableSuffix({ environmentName: "e1", packageName: "q" }),
      ).not.toBe(a);
      // Never a version's suffix, so a package's shared tables and a
      // version's own never meet.
      expect(
         versionTableSuffix({
            environmentName: "e1",
            packageName: "p",
            version: "1.0.0",
         }),
      ).not.toContain(a.slice(3));
      // The parts are delimited: `e/1` + `p` is not `e` + `1/p`.
      expect(
         packageTableSuffix({ environmentName: "e/1", packageName: "p" }),
      ).not.toBe(
         packageTableSuffix({ environmentName: "e", packageName: "1/p" }),
      );
   });
});

describe("the ownership mark", () => {
   it("round-trips any package name, and nothing else reads as one", () => {
      for (const owner of [A, "e n/v'x", "a=b c"]) {
         expect(parseOwnedTableComment(ownedTableComment(owner))).toBe(owner);
         // BigQuery's TABLE_OPTIONS spelling.
         expect(parseOwnedTableComment(`"${ownedTableComment(owner)}"`)).toBe(
            owner,
         );
      }
      for (const comment of [
         null,
         "",
         "some comment",
         "malloy-publisher:table",
         "malloy-publisher:table owner=",
         "malloy-publisher:table owner=a extra",
         "malloy-publisher:tablex owner=a",
         nameViewComment({ target: "t", owner: A }),
      ]) {
         expect(parseOwnedTableComment(comment)).toBeUndefined();
      }
   });

   it("is written in each dialect's own table comment", () => {
      expect(ownedTableMarkSql(["s", "t"], A, "postgres")).toBe(
         `COMMENT ON TABLE "s"."t" IS 'malloy-publisher:table owner=env1%2Fpkg_a'`,
      );
      expect(ownedTableMarkSql(["d", "t"], A, "standardsql")).toContain(
         "SET OPTIONS(description = ",
      );
      expect(ownedTableMarkSql(["t"], A, "mysql")).toContain("COMMENT = ");
      expect(ownedTableMarkSql(["t"], A, "sqlserver")).toContain(
         "@level1type = N'TABLE'",
      );
      expect(ownedTableProbeSql(["t"], "mysql")).toContain("TABLE_COMMENT");
      expect(ownedTableProbeSql(["c", "s", "t"], "presto")).toContain(
         "information_schema.tables",
      );
   });

   it("reads who holds a name, as one package sees it", () => {
      const table = (comment: unknown) => ({ kind: "BASE TABLE", comment });
      const view = (comment: unknown) => ({ kind: "VIEW", comment });
      expect(tableHolderOf([], A)).toEqual({ state: "free" });
      expect(tableHolderOf([table(ownedTableComment(A))], A)).toEqual({
         state: "ours",
      });
      expect(tableHolderOf([table(null)], A)).toEqual({ state: "unmarked" });
      expect(tableHolderOf([table("a reader's note")], A)).toEqual({
         state: "unmarked",
      });
      expect(tableHolderOf([table(ownedTableComment(B))], A)).toEqual({
         state: "taken",
         holder: `a table package '${B}' built`,
      });
      expect(
         tableHolderOf([view(nameViewComment({ target: "t", owner: A }))], A),
      ).toEqual({ state: "ours", view: true });
      expect(
         tableHolderOf([view(nameViewComment({ target: "t", owner: B }))], A),
      ).toEqual({
         state: "taken",
         holder: `a name= view package '${B}' made`,
      });
      expect(tableHolderOf([view(null)], A)).toEqual({
         state: "taken",
         holder: "a view Publisher did not create",
      });
      // Two relations a DuckDB two-part name may mean: either taken wins.
      expect(
         tableHolderOf(
            [table(ownedTableComment(A)), table(ownedTableComment(B))],
            A,
         ).state,
      ).toBe("taken");
   });
});

describe("whose name a table is", () => {
   const source = (sourceID: string, annotation: string) =>
      ({
         sourceID,
         name: sourceID,
         annotations: new Annotations({
            notes: [
               {
                  text: annotation,
                  at: {
                     url: "file:///pkg/m.malloy",
                     range: {
                        start: { line: 0, character: 0 },
                        end: { line: 0, character: 0 },
                     },
                  },
               },
            ],
         } as ConstructorParameters<typeof Annotations>[0]),
      }) as unknown as PersistSource;

   it("the author's only for a name= the author wrote", () => {
      const compiled = {
         preaggregatePlans: { rollup: {} },
         materializePlans: {
            declared: { persist: { name: "t" } },
            defaulted: { persist: { name: "defaulted" }, nameDefaulted: true },
         },
      } as never;
      expect(
         hasAuthorTableName(compiled, source("v1", '#@ persist name="t"\n')),
      ).toBe(true);
      expect(hasAuthorTableName(compiled, source("v1", "#@ persist\n"))).toBe(
         false,
      );
      // A rollup's name is Publisher's, even when it carries a namespace.
      expect(
         hasAuthorTableName(
            compiled,
            source("rollup", '#@ persist name="ns.r"\n'),
         ),
      ).toBe(false);
      expect(
         hasAuthorTableName(
            compiled,
            source("declared", '#@ persist name="t"\n'),
         ),
      ).toBe(true);
      expect(
         hasAuthorTableName(
            compiled,
            source("defaulted", '#@ persist name="defaulted"\n'),
         ),
      ).toBe(false);
   });

   it("never a name= that ends the way Publisher names a package's or a version's own tables", () => {
      const own = packageTableSuffix({
         environmentName: "e",
         packageName: "p",
      });
      const version = versionTableSuffix({
         environmentName: "e",
         packageName: "p",
         version: "1.2.0-rc.1",
      });
      for (const name of [
         `orders${own}`,
         `s.orders${own.toUpperCase()}`,
         `orders${version}`,
         versionedTableName("x".repeat(80), own),
      ]) {
         expect(hasReservedTableSuffix(name)).toBe(true);
      }
      for (const name of [
         "orders",
         "orders__p",
         "orders_p1a2b3c4d",
         "p1a2b3c4d.t",
      ]) {
         expect(hasReservedTableSuffix(name)).toBe(false);
      }
   });

   it("this package's by its records only for exactly that table, there", () => {
      const run = (
         entry: Partial<ManifestEntry>,
         status = "MANIFEST_FILE_READY",
      ) =>
         ({
            status,
            manifest: {
               entries: {
                  a: {
                     sourceEntityId: "a",
                     physicalTableName: "named_t",
                     connectionName: "wh",
                     ...entry,
                  },
               },
            },
         }) as never;
      const target = { physicalTableName: "named_t", connectionName: "wh" };
      expect(packageRecordsName([run({})], target)).toBe(true);
      for (const entry of [
         { physicalTableName: "NAMED_T" },
         { physicalTableName: "s.named_t" },
         { connectionName: "wh2" },
         { storageDestinationName: "lake" },
         { error: "boom" },
      ]) {
         expect(packageRecordsName([run(entry)], target)).toBe(false);
      }
      expect(packageRecordsName([run({}, "FAILED")], target)).toBe(false);
      expect(
         packageRecordsName([run({ storageDestinationName: "lake" })], {
            ...target,
            storageDestinationName: "lake",
         }),
      ).toBe(true);
   });
});

describe("on a DuckDB connection", () => {
   let dir: string;
   let connection: DuckDBConnection;
   const run = (sql: string) => connection.runSQL(sql);
   const comment = async (table: string) =>
      (
         await run(
            `SELECT comment FROM duckdb_tables() WHERE table_name = '${table}'`,
         )
      ).rows[0]?.comment;

   beforeEach(() => {
      dir = mkdtempSync(join(tmpdir(), "table-ownership-"));
      connection = new DuckDBConnection(
         "table_ownership",
         join(dir, "wh.duckdb"),
         dir,
      );
   });
   afterEach(async () => {
      await connection.close().catch(() => undefined);
      rmSync(dir, { recursive: true, force: true });
   });

   const target = { physicalTableName: "named_t", connectionName: "wh" };
   const check = (owner: string, ownRecords = false) =>
      assertTableOwnable(
         colocatedNameViewSite(connection),
         target,
         owner,
         "named",
         async () => ownRecords,
      );

   it("a free name is built, and marked once built", async () => {
      await check(A);
      await run("CREATE TABLE named_t AS SELECT 1 AS x");
      await markOwnedTable(colocatedNameViewSite(connection), "named_t", A);
      expect(await comment("named_t")).toBe(ownedTableComment(A));
      expect(
         await probeTableHolder(
            colocatedNameViewSite(connection),
            "named_t",
            A,
         ),
      ).toEqual({ state: "ours" });
      // Its own package builds it again; another is refused, coded.
      await check(A);
      const refused = await check(B).catch((e) => e);
      expect(refused).toBeInstanceOf(TableOwnershipError);
      expect(refused.code).toBe(TABLE_OWNED_ELSEWHERE);
      expect(refused.message).toContain(`a table package '${A}' built`);
   });

   it("a table with no mark is this package's only when its records say so", async () => {
      await run("CREATE TABLE named_t AS SELECT 1 AS x");
      await check(A, true);
      const refused = await check(A, false).catch((e) => e);
      expect(refused).toBeInstanceOf(TableOwnershipError);
      expect(refused.code).toBe(TABLE_OWNED_ELSEWHERE);
      expect(refused.message).toContain("no Publisher mark");
   });

   it("this package's own name= view is its own; any other view is refused", async () => {
      await run("CREATE TABLE g1 AS SELECT 1 AS x");
      const site = colocatedNameViewSite(connection);
      await pointNameView(site, "named_t", { target: "g1", owner: A });
      await check(A);
      expect((await check(B).catch((e) => e)).code).toBe(TABLE_OWNED_ELSEWHERE);
      await run("DROP VIEW named_t");
      await run("CREATE VIEW named_t AS SELECT 2 AS x");
      expect((await check(A, true).catch((e) => e)).code).toBe(
         TABLE_OWNED_ELSEWHERE,
      );
   });

   it("a name whose holder cannot be read is refused", async () => {
      const failing = {
         ...colocatedNameViewSite(connection),
         run: async () => {
            throw new Error("permission denied for information schema");
         },
      };
      const refused = await assertTableOwnable(
         failing,
         target,
         A,
         "named",
         async () => true,
      ).catch((e) => e);
      expect(refused.code).toBe(TABLE_OWNERSHIP_UNVERIFIED);
      expect(refused.message).toContain("permission denied");
      expect(
         (
            await assertTableOwnable(
               undefined,
               target,
               A,
               "named",
               async () => true,
            ).catch((e) => e)
         ).code,
      ).toBe(TABLE_OWNERSHIP_UNVERIFIED);
      expect(
         (
            await assertTableOwnable(
               { ...colocatedNameViewSite(connection), dialect: "oracle" },
               target,
               A,
               "named",
               async () => true,
            ).catch((e) => e)
         ).code,
      ).toBe(TABLE_OWNERSHIP_UNVERIFIED);
   });

   it("a drop sees a table another package marked", async () => {
      const site = colocatedNameViewSite(connection);
      await run("CREATE TABLE named_t AS SELECT 1 AS x");
      expect(await ownedElsewhere(site, "named_t", A)).toBeUndefined();
      await markOwnedTable(site, "named_t", B);
      expect(await ownedElsewhere(site, "named_t", A)).toBe(B);
      expect(await ownedElsewhere(site, "named_t", B)).toBeUndefined();
      expect(await ownedElsewhere(site, "no_such_table", A)).toBeUndefined();
   });

   it("a carried table at the author's name= is reused only while marked as this package's", async () => {
      const context: NameViewContext = {
         connectionFor: () => connection,
         destinationFor: () => undefined,
         environmentPath: dir,
      };
      const entry = (table: string): ManifestEntry =>
         ({
            sourceEntityId: table,
            sourceName: table,
            physicalTableName: table,
            connectionName: "wh",
         }) as ManifestEntry;
      const instruction = (table: string): BuildInstruction =>
         ({
            sourceEntityId: table,
            materializedTableId: `local-${table}`,
            physicalTableName: table,
            realization: "COPY",
         }) as BuildInstruction;
      await run("CREATE TABLE mine AS SELECT 1 AS x");
      await markOwnedTable(colocatedNameViewSite(connection), "mine", A);
      await run("CREATE TABLE theirs AS SELECT 1 AS x");
      await markOwnedTable(colocatedNameViewSite(connection), "theirs", B);
      await run("CREATE TABLE legacy AS SELECT 1 AS x");
      const names = ["mine", "theirs", "legacy", "gone"];
      const carried = Object.fromEntries(names.map((n) => [n, entry(n)]));
      const instructions: BuildInstruction[] = [];
      await uncarryUnownedTables(
         Object.fromEntries(names.map((n) => [n, instruction(n)])),
         carried,
         instructions,
         A,
         context,
      );
      expect(Object.keys(carried)).toEqual(["mine"]);
      expect(instructions.map((i) => i.physicalTableName).sort()).toEqual([
         "gone",
         "legacy",
         "theirs",
      ]);
   });
});

describe("in a DuckLake destination", () => {
   let dir: string;
   let session: DuckDBConnection;
   const lake = { name: "lake", type: "ducklake" } as ApiConnection;

   beforeEach(async () => {
      dir = mkdtempSync(join(tmpdir(), "table-ownership-lake-"));
      session = new DuckDBConnection("table_ownership_lake", ":memory:", dir);
      await session.runSQL("INSTALL ducklake");
      await session.runSQL("LOAD ducklake");
      await session.runSQL(
         `ATTACH 'ducklake:${join(dir, "catalog.ducklake")}' AS lake ` +
            `(DATA_PATH '${join(dir, "data")}')`,
      );
   });
   afterEach(async () => {
      await session.close().catch(() => undefined);
      rmSync(dir, { recursive: true, force: true });
   });

   it("a stored table is marked, read back, and refused to another package", async () => {
      const site = storageNameViewSite("lake", lake, session);
      const target = {
         physicalTableName: "stored_named",
         connectionName: "wh",
         storageDestinationName: "lake",
      };
      await assertTableOwnable(site, target, A, "s", async () => false);
      await session.runSQL(
         "CREATE TABLE lake.main.stored_named AS SELECT 1 AS x",
      );
      await markOwnedTable(site, "stored_named", A);
      expect(await probeTableHolder(site, "stored_named", A)).toEqual({
         state: "ours",
      });
      await assertTableOwnable(site, target, A, "s", async () => false);
      expect(
         (
            await assertTableOwnable(
               site,
               target,
               B,
               "s",
               async () => true,
            ).catch((e) => e)
         ).code,
      ).toBe(TABLE_OWNED_ELSEWHERE);
      expect(await ownedElsewhere(site, "stored_named", B)).toBe(A);
   });
});
