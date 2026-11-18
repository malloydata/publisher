// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { Annotations, type PersistSource } from "@malloydata/malloy";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { join } from "node:path";
import {
   openTempDuckDB,
   type TempDuckDB,
} from "../../../test_helpers/temp_storage";
import type { components } from "../../../api";
import {
   packageTableSuffix,
   versionedTableName,
   versionTableSuffix,
} from "../../versions/materialization_scope";
import type { BuildInstruction, ManifestEntry } from "../domain/types";
import { dropOwnNameView, ownNameViewTarget } from "./leftover_name_views";
import {
   databaseTableSite,
   storageTableSite,
   type TableSiteContext,
} from "./table_site";
import { ownedTableProbeSql } from "./warehouse_sql";
import { parseOwnedTableMark } from "./table_mark_sql";
import { hasReservedTableSuffix } from "../domain/table_name";
import {
   assertTableOwnable,
   hasAuthorTableName,
   markedFor,
   markOwnedTable,
   ownedElsewhere,
   ownedTableComment,
   ownedTableMarkSql,
   packageRecordsName,
   parseOwnedTableComment,
   probeTableHolder,
   tableHolderOf,
   tableIdentityAt,
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

/**
 * The comment an earlier release marked its `name=` views with: Publisher
 * makes none now, but still recognizes this package's own (leftover_name_views.ts).
 */
const nameViewComment = (mark: { target: string; owner: string }) =>
   `malloy-publisher:name-view owner=${encodeURIComponent(mark.owner)} ` +
   `target=${encodeURIComponent(mark.target)}`;

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
         "malloy-publisher:table owner=a table=",
         "malloy-publisher:table owner=a tablex=t",
         "malloy-publisher:table owner=a table=t extra",
         "malloy-publisher:table table=t owner=a",
      ]) {
         expect(parseOwnedTableComment(comment)).toBeUndefined();
      }
   });

   it("names the table it is on from 0.16.0; a build and a reclaim read the owner of either form", () => {
      const mark = ownedTableComment(A, "s.t");
      expect(mark).toBe("malloy-publisher:table owner=env1%2Fpkg_a table=s.t");
      expect(parseOwnedTableMark(mark)).toEqual({ owner: A, table: "s.t" });
      expect(parseOwnedTableMark(`"${mark}"`)).toEqual({
         owner: A,
         table: "s.t",
      });
      expect(parseOwnedTableComment(mark)).toBe(A);
      // The form before 0.16.0: an owner, no table.
      expect(parseOwnedTableMark(ownedTableComment(A))).toEqual({ owner: A });
      expect(parseOwnedTableComment(ownedTableComment(A))).toBe(A);
      // The table is compared as the dialect resolves it.
      expect(tableIdentityAt(["S", "Orders"], "duckdb")).toBe("s.orders");
      expect(tableIdentityAt(["S", "Orders"], "postgres")).toBe("S.Orders");
   });

   it("a caller's drop accepts a mark only on the table it names", () => {
      const row = (comment: string) => [{ kind: "BASE TABLE", comment }];
      expect(markedFor(row(ownedTableComment(A, "s.t")), A, "s.t")).toEqual({
         state: "ours",
      });
      for (const comment of [
         ownedTableComment(A, "s.other"),
         ownedTableComment(A, "other.t"),
         ownedTableComment(A),
      ]) {
         expect(markedFor(row(comment), A, "s.t")).toEqual({
            state: "unmarked",
         });
      }
      expect(markedFor(row(ownedTableComment(B, "s.t")), A, "s.t").state).toBe(
         "taken",
      );
      // The build's reading is unchanged: the owner alone.
      expect(tableHolderOf(row(ownedTableComment(A, "s.other")), A)).toEqual({
         state: "ours",
      });
   });

   it("is written in each dialect's own table comment", () => {
      expect(ownedTableMarkSql(["s", "t"], A, "postgres")).toBe(
         `COMMENT ON TABLE "s"."t" IS 'malloy-publisher:table owner=env1%2Fpkg_a table=s.t'`,
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
         tables: {
            rollup: { kind: "rollup", rollup: {} },
            declared: {
               kind: "materialize",
               materialize: { persist: { name: "t" } },
            },
            defaulted: {
               kind: "materialize",
               materialize: {
                  persist: { name: "defaulted" },
                  nameDefaulted: true,
               },
            },
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
      // A #(materialize) name= is the author's; a defaulted one is not.
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

   let temp: TempDuckDB;
   beforeEach(() => {
      temp = openTempDuckDB("table-ownership-", "table_ownership", {
         file: "wh.duckdb",
         workingDirectory: true,
      });
      ({ dir, connection } = temp);
   });
   afterEach(() => temp.dispose());

   const target = { physicalTableName: "named_t", connectionName: "wh" };
   const check = (owner: string, ownRecords = false) =>
      assertTableOwnable(
         databaseTableSite(connection),
         target,
         owner,
         "named",
         async () => ownRecords,
      );

   it("a free name is built, and marked once built", async () => {
      await check(A);
      await run("CREATE TABLE named_t AS SELECT 1 AS x");
      await markOwnedTable(databaseTableSite(connection), "named_t", A);
      expect(await comment("named_t")).toBe(ownedTableComment(A, "named_t"));
      expect(
         await probeTableHolder(databaseTableSite(connection), "named_t", A),
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

   it("this package's own leftover name= view is its own, replaced by the table; any other view is refused and kept", async () => {
      // A view an earlier release made over a generation-named table, then
      // the package moved that source to `#@ persist name=` (unversioned).
      await run("CREATE TABLE g1 AS SELECT 1 AS x");
      await run("CREATE VIEW named_t AS SELECT * FROM g1");
      await run(
         `COMMENT ON VIEW named_t IS '${nameViewComment({ target: "g1", owner: A })}'`,
      );
      const site = databaseTableSite(connection);
      expect(await check(A)).toEqual({ ownView: true });
      // Another package never treats it as its own, nor drops it.
      expect((await check(B).catch((e) => e)).code).toBe(TABLE_OWNED_ELSEWHERE);
      expect(await dropOwnNameView(site, "named_t", B)).toBe(false);
      expect(await ownNameViewTarget(site, "named_t", A)).toBe("g1");
      // Its own package drops it, and the table takes the name.
      expect(await dropOwnNameView(site, "named_t", A)).toBe(true);
      expect(await check(A)).toEqual({ ownView: false });
      // The generation it read is left to the records that name it.
      expect((await run("SELECT x FROM g1")).rows).toEqual([{ x: 1 }]);
      // An unmarked view, even one reading this package's table, is refused
      // and never dropped.
      await run("CREATE VIEW named_t AS SELECT * FROM g1");
      expect((await check(A, true).catch((e) => e)).code).toBe(
         TABLE_OWNED_ELSEWHERE,
      );
      expect(await dropOwnNameView(site, "named_t", A)).toBe(false);
      // A table at the name is never dropped as a view.
      await run("DROP VIEW named_t");
      await run("CREATE TABLE named_t AS SELECT 3 AS x");
      await run(
         `COMMENT ON TABLE named_t IS '${nameViewComment({ target: "g1", owner: A })}'`,
      );
      expect(await dropOwnNameView(site, "named_t", A)).toBe(false);
      expect((await run("SELECT x FROM named_t")).rows).toEqual([{ x: 3 }]);
   });

   it("a name whose holder cannot be read is refused", async () => {
      const failing = {
         ...databaseTableSite(connection),
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
               { ...databaseTableSite(connection), dialect: "oracle" },
               target,
               A,
               "named",
               async () => true,
            ).catch((e) => e)
         ).code,
      ).toBe(TABLE_OWNERSHIP_UNVERIFIED);
   });

   it("a drop sees a table another package marked", async () => {
      const site = databaseTableSite(connection);
      await run("CREATE TABLE named_t AS SELECT 1 AS x");
      expect(await ownedElsewhere(site, "named_t", A)).toBeUndefined();
      await markOwnedTable(site, "named_t", B);
      expect(await ownedElsewhere(site, "named_t", A)).toBe(B);
      expect(await ownedElsewhere(site, "named_t", B)).toBeUndefined();
      expect(await ownedElsewhere(site, "no_such_table", A)).toBeUndefined();
   });

   it("a carried table at the author's name= is reused only while marked as this package's", async () => {
      const context: TableSiteContext = {
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
      await markOwnedTable(databaseTableSite(connection), "mine", A);
      await run("CREATE TABLE theirs AS SELECT 1 AS x");
      await markOwnedTable(databaseTableSite(connection), "theirs", B);
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

   let temp: TempDuckDB;
   beforeEach(async () => {
      temp = openTempDuckDB("table-ownership-lake-", "table_ownership_lake", {
         workingDirectory: true,
      });
      ({ dir, connection: session } = temp);
      await session.runSQL("INSTALL ducklake");
      await session.runSQL("LOAD ducklake");
      await session.runSQL(
         `ATTACH 'ducklake:${join(dir, "catalog.ducklake")}' AS lake ` +
            `(DATA_PATH '${join(dir, "data")}')`,
      );
   });
   afterEach(() => temp.dispose());

   it("a stored table is marked, read back, and refused to another package", async () => {
      const site = storageTableSite("lake", lake, session);
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
