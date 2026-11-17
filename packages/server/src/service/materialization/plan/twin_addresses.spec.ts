// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// P2-21: the same SQL built in its own warehouse and with `storage=` (twins).
// Malloy groups them into one build target, so they shared one content
// address: a run's manifest kept one entry, and the colocated chain over the
// colocated twin found the stored twin's (or nothing) and missed strictly.
// Each `storage=` twin now folds its destination into its address, the
// colocated twin keeps the plain one, and a downstream source depends only on
// the twin its own definition reads. No other address moves.

import { openTempDuckDB } from "../../../test_helpers/temp_storage";
import {
   type BuildTarget,
   FixedConnectionMap,
   InMemoryURLReader,
   type PersistSource,
   Runtime,
} from "@malloydata/malloy";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../test_helpers/settings_env";
import {
   computeSourceEntityId,
   foldDistinctTableNames,
   planAddresses,
   plannedSources,
   plannedTargetOf,
   sourceBuildsOf,
} from "./addressing";

const MODEL = `##! experimental.persistence
source: ev is duckdb.table('ev')

#@ persist
source: col_a is ev -> { group_by: id; aggregate: n is count() }

#@ persist storage="lake"
source: twin_a is ev -> { group_by: id; aggregate: n is count() }

#@ persist
source: col_b is col_a -> { group_by: id; aggregate: m is n.sum() }

#@ persist storage="lake"
source: stored_b is twin_a -> { group_by: id; aggregate: m is n.sum() }

#@ persist
source: other is ev -> { group_by: id; aggregate: total is amount.sum() }
`;

const temp = openTempDuckDB("twin-addresses-", "duckdb", {
   file: "duckdb.db",
});
let sources: Record<string, PersistSource>;
let targets: Awaited<ReturnType<Runtime["getBuildTargets"]>>;
let digests: Record<string, string>;
let restore: (() => void) | undefined;

beforeAll(async () => {
   await temp.connection.runSQL(
      "CREATE TABLE ev AS SELECT * FROM (VALUES (1, 5.0), (2, 20.0)) AS t(id, amount)",
   );
   const runtime = new Runtime({
      urlReader: new InMemoryURLReader(new Map([["file:///m.malloy", MODEL]])),
      connections: new FixedConnectionMap(
         new Map([["duckdb", temp.connection]]),
         "duckdb",
      ),
   });
   const model = await runtime
      .loadModel(new URL("file:///m.malloy"))
      .getModel();
   targets = await runtime.getBuildTargets(model);
   sources = Object.fromEntries(
      plannedSources(targets).map((s) => [s.name, s]),
   );
   digests = { duckdb: await temp.connection.getDigest() };
});

afterEach(() => {
   restore?.();
   restore = undefined;
});

afterAll(async () => {
   await temp.dispose();
});

function withSettings(env: Record<string, string>): void {
   restore?.();
   restore = installSettings({ env }).restore;
}

function plan(of: Record<string, PersistSource> = sources) {
   const tables: Record<
      string,
      { source: PersistSource; target?: BuildTarget }
   > = {};
   for (const s of Object.values(of)) {
      tables[s.sourceID] = { source: s, target: plannedTargetOf(s) };
   }
   const base = { tables, connectionDigests: digests };
   const tableFolds = foldDistinctTableNames(base, () => []);
   const addresses = planAddresses({ ...base, tableFolds });
   const at = (name: string) => addresses[of[name].sourceID] as string;
   const plain = (name: string) => computeSourceEntityId(of[name], digests);
   return { tableFolds, at, plain };
}

describe("P2-21: twins built colocated and with storage=", () => {
   it("are one build target to Malloy (the shape that collided)", () => {
      expect(plannedTargetOf(sources.col_a)).toBe(
         plannedTargetOf(sources.twin_a),
      );
   });

   it("get distinct addresses; only the storage= twin moves", () => {
      withSettings({ PERSIST_STORAGE_MODE: "on" });
      const { at, plain, tableFolds } = plan();
      expect(plain("col_a")).toBe(plain("twin_a"));
      expect(at("twin_a")).not.toBe(at("col_a"));
      expect(at("col_a")).toBe(plain("col_a"));
      // `col_b` and `stored_b` inline to one SQL too: twins in turn.
      expect(Object.keys(tableFolds).sort()).toEqual(
         [sources.twin_a.sourceID, sources.stored_b.sourceID].sort(),
      );
      expect(at("stored_b")).not.toBe(at("col_b"));
      // No colocated source, and no source that is not a twin, moves.
      for (const name of ["col_a", "col_b", "other"]) {
         expect(at(name)).toBe(plain(name));
      }
   });

   it("move nothing while storage= is off (both build colocated)", () => {
      withSettings({ PERSIST_STORAGE_MODE: "off" });
      const { at, tableFolds } = plan();
      expect(tableFolds).toEqual({});
      expect(at("twin_a")).toBe(at("col_a"));
   });

   it("are already apart when the destination is in every address (P12)", () => {
      withSettings({
         PERSIST_STORAGE_MODE: "on",
         PUBLISHER_PERSIST_ADDRESS_INCLUDES_DESTINATION: "true",
      });
      const { at, plain, tableFolds } = plan();
      expect(tableFolds).toEqual({});
      expect(at("twin_a")).toBe(plain("twin_a"));
      expect(at("twin_a")).not.toBe(at("col_a"));
   });

   it("give each downstream only the twin it reads as an upstream", () => {
      const [build] = sourceBuildsOf(targets);
      expect(build.dependsOn[sources.col_b.sourceID]).toEqual([
         sources.col_a.sourceID,
      ]);
      expect(build.dependsOn[sources.stored_b.sourceID]).toEqual([
         sources.twin_a.sourceID,
      ]);
      expect(build.dependsOn[sources.other.sourceID]).toEqual([]);
   });
});

// The same twins with one `name=` on both: only where they build tells them
// apart, and still only the storage= twin moves.
describe("P2-21: twins that declare one name=", () => {
   let named: Record<string, PersistSource>;
   beforeAll(async () => {
      const text = MODEL.replace(
         "#@ persist\nsource: col_a",
         '#@ persist name="t_a"\nsource: col_a',
      ).replace(
         '#@ persist storage="lake"\nsource: twin_a',
         '#@ persist storage="lake" name="t_a"\nsource: twin_a',
      );
      expect(text).toContain('name="t_a"\nsource: twin_a');
      const runtime = new Runtime({
         urlReader: new InMemoryURLReader(
            new Map([["file:///named.malloy", text]]),
         ),
         connections: new FixedConnectionMap(
            new Map([["duckdb", temp.connection]]),
            "duckdb",
         ),
      });
      const model = await runtime
         .loadModel(new URL("file:///named.malloy"))
         .getModel();
      named = Object.fromEntries(
         plannedSources(await runtime.getBuildTargets(model)).map((s) => [
            s.name,
            s,
         ]),
      );
   });

   it("keep the colocated twin's address; only the storage= twin moves", () => {
      withSettings({ PERSIST_STORAGE_MODE: "on" });
      const { at, plain, tableFolds } = plan(named);
      expect(at("twin_a")).not.toBe(at("col_a"));
      expect(at("col_a")).toBe(plain("col_a"));
      expect(tableFolds[named.col_a.sourceID]).toBeUndefined();
      expect(tableFolds[named.twin_a.sourceID]).toBeDefined();
   });
});
