// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// P12: a `storage=` source's destination folded into its content address,
// behind `PUBLISHER_PERSIST_ADDRESS_INCLUDES_DESTINATION`. Off, no address
// moves; on, exactly the `storage=` sources move, and only while
// `PERSIST_STORAGE_MODE` lets `storage=` take effect. The compiler's own key
// never moves: a manifest handed to the compiler carries both keys, as it
// already does for a partitioned source.

import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   makeDigest,
   type PersistSource,
   Runtime,
} from "@malloydata/malloy";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../test_helpers/settings_env";
import {
   addressDestination,
   addressSQL,
   compilerBuildId,
   computeSourceEntityId,
   plannedSources,
} from "./addressing";

const MODEL = `##! experimental.persistence
source: ev is duckdb.table('ev')

#@ persist name="colocated_t"
source: colocated is ev -> { group_by: id; aggregate: n is count() }

#@ persist name="stored_t" storage="lake"
source: stored is ev -> { group_by: id; aggregate: total is amount.sum() }

source: stored_ext is stored extend { dimension: big is total > 10 }
`;

const duckdb = new DuckDBConnection("duckdb");
let sources: Record<string, PersistSource>;
let digests: Record<string, string>;
let restore: (() => void) | undefined;

beforeAll(async () => {
   await duckdb.runSQL(
      "CREATE TABLE ev AS SELECT * FROM (VALUES (1, 5.0), (2, 20.0)) AS t(id, amount)",
   );
   const runtime = new Runtime({
      urlReader: new InMemoryURLReader(new Map([["file:///m.malloy", MODEL]])),
      connections: new FixedConnectionMap(
         new Map([["duckdb", duckdb]]),
         "duckdb",
      ),
   });
   const model = await runtime
      .loadModel(new URL("file:///m.malloy"))
      .getModel();
   const planned = plannedSources(await runtime.getBuildTargets(model));
   sources = Object.fromEntries(planned.map((s) => [s.name, s]));
   digests = { duckdb: await duckdb.getDigest() };
});

afterEach(() => {
   restore?.();
   restore = undefined;
});

afterAll(async () => {
   await duckdb.close();
});

function withSettings(env: Record<string, string>): void {
   restore?.();
   restore = installSettings({ env }).restore;
}

const address = (name: string) => computeSourceEntityId(sources[name], digests);
const plain = (name: string) =>
   makeDigest(digests.duckdb, addressSQL(sources[name]));

describe("P12: the storage destination in the content address", () => {
   it("moves nothing while the switch is off (the default)", () => {
      withSettings({ PERSIST_STORAGE_MODE: "on" });
      for (const name of Object.keys(sources)) {
         expect(addressDestination(sources[name])).toBeUndefined();
         expect(address(name)).toBe(plain(name));
      }
   });

   it("moves exactly the storage= sources while the switch is on", () => {
      withSettings({
         PERSIST_STORAGE_MODE: "on",
         PUBLISHER_PERSIST_ADDRESS_INCLUDES_DESTINATION: "true",
      });
      expect(address("colocated")).toBe(plain("colocated"));
      expect(addressDestination(sources.stored)).toBe("lake");
      expect(address("stored")).toBe(
         makeDigest(
            digests.duckdb,
            `${addressSQL(sources.stored)}\n-- stored in: lake`,
         ),
      );
      expect(address("stored")).not.toBe(plain("stored"));
   });

   it("gives an extension its base's table under the new address too", () => {
      withSettings({
         PERSIST_STORAGE_MODE: "on",
         PUBLISHER_PERSIST_ADDRESS_INCLUDES_DESTINATION: "true",
      });
      // `#@ persist` is inherited along extend, so the extension shares the
      // base's table; both move together and stay one address.
      expect(Object.keys(sources)).toContain("stored_ext");
      expect(address("stored_ext")).toBe(address("stored"));
   });

   it("folds the destination while writing only, and not while storage= is off", () => {
      withSettings({
         PERSIST_STORAGE_MODE: "write-only",
         PUBLISHER_PERSIST_ADDRESS_INCLUDES_DESTINATION: "true",
      });
      expect(address("stored")).not.toBe(plain("stored"));
      withSettings({
         PERSIST_STORAGE_MODE: "off",
         PUBLISHER_PERSIST_ADDRESS_INCLUDES_DESTINATION: "true",
      });
      // Off, storage= is inert and the source builds colocated.
      expect(addressDestination(sources.stored)).toBeUndefined();
      expect(address("stored")).toBe(plain("stored"));
   });

   it("never moves the compiler's own key", () => {
      withSettings({ PERSIST_STORAGE_MODE: "on" });
      const before = compilerBuildId(sources.stored, digests);
      withSettings({
         PERSIST_STORAGE_MODE: "on",
         PUBLISHER_PERSIST_ADDRESS_INCLUDES_DESTINATION: "true",
      });
      expect(compilerBuildId(sources.stored, digests)).toBe(before);
   });
});

// A `#@ preaggregate` rollup is synthesized into a companion model, so the
// author-model corpus never plans one; this pins that a rollup's own
// `storage=` folds into its address like any `storage=` source, and that a
// colocated rollup beside it stays put.
