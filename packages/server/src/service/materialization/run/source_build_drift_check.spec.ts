// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What a chained build's drift check holds to its recorded shape before the
 * build reads a stored upstream's table:
 *
 * - {@link driftCheckedSourceIds}: a `#(materialize)` extension's parent,
 *   which the walk of the author's text never reaches (it names the parent,
 *   not the generated build source the build compiles). Without it, an
 *   extension was built over a parent table retyped since the parent's
 *   build, and served the wrong value.
 * - {@link storedUpstreamMove}: a `storage=` upstream's table, held to its
 *   contract as its destination reads it now, so a moved column is refused
 *   as `schema_incompatible` before the destination's engine meets it.
 */

import { describe, expect, it } from "bun:test";
import { contractFor } from "../../../test_helpers/contract_fixture";
import type { ManifestEntry, ObservedColumns } from "../domain/types";
import { storedUpstreamMove } from "./database_column_read";
import { driftCheckedSourceIds } from "./source_build";

describe("the sources a chained build's drift check holds", () => {
   const extension = {
      materializePlans: {
         ext: { buildsOver: { sourceID: "a", sourceName: "a" } },
      },
   } as never;

   it("adds an extension's parent to what the walk reached", () => {
      expect([...driftCheckedSourceIds(new Set(), "ext", extension)]).toEqual([
         "a",
      ]);
      expect([
         ...driftCheckedSourceIds(new Set(["raw_p"]), "ext", extension),
      ]).toEqual(["raw_p", "a"]);
   });

   it("is the walk alone for a source that extends no materialized parent", () => {
      const reached = new Set(["a"]);
      expect(driftCheckedSourceIds(reached, "b", extension)).toBe(reached);
      expect(driftCheckedSourceIds(reached, "b", {} as never)).toBe(reached);
   });
});

describe("a storage= upstream's table, held to its contract", () => {
   const entry = {
      contract: contractFor([
         { name: "k", type: "INTEGER" },
         { name: "amt", type: "DECIMAL(10,1)" },
      ]),
   } as Pick<ManifestEntry, "contract">;
   const read = (amt?: string): ObservedColumns => ({
      dialect: "duckdb",
      columns: [
         { name: "k", type: "number", nativeType: "INTEGER" },
         ...(amt ? [{ name: "amt", type: "string", nativeType: amt }] : []),
      ],
   });

   it("is unmoved while every column holds the type its build recorded", () => {
      expect(storedUpstreamMove(entry, read("DECIMAL(10,1)"))).toBeUndefined();
   });

   it("names a column retyped since the build, with both types", () => {
      expect(storedUpstreamMove(entry, read("VARCHAR"))).toEqual({
         kind: "moved",
         reason:
            "its column 'amt' was built as DECIMAL(10,1) and is now VARCHAR",
      });
   });

   it("names a column gone since the build", () => {
      expect(storedUpstreamMove(entry, read())).toEqual({
         kind: "moved",
         reason: "its column 'amt' is gone",
      });
   });

   it("counts a table it could not read as moved (fail safe)", () => {
      expect(
         storedUpstreamMove(entry, { unread: "catalog_query_failed" })?.kind,
      ).toBe("unreadable");
      expect(storedUpstreamMove(entry, undefined)?.kind).toBe("unreadable");
   });

   it("leaves an entry with no contract unjudged", () => {
      expect(storedUpstreamMove({}, read("VARCHAR"))).toBeUndefined();
   });
});
