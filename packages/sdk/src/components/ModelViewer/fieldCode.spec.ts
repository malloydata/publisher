// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { makeGetFieldCode } from "./fieldCode";
import { groupEntitiesBySource, type FieldEntity } from "./fieldEntities";

const entity = (
   over: Partial<FieldEntity> & Pick<FieldEntity, "name">,
): FieldEntity => ({
   fieldType: "dimension",
   ...over,
});

describe("groupEntitiesBySource", () => {
   it("keeps same-named fields of different sources apart", () => {
      // The whole reason the map is not keyed by bare field name: a model that
      // declares `aircraft` and `aircraft_models` has an `id` in each, and a
      // flat map would render one source's definition on the other's field.
      const bySource = groupEntitiesBySource([
         entity({ name: "id", source: "aircraft", code: "-- aircraft id" }),
         entity({ name: "id", source: "aircraft_models", code: "-- model id" }),
      ]);

      expect(bySource.get("aircraft")?.get("id")?.code).toBe("-- aircraft id");
      expect(bySource.get("aircraft_models")?.get("id")?.code).toBe(
         "-- model id",
      );
   });

   it("drops entities with no declaring source rather than guessing one", () => {
      // `source` is absent on artifacts predating its capture. Attributing such
      // an entity to an arbitrary source would show the wrong code.
      const bySource = groupEntitiesBySource([
         entity({ name: "orphan", code: "x" }),
         entity({ name: "kept", source: "s", code: "y" }),
      ]);

      expect([...bySource.keys()]).toEqual(["s"]);
   });
});

describe("makeGetFieldCode", () => {
   it("suppresses an inherited column's code and marks it inherited", () => {
      // A column entity's stored slice is the SOURCE's declaring line, which
      // reads as nonsense on the field — the row must say "inherited" instead.
      const get = makeGetFieldCode({
         entities: new Map([
            [
               "id",
               entity({
                  name: "id",
                  source: "a",
                  fieldType: "column",
                  code: "duckdb.table('x')",
               }),
            ],
         ]),
         loading: false,
         error: false,
      });

      expect(get({ name: "id" })).toMatchObject({
         code: undefined,
         inherited: true,
      });
   });

   it("returns a declared field's code verbatim", () => {
      const get = makeGetFieldCode({
         entities: new Map([
            ["total", entity({ name: "total", source: "a", code: "sum(x)" })],
         ]),
         loading: false,
         error: false,
      });

      expect(get({ name: "total" })).toMatchObject({
         code: "sum(x)",
         inherited: false,
      });
   });

   it("never reports loading once the fetch has errored", () => {
      // An error carries no evidence of absence; showing a spinner next to a
      // failed fetch would read as "still coming".
      const get = makeGetFieldCode({
         entities: undefined,
         loading: true,
         error: true,
      });

      expect(get({ name: "anything" })).toMatchObject({
         loading: false,
         error: true,
      });
   });

   it("describes an inherited column from the table's generated descriptions", () => {
      const get = makeGetFieldCode({
         entities: new Map([
            ["id", entity({ name: "id", source: "a", fieldType: "column" })],
         ]),
         loading: false,
         error: false,
         columnDescriptions: new Map([
            ["id", "Primary key of the orders table."],
         ]),
      });

      expect(get({ name: "id" }).columnDoc).toBe(
         "Primary key of the orders table.",
      );
   });

   it("withholds a column description from a field the model defines", () => {
      // A same-named computed dimension is not that column: `state is
      // upper(state_code)` would otherwise inherit the description of a
      // `state` column it doesn't read.
      const get = makeGetFieldCode({
         entities: new Map([
            [
               "state",
               entity({
                  name: "state",
                  source: "a",
                  code: "upper(state_code)",
               }),
            ],
         ]),
         loading: false,
         error: false,
         columnDescriptions: new Map([["state", "Two-letter US state code."]]),
      });

      expect(get({ name: "state" }).columnDoc).toBeUndefined();
   });

   it("gives a joined source's field the same shape as the active source's", () => {
      // The parity this factory exists to guarantee: the joined section must
      // not render a field differently from that source's own page.
      const fields = new Map([
         [
            "manufacturer",
            entity({
               name: "manufacturer",
               source: "aircraft_models",
               fieldType: "column",
            }),
         ],
      ]);
      const asActive = makeGetFieldCode({
         entities: fields,
         loading: false,
         error: false,
      });
      const asJoined = makeGetFieldCode({
         entities: fields,
         loading: false,
         error: false,
      });

      const a = asActive({ name: "manufacturer" });
      const b = asJoined({ name: "manufacturer" });
      expect(a).toEqual(b);
      expect(a.inherited).toBe(true);
   });
});
