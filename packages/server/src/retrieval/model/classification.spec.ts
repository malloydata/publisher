// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A field's `#(classification)` class rides its get_context entity, on the
 * source's own fields and on the copies reached through a join, and is never
 * mistaken for doc text: a field with a class and no `#(doc)` has an empty
 * description rather than the annotation line.
 */

import { beforeAll, describe, expect, it } from "bun:test";
import {
   compileModelFiles,
   storeServing,
} from "../../test_helpers/get_context_join_fixture";
import { projectEntity, toSourceResults } from "../search/response";
import type { Entity, PackageIndex, ResultEntity } from "./types";
import { docText, getPackageIndex } from "./package_entities";

const MODEL = `
source: patients is duckdb.sql("select 1 as id, '999-11-2222' as ssn, 'MA' as st") extend {
  #(doc) Social security number.
  #(classification) "phi.direct_identifier"
  dimension: social is ssn
  #(classification) "phi.quasi_identifier"
  dimension: state is st
  dimension: raw_state is st
  #(classification) "phi.financial"
  measure: patient_count is count()
  dimension: masked is concat('***', social)
  #(classification) "none"
  dimension: state_initial is substr(st, 1, 1)
}

source: encounters is duckdb.sql("select 1 as id, 1 as patient_id") extend {
  dimension: encounter_id is id
  join_one: patient is patients on patient.id = patient_id
}
`;

let index: PackageIndex;
beforeAll(async () => {
   const { pkg } = await compileModelFiles({ "m.malloy": MODEL });
   index = await getPackageIndex(storeServing(pkg), "env", "pkg");
});

function entity(source: string, name: string): Entity {
   // retrievalEntities holds the joined copies too; directEntities does not.
   const e = index.retrievalEntities.find(
      (c) => c.source === source && c.name === name,
   );
   if (!e) throw new Error(`no indexed entity ${source}.${name}`);
   return e;
}

describe("classification on get_context entities", () => {
   it("carries the class on the source's own dimensions and measures", () => {
      expect(entity("patients", "social").classifications).toEqual([
         "phi.direct_identifier",
      ]);
      expect(entity("patients", "patient_count").classifications).toEqual([
         "phi.financial",
      ]);
   });

   it("keeps the class when two spellings of one column fold into one entity", () => {
      // `state` and `raw_state` are both `st`; the fold keeps one spelling and
      // the column's class comes with it whichever that is.
      const kept = entity("patients", "raw_state");
      expect(kept.aliases).toContain("state");
      expect(kept.classifications).toEqual(["phi.quasi_identifier"]);
      expect(
         index.retrievalEntities.find(
            (c) => c.source === "patients" && c.name === "state",
         ),
      ).toBeUndefined();
   });

   it("infers a derived field's class and reports the inputs, on the card too", () => {
      const masked = entity("patients", "masked");
      expect(masked.classifications).toEqual(["phi.direct_identifier"]);
      expect(masked.classificationInferred).toBe(true);
      expect(masked.classificationInputs).toEqual([
         { field: "social", classifications: ["phi.direct_identifier"] },
      ]);
      const ranked: ResultEntity = {
         ...projectEntity(masked, "env", "pkg"),
         score: 1,
         targetScores: new Map([[0, 1]]),
      };
      const cards = toSourceResults(
         [ranked],
         index.sourceContext,
         "env",
         "pkg",
      );
      const card = cards.find(
         (c) => c.source_info.resource_id.source === "patients",
      );
      const entityCard = card?.entities?.find((e) => e.name === "masked");
      expect(entityCard?.classifications).toEqual(["phi.direct_identifier"]);
      expect(entityCard?.classification_inferred).toBe(true);
      expect(entityCard?.classification_inputs).toEqual([
         { field: "social", classifications: ["phi.direct_identifier"] },
      ]);
   });

   it("keeps a declared class over the inputs' and still shows them", () => {
      const initial = entity("patients", "state_initial");
      expect(initial.classifications).toEqual(["none"]);
      expect(initial.classificationInferred).toBeUndefined();
      // `st` itself carries no class; `state` (quasi) and `raw_state` fold into it.
      expect(initial.classificationInputs ?? []).toEqual([]);
   });

   it("keeps a classified field's description to its doc, empty when it has none", () => {
      expect(entity("patients", "social").doc).toBe("Social security number.");
      expect(entity("patients", "raw_state").doc).toBe("");
      expect(entity("patients", "raw_state").embedDoc).toBe("");
   });

   it("carries the class on the copy reached through a join", () => {
      expect(entity("encounters", "patient.social").classifications).toEqual([
         "phi.direct_identifier",
      ]);
      expect(entity("encounters", "patient.state").classifications).toEqual([
         "phi.quasi_identifier",
      ]);
      // Joined copies are not folded, so the unclassified spelling stays bare.
      expect(
         entity("encounters", "patient.raw_state").classifications,
      ).toBeUndefined();
   });

   it("projects it into the ranked row and the card", () => {
      const ranked: ResultEntity = {
         ...projectEntity(entity("patients", "social"), "env", "pkg"),
         score: 1,
         targetScores: new Map([[0, 1]]),
      };
      expect(ranked.classifications).toEqual(["phi.direct_identifier"]);
      const cards = toSourceResults(
         [ranked],
         index.sourceContext,
         "env",
         "pkg",
      );
      const card = cards.find(
         (c) => c.source_info.resource_id.source === "patients",
      );
      const social = card?.entities?.find((e) => e.name === "social");
      expect(social?.classifications).toEqual(["phi.direct_identifier"]);
      expect(social?.description).toBe("Social security number.");
   });
});

describe("docText and classification", () => {
   it("does not fall back to the classification line as a description", () => {
      expect(docText(['#(classification) "phi.clinical"'])).toBe("");
      expect(docText(['#(classification) "phi.clinical"', "# currency"])).toBe(
         "# currency",
      );
   });
});
