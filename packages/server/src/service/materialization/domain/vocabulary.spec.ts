// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The hand-written kind and leg lists agree with the vocabulary's: checked
 * at compile time here, so a word added to one list and not the other fails
 * to compile. A union defined from the vocabulary (`BindingKind`,
 * `TableOrigin`, `LegKind`, `VariantKind`, the build and gate tiers) agrees
 * by construction and is not checked.
 */

import { describe, expect, it } from "bun:test";
import { BUILD_KINDS } from "../../../materialization_metrics";
import { GATE_KINDS, GATE_LEGS } from "./gate_policy";
import { LEGS, type Leg, TABLE_KINDS, type TableKind } from "./vocabulary";

type Exactly<A, B> = [A] extends [B] ? ([B] extends [A] ? true : never) : never;

const BUILD: Exactly<(typeof BUILD_KINDS)[number], TableKind> = true;
const GATE: Exactly<
   (typeof GATE_KINDS)[number],
   Exclude<TableKind, "query">
> = true;
const GATE_LEG: Exactly<
   (typeof GATE_LEGS)[number],
   Exclude<Leg, "query">
> = true;

describe("the materialization vocabulary", () => {
   it("holds every hand-written kind and leg list to the vocabulary's", () => {
      expect([BUILD, GATE, GATE_LEG]).toEqual([true, true, true]);
      expect([...TABLE_KINDS].sort()).toEqual([...BUILD_KINDS].sort());
      expect([...LEGS]).toEqual(["view", "query", "rollup", "served"]);
   });
});
