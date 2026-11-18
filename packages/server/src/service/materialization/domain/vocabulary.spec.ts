// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Every kind, tier and leg union is the vocabulary's, or a named part of
 * it: checked at compile time here, so a word added to one list and not
 * the other fails to compile.
 */

import { describe, expect, it } from "bun:test";
import { BUILD_KINDS, BUILD_TIERS } from "../../../materialization_metrics";
import type { VariantKind } from "../../query_router";
import type { LegKind } from "../serve/leg";
import type { BindingKind } from "./binding";
import { GATE_KINDS, GATE_LEGS, GATE_TIERS } from "./gate_policy";
import type { TableOrigin } from "./table_plans";
import {
   LEGS,
   type Leg,
   TABLE_KINDS,
   type TableKind,
   TIERS,
} from "./vocabulary";

type Exactly<A, B> = [A] extends [B] ? ([B] extends [A] ? true : never) : never;

const BINDING: Exactly<BindingKind, Exclude<TableKind, "pair">> = true;
const ORIGIN: Exactly<TableOrigin, Exclude<TableKind, "pair">> = true;
const BUILD: Exactly<(typeof BUILD_KINDS)[number], TableKind> = true;
const GATE: Exactly<
   (typeof GATE_KINDS)[number],
   Exclude<TableKind, "query">
> = true;
const LEG: Exactly<LegKind, Leg> = true;
const VARIANT: Exactly<VariantKind, Leg> = true;
const GATE_LEG: Exactly<
   (typeof GATE_LEGS)[number],
   Exclude<Leg, "query">
> = true;

describe("the materialization vocabulary", () => {
   it("derives every kind, tier and leg union from one list", () => {
      expect([BINDING, ORIGIN, BUILD, GATE, LEG, VARIANT, GATE_LEG]).toEqual(
         Array(7).fill(true),
      );
      expect([...TABLE_KINDS].sort()).toEqual([...BUILD_KINDS].sort());
      expect([...GATE_TIERS]).toEqual([...TIERS]);
      expect([...BUILD_TIERS]).toEqual([...TIERS]);
      expect([...LEGS]).toEqual(["view", "query", "rollup", "served"]);
   });
});
