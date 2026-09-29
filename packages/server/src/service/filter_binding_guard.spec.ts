// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Unit-level coverage for `./filter_binding_guard`'s own exported helpers —
 * cases cheaper and more precise to prove directly against synthetic
 * `SourceDef`-shaped test doubles than by compiling a real Malloy model
 * (join-depth exhaustion) or that a single alias mismatch would otherwise
 * bury in a large integration fixture. Every other shape this
 * module covers (grafted gates, plain inherited `where:`, `#(filter)`
 * injection) is exercised end to end in `filter_binding_guard_integration.spec.ts`.
 */
import { describe, expect, it } from "bun:test";
import type {
   FieldDef,
   FilterCondition,
   ModelDef,
   SourceDef,
} from "@malloydata/malloy";
import {
   assertGraftedGateBindsToDeclaringSource,
   assertInheritedSourceFiltersBind,
   fieldPathIdentical,
   MAX_JOIN_RECURSION_DEPTH,
} from "./filter_binding_guard";

/** A joined `SourceDef`-shaped test double one level deep — `join` is what
 *  `isJoined` duck-types on, `type: "table"` is what `isSourceDef` does. */
function joinedLeaf(name: string, inner?: SourceDef): SourceDef {
   const fields: FieldDef[] = inner
      ? [
           {
              ...(inner as unknown as Record<string, unknown>),
              join: "one",
           } as unknown as FieldDef,
        ]
      : [];
   return {
      type: "table",
      name,
      dialect: "duckdb",
      connection: "duckdb",
      tablePath: name,
      fields,
      filterList: [],
      structRelationship: { type: "basetable", connectionName: "duckdb" },
   } as unknown as SourceDef;
}

/** A chain of `depth` nested `join_one:` hops, innermost first. */
function buildJoinChain(depth: number): SourceDef {
   let current = joinedLeaf("leaf");
   for (let i = 0; i < depth; i++) {
      current = joinedLeaf(`s${i}`, current);
   }
   return current;
}

describe("filter_binding_guard — assertInheritedSourceFiltersBind join-depth bound", () => {
   it("throws when the join chain exceeds MAX_JOIN_RECURSION_DEPTH (deny, not a silent stop)", () => {
      const chain = buildJoinChain(MAX_JOIN_RECURSION_DEPTH + 4);
      expect(() => assertInheritedSourceFiltersBind(chain, undefined)).toThrow(
         /join-depth resolution bound/,
      );
   });

   it("does not throw for a chain within the bound (negative control)", () => {
      const chain = buildJoinChain(MAX_JOIN_RECURSION_DEPTH - 2);
      expect(() =>
         assertInheritedSourceFiltersBind(chain, undefined),
      ).not.toThrow();
   });

   it("a genuine cycle (a struct reachable from itself) stops silently, not by throwing", () => {
      // `visited` is keyed by object identity, so a hand-built self-loop
      // (the same object nested as its own join target) is the cheapest way
      // to construct one without a real recursive Malloy model.
      const self: SourceDef = joinedLeaf("cyclic");
      (self.fields as unknown as FieldDef[]).push({
         ...(self as unknown as Record<string, unknown>),
         join: "one",
      } as unknown as FieldDef);
      expect(() =>
         assertInheritedSourceFiltersBind(self, undefined),
      ).not.toThrow();
   });
});

describe("filter_binding_guard — assertGraftedGateBindsToDeclaringSource entry-point resolution", () => {
   it("throws when the entry point struct could not be resolved (deny, not a silent no-op)", () => {
      const executed = joinedLeaf("executed");
      const condition = {
         code: "true",
         refSummary: { fieldUsage: [] },
      } as unknown as FilterCondition;
      const modelDef = { contents: {} } as unknown as ModelDef;
      expect(() =>
         assertGraftedGateBindsToDeclaringSource(
            undefined,
            executed,
            condition,
            modelDef,
         ),
      ).toThrow(/entry point could not be resolved/);
   });
});

describe("filter_binding_guard — activeName alias resolution", () => {
   /** A field with a real `.name` but a different DISPLAYED `.as` — the
    *  shape an aliased join member (`join_one: alias is real_join on ...`)
    *  takes. */
   function aliasedField(name: string, as: string): FieldDef {
      return {
         type: "number",
         name,
         as,
         e: { node: "field", path: [name] },
      } as unknown as FieldDef;
   }

   it("resolves a field by its ACTIVE (aliased) name, not its underlying .name", () => {
      const declaring: SourceDef = {
         ...joinedLeaf("declaring"),
         fields: [aliasedField("org_id", "tenant")],
      } as SourceDef;
      const executed: SourceDef = {
         ...joinedLeaf("executed"),
         fields: [aliasedField("org_id", "tenant")],
      } as SourceDef;
      // Looked up by the ALIAS ("tenant"), not the underlying physical name
      // ("org_id") — a lookup keyed on `.name` alone would fail to resolve
      // this path at all and read as "different field" (false deny) rather
      // than the legitimately-identical field it is.
      expect(fieldPathIdentical(declaring, executed, ["tenant"])).toBe(true);
   });

   it("still detects a genuine mismatch through an alias (negative control)", () => {
      const declaring: SourceDef = {
         ...joinedLeaf("declaring"),
         fields: [aliasedField("org_id", "tenant")],
      } as SourceDef;
      const executed: SourceDef = {
         ...joinedLeaf("executed"),
         fields: [aliasedField("owner", "tenant")],
      } as SourceDef;
      expect(fieldPathIdentical(declaring, executed, ["tenant"])).toBe(false);
   });
});
