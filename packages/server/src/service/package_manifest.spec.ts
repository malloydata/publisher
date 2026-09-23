// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   materializationWithQueryMetadata,
   parsePackageMaterialization,
   parsePackageScope,
   queryMetadataParseWarnings,
   resolveExplores,
   resolvePackageQueryMetadata,
   resolvePackageScope,
} from "./package_manifest";

describe("service/package_manifest", () => {
   describe("resolveExplores", () => {
      const resolve = (
         declaredExplores: unknown,
         modelPaths: readonly string[],
         declaredQueryableSources?: unknown,
      ) =>
         resolveExplores({
            declaredExplores,
            declaredQueryableSources,
            modelPaths,
         });

      it("defaults the surface to a root index.malloy when no explores is declared", () => {
         expect(
            resolve(undefined, ["index.malloy", "orders.malloy"]).explores,
         ).toEqual(["index.malloy"]);
      });

      it("says so when the convention curates a package on the file alone", () => {
         // The one path that arms curation with nothing in publisher.json
         // asking for it, so it is the one an upgrading package meets by
         // surprise. It must name the consequence and the opt-out.
         const text = resolve(undefined, [
            "index.malloy",
            "orders.malloy",
         ]).warnings.join("\n");
         expect(text).toContain("is its published surface");
         expect(text).toContain("404");
         expect(text).toContain('"explores": []');
         // Renaming is NOT an opt-out -- it widens the surface silently and
         // breaks every import naming the file. RELEASE_NOTES says so; the
         // load-time message must not steer the other way.
         expect(text).not.toMatch(/or rename the file/);
         expect(text).toContain("Do NOT rename or delete the file");
      });

      it("counts models, not notebooks, when deciding something is withheld", () => {
         // filterModelPaths also yields .malloynb, and a notebook is always
         // listed and never subject to the boundary, so this package hides
         // nothing and must stay quiet.
         expect(
            resolve(undefined, ["index.malloy", "report.malloynb"]).warnings,
         ).toEqual([]);
      });

      it("stays quiet when the index.malloy is the only model", () => {
         // Nothing is withheld, so there is nothing to report.
         expect(resolve(undefined, ["index.malloy"]).warnings).toEqual([]);
      });

      it("leaves a package with no index.malloy uncurated", () => {
         expect(
            resolve(undefined, ["orders.malloy", "internal.malloy"]).explores,
         ).toBeUndefined();
      });

      it("prefers an explicit explores and says the index file is left out", () => {
         const { explores, warnings } = resolve(
            ["orders.malloy"],
            ["index.malloy", "orders.malloy"],
         );
         expect(explores).toEqual(["orders.malloy"]);
         // Names the file and the declared set, so the author can see which of
         // the two they wrote is actually in force.
         const omission = warnings.find((w) =>
            w.startsWith("This package has an"),
         );
         expect(omission).toContain("index.malloy");
         expect(omission).toContain('["orders.malloy"]');
      });

      it("treats an empty explores as a deliberate opt-out that suppresses the convention", () => {
         const { explores, warnings } = resolve([], ["index.malloy"]);
         expect(explores).toEqual([]);
         // Nothing is hidden by an empty surface, so there is no disagreement
         // to report.
         expect(warnings.some((w) => w.startsWith("This package has an"))).toBe(
            false,
         );
         // And this author must NOT be told to delete the key: beside an
         // index.malloy the empty array is the documented opt-out, so deleting
         // it would curate and bound the package they asked to leave open.
         const text = warnings.join("\n");
         expect(text).toContain("keeping this package uncurated");
         expect(text).toContain("do NOT delete the key");
         expect(text).not.toContain("then delete the key");
      });

      it("treats an empty explores as the opt-out with no index.malloy too", () => {
         // There is no convention to suppress here, but it is still the
         // supported "do not curate" state. The deprecation must not reach it:
         // its advice ends "it curates and enforces exactly as the key does",
         // which is false of an empty array, so following it would curate a
         // package the author deliberately left open.
         const text = resolve([], ["orders.malloy"]).warnings.join("\n");
         expect(text).toContain("keeping this package uncurated");
         expect(text).not.toContain("then delete the key");
      });

      it("only counts a root index.malloy, not a nested one", () => {
         expect(
            resolve(undefined, ["reports/index.malloy", "orders.malloy"])
               .explores,
         ).toBeUndefined();
      });

      it("refuses to load a package whose explores is malformed", () => {
         // `explores` decides what is REACHABLE, so a half-understood value is
         // not half-applied. Ignoring it would resolve to no surface at all and
         // publish every source the author curated away, which is the one
         // direction this key must never fail in.
         expect(() => resolve("orders.malloy", ["orders.malloy"])).toThrow(
            /Invalid "explores"/,
         );
         // A single bad element condemns the array: keeping the rest would
         // serve a surface the author did not write.
         expect(() => resolve(["orders.malloy", 7], ["orders.malloy"])).toThrow(
            /Invalid "explores"/,
         );
         expect(() => resolve([null], ["orders.malloy"])).toThrow();
      });

      it("names the value and the fix when it refuses", () => {
         expect(() => resolve(["orders.malloy", 7], ["orders.malloy"])).toThrow(
            /expected an array of model paths, got \["orders.malloy",7\]/,
         );
         expect(() => resolve("orders.malloy", ["orders.malloy"])).toThrow(
            /Fix: "explores": \["index.malloy"\]/,
         );
      });

      it("keeps a well-formed empty array out of the refusal", () => {
         // [] is the documented opt-out, not a malformation.
         expect(() => resolve([], ["orders.malloy"])).not.toThrow();
      });

      it("tells a queryableSources author to delete it, but never tells the 'all' author that", () => {
         const declared = resolve(undefined, ["index.malloy"], "declared");
         expect(declared.warnings.join("\n")).toContain("Delete it.");

         // "all" is the one thing the convention cannot express: it curates
         // listings without refusing queries. With no replacement there is
         // nothing to deprecate it in favor of, so it gets no notice at all.
         const all = resolve(
            ["orders.malloy", "customers.malloy"],
            ["orders.malloy", "customers.malloy"],
            "all",
         );
         expect(all.warnings).toEqual([]);
      });

      it("names a replacement only for a one-file explores", () => {
         const one = resolve(["orders.malloy"], ["orders.malloy"]);
         expect(one.warnings.join("\n")).toContain(
            '"explores" in publisher.json is deprecated',
         );

         // Several files is what the convention cannot express, and it is how
         // dashboards are served beside an index.malloy, so it is not nagged.
         const several = resolve(
            ["index.malloy", "dashboards/overview.malloy"],
            ["index.malloy", "dashboards/overview.malloy"],
         );
         expect(several.warnings).toEqual([]);
      });
   });

   describe("resolvePackageScope", () => {
      it("reads the canonical materialization.scope with no warning", () => {
         expect(resolvePackageScope(undefined, { scope: "version" })).toEqual({
            scope: "version",
            warnings: [],
         });
      });

      it("defaults to 'package' when neither home declares it", () => {
         expect(resolvePackageScope(undefined, undefined)).toEqual({
            scope: "package",
            warnings: [],
         });
         expect(resolvePackageScope(undefined, { freshness: {} })).toEqual({
            scope: "package",
            warnings: [],
         });
      });

      it("still honors a root scope, with a deprecation warning", () => {
         const resolved = resolvePackageScope("version", undefined);
         expect(resolved.scope).toBe("version");
         expect(resolved.warnings).toHaveLength(1);
         expect(resolved.warnings[0]).toMatch(/deprecated/i);
      });

      it("accepts both homes agreeing without a warning", () => {
         // The transition state the server writes: envelope for this build, root
         // for an older publisher. Warning would blame the operator for it.
         expect(resolvePackageScope("version", { scope: "version" })).toEqual({
            scope: "version",
            warnings: [],
         });
      });

      it("throws when the two homes disagree rather than guessing", () => {
         expect(() =>
            resolvePackageScope("package", { scope: "version" }),
         ).toThrow(/conflicting/i);
      });

      it("reports an invalid value as invalid, not as a conflict", () => {
         expect(() =>
            resolvePackageScope(undefined, { scope: "shared" }),
         ).toThrow(/expected "version" or "package"/i);
         expect(() =>
            resolvePackageScope("shared", { scope: "version" }),
         ).toThrow(/expected "version" or "package"/i);
      });
   });

   describe("parsePackageScope", () => {
      it("defaults to 'package' when absent", () => {
         expect(parsePackageScope(undefined)).toBe("package");
         expect(parsePackageScope(null)).toBe("package");
      });

      it("accepts the two valid modes verbatim", () => {
         expect(parsePackageScope("version")).toBe("version");
         expect(parsePackageScope("package")).toBe("package");
      });

      it("throws on any other value (no silent default)", () => {
         expect(() => parsePackageScope("shared")).toThrow(/scope/i);
         expect(() => parsePackageScope("")).toThrow(/scope/i);
         expect(() => parsePackageScope(7)).toThrow(/scope/i);
      });
   });

   describe("parsePackageMaterialization", () => {
      it("extracts a string schedule", () => {
         expect(parsePackageMaterialization({ schedule: "0 6 * * *" })).toEqual(
            { schedule: "0 6 * * *", freshness: null, queryMetadata: null },
         );
      });

      it("returns null when the block is absent", () => {
         expect(parsePackageMaterialization(undefined)).toBeNull();
         expect(parsePackageMaterialization(null)).toBeNull();
      });

      it("ignores extra/unknown fields", () => {
         expect(
            parsePackageMaterialization({
               schedule: "*/15 * * * *",
               retries: 3,
            }),
         ).toEqual({
            schedule: "*/15 * * * *",
            freshness: null,
            queryMetadata: null,
         });
      });

      it("degrades a non-string schedule to null", () => {
         expect(parsePackageMaterialization({ schedule: 42 })).toEqual({
            schedule: null,
            freshness: null,
            queryMetadata: null,
         });
         expect(parsePackageMaterialization({})).toEqual({
            schedule: null,
            freshness: null,
            queryMetadata: null,
         });
      });

      it("returns null for non-object input", () => {
         expect(parsePackageMaterialization("0 6 * * *")).toBeNull();
         expect(parsePackageMaterialization(7)).toBeNull();
      });

      it("extracts a full freshness block verbatim", () => {
         expect(
            parsePackageMaterialization({
               freshness: { window: "24h", fallback: "stale_ok" },
            }),
         ).toEqual({
            schedule: null,
            freshness: { window: "24h", fallback: "stale_ok" },
            queryMetadata: null,
         });
      });

      it("keeps a partial freshness block (window only)", () => {
         expect(
            parsePackageMaterialization({ freshness: { window: "1h" } }),
         ).toEqual({
            schedule: null,
            freshness: { window: "1h" },
            queryMetadata: null,
         });
      });

      it("drops invalid freshness fields rather than defaulting them", () => {
         // A bad value is reported as absent — never substituted — so the
         // control plane sees exactly what was (validly) declared.
         expect(
            parsePackageMaterialization({
               freshness: { window: 24, fallback: "retry" },
            }),
         ).toEqual({ schedule: null, freshness: {}, queryMetadata: null });
      });

      it("degrades a non-object freshness to null", () => {
         expect(parsePackageMaterialization({ freshness: "24h" })).toEqual({
            schedule: null,
            freshness: null,
            queryMetadata: null,
         });
      });

      it("keeps queryMetadata properties verbatim", () => {
         expect(
            parsePackageMaterialization({
               queryMetadata: { team: "finance", workload: "marts" },
            }),
         ).toEqual({
            schedule: null,
            freshness: null,
            queryMetadata: { team: "finance", workload: "marts" },
         });
      });

      it("keeps a contract-violating property so publish can report it", () => {
         // Filtering here would make the author's typo disappear with nothing
         // to point at; the validator warns and the runtime clamps instead.
         expect(
            parsePackageMaterialization({
               queryMetadata: { "team.name": "finance" },
            }),
         ).toEqual({
            schedule: null,
            freshness: null,
            queryMetadata: { "team.name": "finance" },
         });
      });

      it("drops non-string values and empties to null", () => {
         expect(
            parsePackageMaterialization({
               queryMetadata: { team: "finance", retries: 3 },
            }),
         ).toEqual({
            schedule: null,
            freshness: null,
            queryMetadata: { team: "finance" },
         });
         expect(
            parsePackageMaterialization({ queryMetadata: { retries: 3 } }),
         ).toEqual({ schedule: null, freshness: null, queryMetadata: null });
         expect(
            parsePackageMaterialization({ queryMetadata: "team=finance" }),
         ).toEqual({ schedule: null, freshness: null, queryMetadata: null });
      });
   });

   describe("resolvePackageQueryMetadata", () => {
      it("reads the canonical root home with no warning", () => {
         expect(
            resolvePackageQueryMetadata({ team: "finance" }, undefined),
         ).toEqual({
            queryMetadata: { team: "finance" },
            home: "root",
            warnings: [],
         });
      });

      it("still honors the enveloped form, with a deprecation warning", () => {
         const resolved = resolvePackageQueryMetadata(undefined, {
            queryMetadata: { team: "finance" },
         });
         expect(resolved.queryMetadata).toEqual({ team: "finance" });
         // The home is carried so a parse warning about one of these properties
         // names the spelling this author actually wrote.
         expect(resolved.home).toBe("envelope");
         expect(resolved.warnings).toHaveLength(1);
         expect(resolved.warnings[0]).toMatch(/deprecated/i);
      });

      it("accepts both homes agreeing without a warning", () => {
         // The transition state the server writes itself: the root for this
         // build, the envelope for an older publisher that reads only that.
         expect(
            resolvePackageQueryMetadata(
               { team: "finance" },
               { queryMetadata: { team: "finance" } },
            ),
         ).toEqual({
            queryMetadata: { team: "finance" },
            home: "root",
            warnings: [],
         });
      });

      it("treats the same bag in a different key order as agreement", () => {
         // Key order is not meaning. Comparing serialized forms directly called
         // these a conflict and sent the author off to reconcile two homes that
         // already agree — the root wins anyway, so the warning was pure noise
         // pointing at a bag that needs no edit.
         const resolved = resolvePackageQueryMetadata(
            { team: "finance", tier: "gold" },
            { queryMetadata: { tier: "gold", team: "finance" } },
         );
         expect(resolved.warnings).toEqual([]);
         expect(resolved.queryMetadata).toEqual({
            team: "finance",
            tier: "gold",
         });
      });

      it("still calls genuinely different bags a conflict", () => {
         // The order-insensitive compare must not swallow a real disagreement.
         expect(
            resolvePackageQueryMetadata(
               { team: "finance", tier: "gold" },
               { queryMetadata: { tier: "bronze", team: "finance" } },
            ).warnings[0],
         ).toMatch(/conflicting/i);
         // Same keys, one missing on the other side.
         expect(
            resolvePackageQueryMetadata(
               { team: "finance", tier: "gold" },
               { queryMetadata: { team: "finance" } },
            ).warnings[0],
         ).toMatch(/conflicting/i);
      });

      it("warns and prefers the root when the homes disagree", () => {
         // Deliberately unlike `scope`, which throws. Scope decides whether an
         // artifact is version-owned; a tag decides a label. Failing a package
         // load over a label would break the rule the feature rests on.
         const resolved = resolvePackageQueryMetadata(
            { team: "finance" },
            { queryMetadata: { team: "marketing" } },
         );
         expect(resolved.queryMetadata).toEqual({ team: "finance" });
         expect(resolved.warnings[0]).toMatch(/conflicting/i);
      });

      it("declares nothing when neither home does", () => {
         expect(resolvePackageQueryMetadata(undefined, undefined)).toEqual({
            queryMetadata: undefined,
            home: "root",
            warnings: [],
         });
         expect(
            resolvePackageQueryMetadata(undefined, { schedule: "0 6 * * *" }),
         ).toEqual({ queryMetadata: undefined, home: "root", warnings: [] });
      });
   });

   describe("queryMetadataParseWarnings", () => {
      it("names the home the author actually wrote", () => {
         // Hardcoding the enveloped spelling pointed the author of a root-only
         // manifest at a `materialization` block their file does not have.
         expect(queryMetadataParseWarnings({ team: 3 }, "root")[0]).toContain(
            "queryMetadata: property 'team'",
         );
         expect(
            queryMetadataParseWarnings({ team: 3 }, "envelope")[0],
         ).toContain("materialization.queryMetadata: property 'team'");
      });
   });

   describe("materializationWithQueryMetadata", () => {
      it("builds a config for a manifest with tags and no build policy", () => {
         // The shape the canonical form produces: a package that declares tags
         // at the root has no `materialization` block to put them in, and its
         // tags must not be lost for want of one.
         expect(
            materializationWithQueryMetadata(null, { team: "finance" }),
         ).toEqual({
            schedule: null,
            freshness: null,
            queryMetadata: { team: "finance" },
         });
      });

      it("keeps the build policy while replacing the tags", () => {
         expect(
            materializationWithQueryMetadata(
               parsePackageMaterialization({
                  schedule: "0 6 * * *",
                  queryMetadata: { team: "marketing" },
               }),
               { team: "finance" },
            ),
         ).toEqual({
            schedule: "0 6 * * *",
            freshness: null,
            queryMetadata: { team: "finance" },
         });
      });

      it("is null only when neither a policy nor a tag exists", () => {
         expect(materializationWithQueryMetadata(null, undefined)).toBeNull();
      });
   });
});
