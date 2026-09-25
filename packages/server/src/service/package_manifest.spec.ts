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

      it("says nothing about the recommended shape: a root index.malloy and no keys", () => {
         for (const paths of [
            ["index.malloy", "orders.malloy"],
            ["index.malloy", "report.malloynb"],
            ["index.malloy"],
         ]) {
            expect(resolve(undefined, paths).warnings).toEqual([]);
         }
      });

      it("leaves a package with no index.malloy uncurated", () => {
         expect(
            resolve(undefined, ["orders.malloy", "internal.malloy"]).explores,
         ).toBeUndefined();
      });

      it("prefers an explicit explores, deprecates it, and says the index file is ignored", () => {
         const { explores, warnings } = resolve(
            ["orders.malloy"],
            ["index.malloy", "orders.malloy"],
         );
         expect(explores).toEqual(["orders.malloy"]);
         expect(warnings).toEqual([
            `"explores" in publisher.json is deprecated. Fix: import ` +
               `orders.malloy into index.malloy, export the sources you publish ` +
               `from it, then delete "explores".`,
            `index.malloy is ignored because "explores" in publisher.json ` +
               `doesn't list it. Fix: delete "explores" to publish what ` +
               `index.malloy exports, or rename index.malloy if it isn't meant ` +
               `to decide what is published.`,
         ]);
      });

      it("deprecates every explores, naming the files index.malloy must import", () => {
         // No index.malloy yet: add one that imports them all.
         expect(
            resolve(
               ["orders.malloy", "customers.malloy"],
               ["orders.malloy", "customers.malloy"],
            ).warnings,
         ).toEqual([
            `"explores" in publisher.json is deprecated. Fix: add an ` +
               `index.malloy at the package root that imports orders.malloy and ` +
               `customers.malloy and exports the sources you want to publish, ` +
               `then delete "explores".`,
         ]);
         // index.malloy and dashboards need no replacement: the file is the
         // surface and every dashboard is served.
         const alreadyPublished =
            `"explores" in publisher.json is deprecated. index.malloy already ` +
            `publishes the same thing. Fix: delete "explores".`;
         expect(
            resolve(
               ["index.malloy", "dashboards/overview.malloy"],
               ["index.malloy", "dashboards/overview.malloy"],
            ).warnings,
         ).toEqual([alreadyPublished]);
         expect(
            resolve(["index.malloy"], ["index.malloy", "orders.malloy"])
               .warnings,
         ).toEqual([alreadyPublished]);
         // Only dashboards listed, index.malloy on disk but left out.
         expect(
            resolve(
               ["dashboards/overview.malloy"],
               ["index.malloy", "dashboards/overview.malloy"],
            ).warnings[0],
         ).toBe(
            `"explores" in publisher.json is deprecated. Fix: delete ` +
               `"explores", so index.malloy decides what is published.`,
         );
         // Only dashboards listed, and no index.malloy at all.
         expect(
            resolve(
               ["dashboards/overview.malloy"],
               ["dashboards/overview.malloy"],
            ).warnings,
         ).toEqual([
            `"explores" in publisher.json is deprecated. Fix: add an ` +
               `index.malloy at the package root that exports the sources you ` +
               `want to publish, then delete "explores".`,
         ]);
      });

      it("deprecates the empty-array opt-out, which the file itself now replaces", () => {
         const { explores, warnings } = resolve([], ["index.malloy"]);
         expect(explores).toEqual([]);
         expect(warnings).toEqual([
            `"explores" in publisher.json is deprecated. Here it stops ` +
               `index.malloy from limiting what this package publishes. Fix: to ` +
               `publish everything, rename index.malloy and delete "explores".`,
         ]);
         // With no index.malloy there is nothing to suppress.
         expect(resolve([], ["orders.malloy"]).warnings).toEqual([
            `"explores": [] in publisher.json does nothing. Fix: delete it.`,
         ]);
      });

      it("only counts a root index.malloy, not a nested one", () => {
         expect(
            resolve(undefined, ["reports/index.malloy", "orders.malloy"])
               .explores,
         ).toBeUndefined();
      });

      it("says a root file named index.malloy in another case is ignored", () => {
         // Almost certainly meant as the surface, and silently uncurated. The
         // exact-match rule stays: a case-insensitive filesystem would
         // otherwise curate the same package on one machine and not another.
         const { explores, warnings } = resolve(undefined, [
            "Index.malloy",
            "orders.malloy",
         ]);
         expect(explores).toBeUndefined();
         expect(warnings).toEqual([
            `Index.malloy is ignored: only a root file named exactly ` +
               `index.malloy decides what is published. Fix: rename it to ` +
               `index.malloy.`,
         ]);
         // Not beside an exact index.malloy, not nested, and not under an
         // explores key, which decides the surface itself.
         expect(
            resolve(undefined, ["index.malloy", "INDEX.malloy"]).warnings,
         ).toEqual([]);
         expect(
            resolve(undefined, ["reports/Index.malloy", "orders.malloy"])
               .warnings,
         ).toEqual([]);
         expect(
            resolve(["Index.malloy"], ["Index.malloy"]).warnings.some((w) =>
               w.includes("is ignored"),
            ),
         ).toBe(false);
      });

      it("refuses to load a package whose explores is malformed, naming the value and the fix", () => {
         // `explores` decides what is REACHABLE, so a half-understood value is
         // not half-applied: ignoring it would publish every source the author
         // curated away.
         expect(() => resolve(["orders.malloy", 7], ["orders.malloy"])).toThrow(
            `Invalid "explores" in publisher.json: it must be a list of file ` +
               `names, but is ["orders.malloy",7]. The package was not loaded. ` +
               `Fix: delete "explores" and add an index.malloy.`,
         );
         expect(() => resolve("orders.malloy", ["orders.malloy"])).toThrow(
            /Invalid "explores"/,
         );
         expect(() => resolve([null], ["orders.malloy"])).toThrow(
            /Invalid "explores"/,
         );
      });

      it("keeps a well-formed empty array out of the refusal", () => {
         expect(() => resolve([], ["orders.malloy"])).not.toThrow();
      });

      it('tells a queryableSources "declared" author to delete it, and says nothing about "all"', () => {
         expect(
            resolve(undefined, ["index.malloy"], "declared").warnings,
         ).toEqual([
            `"queryableSources" in publisher.json does nothing. Fix: delete it.`,
         ]);
         // "all" is the one way to hide a source from listings while it stays
         // queryable by name, and nothing replaces it.
         expect(resolve(undefined, ["index.malloy"], "all").warnings).toEqual(
            [],
         );
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
