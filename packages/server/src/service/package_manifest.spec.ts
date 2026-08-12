import { describe, expect, it } from "bun:test";
import {
   packageMaterializationWarnings,
   parsePackageMaterialization,
   parsePackageScope,
   resolveExplores,
   resolvePackageScope,
   resolvePatchedExploresOrigin,
} from "./package_manifest";

describe("service/package_manifest", () => {
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

   describe("packageMaterializationWarnings", () => {
      it("names a dropped non-string value", () => {
         // The drop happens before the config validation that would otherwise
         // report it, so without this an unquoted `"team": 123` — a plausible
         // hand-edit — vanishes with nothing anywhere to point at.
         const warnings = packageMaterializationWarnings({
            queryMetadata: { team: "finance", retries: 3 },
         });
         expect(warnings).toHaveLength(1);
         expect(warnings[0]).toContain("'retries'");
         expect(warnings[0]).toContain("got number");
      });

      it("says nothing about a block that parses cleanly", () => {
         expect(
            packageMaterializationWarnings({
               schedule: "0 6 * * *",
               queryMetadata: { team: "finance" },
            }),
         ).toEqual([]);
         expect(packageMaterializationWarnings(undefined)).toEqual([]);
      });
   });

   describe("resolveExplores", () => {
      const WITH_INDEX = ["index.malloy", "internal.malloy"];
      const WITHOUT_INDEX = ["orders.malloy", "internal.malloy"];
      const resolve = (
         declaredExplores: unknown,
         modelPaths: readonly string[],
         declaredQueryableSources: unknown = undefined,
      ) =>
         resolveExplores({
            declaredExplores,
            declaredQueryableSources,
            modelPaths,
         });

      it("defaults to index.malloy when the manifest declares no explores", () => {
         expect(resolve(undefined, WITH_INDEX)).toEqual({
            explores: ["index.malloy"],
            fromConvention: true,
            warnings: [],
         });
      });

      it("leaves a package with no index.malloy uncurated", () => {
         expect(resolve(undefined, WITHOUT_INDEX)).toEqual({
            explores: undefined,
            fromConvention: false,
            warnings: [],
         });
      });

      it("lets an explicit explores win over the convention", () => {
         const resolved = resolve(["orders.malloy"], WITH_INDEX);
         expect(resolved.explores).toEqual(["orders.malloy"]);
         expect(resolved.fromConvention).toBe(false);
      });

      it("warns when an explicit explores disagrees with the convention", () => {
         const { warnings } = resolve(["orders.malloy"], WITH_INDEX);
         expect(warnings).toHaveLength(1);
         expect(warnings[0]).toContain("does not list it");
         // The warning must say which side won, or it is not actionable.
         expect(warnings[0]).toContain("explicit key wins");
      });

      it("warns that an explicit explores naming index.malloy is now redundant", () => {
         const { warnings } = resolve(["index.malloy"], WITH_INDEX);
         expect(warnings).toHaveLength(1);
         expect(warnings[0]).toContain("you can delete it");
      });

      it("stays silent for a curated package that has no index.malloy", () => {
         // The deprecation must not fire where the convention offers no
         // replacement: a multi-file surface cannot be expressed by it, so a
         // warning here would have no available fix.
         expect(
            resolve(["orders.malloy", "secured.malloy"], WITHOUT_INDEX),
         ).toEqual({
            explores: ["orders.malloy", "secured.malloy"],
            fromConvention: false,
            warnings: [],
         });
      });

      it("treats an explicit empty explores as explicit, not as absent", () => {
         // `explores: []` means uncurated today. Letting the convention curate
         // it would change served behavior off a config the author did write.
         expect(resolve([], WITH_INDEX)).toEqual({
            explores: [],
            fromConvention: false,
            warnings: [],
         });
      });

      it("normalizes declared entries the same way listPackageFiles does", () => {
         expect(resolve(["./index.malloy"], WITH_INDEX).explores).toEqual([
            "index.malloy",
         ]);
         // And the normalized form is what the redundancy check compares, so a
         // "./"-prefixed entry is recognized as naming the convention file.
         expect(resolve(["./index.malloy"], WITH_INDEX).warnings[0]).toContain(
            "you can delete it",
         );
      });

      it("ignores a nested index.malloy", () => {
         expect(resolve(undefined, ["reports/index.malloy"])).toEqual({
            explores: undefined,
            fromConvention: false,
            warnings: [],
         });
      });

      it("reports queryableSources as inert when the convention drives", () => {
         const { warnings, fromConvention } = resolve(
            undefined,
            WITH_INDEX,
            "declared",
         );
         expect(fromConvention).toBe(true);
         expect(warnings).toHaveLength(1);
         expect(warnings[0]).toContain("has no effect");
         // The remedy must be the one that actually produces a boundary.
         expect(warnings[0]).toContain('Add an explicit "explores"');
      });

      it("gives 'all' a remedy that would actually work", () => {
         // "all" already means no boundary, so telling this author to declare
         // explores would send them to a state that still does not enforce.
         const { warnings } = resolve(undefined, WITH_INDEX, "all");
         expect(warnings).toHaveLength(1);
         expect(warnings[0]).not.toContain('Add an explicit "explores"');
         expect(warnings[0]).toContain("you can delete it");
         expect(warnings[0]).toContain('"queryableSources": "declared"');
      });

      it("says nothing about queryableSources alongside an explicit explores", () => {
         // There it is fully meaningful, so there is nothing to warn about.
         expect(
            resolve(["orders.malloy"], WITHOUT_INDEX, "declared").warnings,
         ).toEqual([]);
      });

      it("does not claim a multi-entry explores can just be deleted", () => {
         // Deleting it would drop secured.malloy: the convention can only ever
         // produce ["index.malloy"].
         const { warnings } = resolve(
            ["index.malloy", "secured.malloy"],
            WITH_INDEX,
         );
         expect(warnings).toEqual([]);
      });

      it("still says a lone index.malloy explores is deletable", () => {
         const { warnings } = resolve(["index.malloy"], WITH_INDEX);
         expect(warnings).toHaveLength(1);
         expect(warnings[0]).toContain("you can delete it");
      });

      it("warns rather than silently curating when explores is not an array", () => {
         // The usual typo of the array form. Before the convention existed this
         // was ignored and everything stayed listed; now it falls through to
         // the convention, so it has to say so or models vanish unexplained.
         const { explores, fromConvention, warnings } = resolve(
            "reports.malloy",
            WITH_INDEX,
         );
         expect(explores).toEqual(["index.malloy"]);
         expect(fromConvention).toBe(true);
         expect(warnings.some((w) => w.includes("is not an array"))).toBe(true);
      });

      it("warns about a malformed explores even with no index.malloy", () => {
         const { explores, warnings } = resolve(
            "reports.malloy",
            WITHOUT_INDEX,
         );
         expect(explores).toBeUndefined();
         expect(warnings.some((w) => w.includes("is not an array"))).toBe(true);
      });
   });

   describe("resolvePatchedExploresOrigin", () => {
      const CONVENTION = ["index.malloy"];

      it("leaves the origin alone when the body does not mention explores", () => {
         expect(
            resolvePatchedExploresOrigin({
               previousFromConvention: true,
               patchedExplores: undefined,
               existingExplores: CONVENTION,
            }),
         ).toBe(true);
      });

      it("treats a body naming a DIFFERENT surface as a declaration", () => {
         expect(
            resolvePatchedExploresOrigin({
               previousFromConvention: true,
               patchedExplores: ["orders.malloy"],
               existingExplores: CONVENTION,
            }),
         ).toBe(false);
      });

      it("does not arm the boundary for a GET-then-PATCH round trip", () => {
         // The client re-sent exactly what it read. It declared nothing, so
         // editing a description must not switch on a 404 boundary.
         expect(
            resolvePatchedExploresOrigin({
               previousFromConvention: true,
               patchedExplores: ["index.malloy"],
               existingExplores: CONVENTION,
            }),
         ).toBe(true);
      });

      it("ignores ordering when deciding whether the surface changed", () => {
         // The surface is consumed as a Set, so a reordered echo is still an
         // echo rather than a new declaration.
         expect(
            resolvePatchedExploresOrigin({
               previousFromConvention: true,
               patchedExplores: ["b.malloy", "a.malloy"],
               existingExplores: ["a.malloy", "b.malloy"],
            }),
         ).toBe(true);
      });

      it("never resurrects the convention on an already-declared surface", () => {
         // Once declared, echoing it back keeps it declared: the exception
         // exists to avoid ARMING a boundary, never to disarm one.
         expect(
            resolvePatchedExploresOrigin({
               previousFromConvention: false,
               patchedExplores: ["orders.malloy"],
               existingExplores: ["orders.malloy"],
            }),
         ).toBe(false);
      });

      it("treats a shrunken surface as a declaration", () => {
         expect(
            resolvePatchedExploresOrigin({
               previousFromConvention: true,
               patchedExplores: [],
               existingExplores: CONVENTION,
            }),
         ).toBe(false);
      });
   });
});
