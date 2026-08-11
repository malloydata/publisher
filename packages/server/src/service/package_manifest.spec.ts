import { describe, expect, it } from "bun:test";
import {
   packageMaterializationWarnings,
   parsePackageMaterialization,
   parsePackageScope,
   resolveExplores,
   resolvePackageScope,
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
         queryableSourcesDeclared = false,
      ) =>
         resolveExplores({
            declaredExplores,
            queryableSourcesDeclared,
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
            true,
         );
         expect(fromConvention).toBe(true);
         expect(warnings).toHaveLength(1);
         expect(warnings[0]).toContain("has no effect");
      });

      it("says nothing about queryableSources alongside an explicit explores", () => {
         // There it is fully meaningful, so there is nothing to warn about.
         expect(
            resolve(["orders.malloy"], WITHOUT_INDEX, true).warnings,
         ).toEqual([]);
      });
   });
});
