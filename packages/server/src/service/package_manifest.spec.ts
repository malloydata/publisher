import { describe, expect, it } from "bun:test";
import {
   materializationWithQueryMetadata,
   packageMaterializationWarnings,
   parsePackageMaterialization,
   parsePackageScope,
   resolvePackageQueryMetadata,
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

   describe("resolvePackageQueryMetadata", () => {
      it("reads the canonical root home with no warning", () => {
         expect(
            resolvePackageQueryMetadata({ team: "finance" }, undefined),
         ).toEqual({ queryMetadata: { team: "finance" }, warnings: [] });
      });

      it("still honors the enveloped form, with a deprecation warning", () => {
         const resolved = resolvePackageQueryMetadata(undefined, {
            queryMetadata: { team: "finance" },
         });
         expect(resolved.queryMetadata).toEqual({ team: "finance" });
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
         ).toEqual({ queryMetadata: { team: "finance" }, warnings: [] });
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
            warnings: [],
         });
         expect(
            resolvePackageQueryMetadata(undefined, { schedule: "0 6 * * *" }),
         ).toEqual({ queryMetadata: undefined, warnings: [] });
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
