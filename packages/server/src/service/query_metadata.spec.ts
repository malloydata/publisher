import { afterEach, describe, expect, it } from "bun:test";
import {
   MAX_PROPERTIES,
   MAX_PROPERTY_VALUE_LENGTH,
   mergeQueryMetadata,
   mintCorrelationId,
   parseQueryClass,
   parseSuppliedQueryMetadata,
   queryContextProperties,
   queryMetadataPortabilityWarnings,
   queryMetadataViolations,
} from "./query_metadata";

const originalMode = process.env.PUBLISHER_QUERY_METADATA;

afterEach(() => {
   if (originalMode === undefined) {
      delete process.env.PUBLISHER_QUERY_METADATA;
   } else {
      process.env.PUBLISHER_QUERY_METADATA = originalMode;
   }
});

describe("queryMetadataViolations", () => {
   it("accepts a conforming bag", () => {
      expect(
         queryMetadataViolations({ team: "finance", run_id: "abc-123" }),
      ).toEqual([]);
      expect(queryMetadataViolations(undefined)).toEqual([]);
      expect(queryMetadataViolations(null)).toEqual([]);
   });

   it("rejects a non-object", () => {
      expect(queryMetadataViolations("team=finance")).toHaveLength(1);
      expect(queryMetadataViolations([1, 2])).toHaveLength(1);
   });

   it("rejects a property name outside the contract", () => {
      // A dot is the one an author reaches for first, and Malloy throws on it at
      // dispatch — so it has to come back as a message here.
      expect(queryMetadataViolations({ "team.name": "finance" })).toHaveLength(
         1,
      );
      expect(queryMetadataViolations({ "team-name": "finance" })).toHaveLength(
         1,
      );
   });

   it("rejects a value that cannot be rendered", () => {
      expect(queryMetadataViolations({ team: 'fin"ance' })).toHaveLength(1);
      expect(queryMetadataViolations({ team: "fin\nance" })).toHaveLength(1);
      expect(queryMetadataViolations({ team: "финансы" })).toHaveLength(1);
   });

   it("rejects a non-string value, naming the property", () => {
      const problems = queryMetadataViolations({ retries: 3 });
      expect(problems).toHaveLength(1);
      expect(problems[0]).toContain("retries");
   });

   it("rejects an over-long value and too many properties", () => {
      expect(
         queryMetadataViolations({
            team: "x".repeat(MAX_PROPERTY_VALUE_LENGTH + 1),
         }),
      ).toHaveLength(1);
      const tooMany: Record<string, string> = {};
      for (let i = 0; i <= MAX_PROPERTIES; i++) tooMany[`p${i}`] = "v";
      expect(queryMetadataViolations(tooMany)).toHaveLength(1);
   });
});

describe("queryMetadataPortabilityWarnings", () => {
   it("flags a name BigQuery will silently drop", () => {
      // Legal per the contract, kept by every other backend, gone on BigQuery.
      expect(
         queryMetadataPortabilityWarnings({ _team: "finance" }),
      ).toHaveLength(1);
      expect(
         queryMetadataPortabilityWarnings({ "2team": "finance" }),
      ).toHaveLength(1);
      expect(queryMetadataPortabilityWarnings({ team2: "finance" })).toEqual(
         [],
      );
   });
});

describe("queryContextProperties", () => {
   it("emits only the fields that are set", () => {
      expect(
         queryContextProperties({
            queryClass: "materialize",
            environment: "prod",
            package: "sales",
            source: "order_rollup",
            trigger: "scheduler",
            runId: "run-7",
            correlationId: "q-1",
         }),
      ).toEqual({
         class: "materialize",
         environment: "prod",
         package: "sales",
         source: "order_rollup",
         trigger: "scheduler",
         run_id: "run-7",
         query_id: "q-1",
      });
      expect(queryContextProperties({})).toEqual({});
   });
});

describe("mintCorrelationId", () => {
   it("mints a distinct id per call", () => {
      const first = mintCorrelationId();
      expect(first).toMatch(/^[0-9a-f-]{36}$/);
      expect(mintCorrelationId()).not.toBe(first);
   });
});

describe("mergeQueryMetadata", () => {
   it("returns nothing when every layer is empty", () => {
      expect(mergeQueryMetadata({})).toEqual({ drops: [] });
   });

   it("merges most-specific-wins per property", () => {
      const resolved = mergeQueryMetadata({
         connection: { team: "finance", tier: "bronze", deployment: "eu" },
         model: { tier: "silver", surface: "marts" },
         request: { tier: "gold" },
      });
      expect(resolved.metadata).toEqual({
         team: "finance",
         tier: "gold",
         surface: "marts",
         deployment: "eu",
      });
   });

   it("lets context win over an author property of the same name", () => {
      // Otherwise a caller could label its own interactive query as a build and
      // corrupt exactly the attribution this feature exists to produce.
      const resolved = mergeQueryMetadata({
         request: { class: "materialize", package: "not-this-one" },
         context: { queryClass: "interactive", package: "sales" },
      });
      expect(resolved.metadata).toEqual({
         class: "interactive",
         package: "sales",
      });
   });

   it("reserves query_id: a caller cannot supply the correlation key", () => {
      // The response hands this back as the join key, so a caller-owned value
      // would let two calls claim the same id.
      const resolved = mergeQueryMetadata({
         request: { query_id: "mine" },
         context: { correlationId: "server-minted" },
      });
      expect(resolved.metadata?.query_id).toBe("server-minted");
   });

   it("drops a property whose name violates the contract", () => {
      // Never throws: it would fail the customer's query at dispatch.
      const resolved = mergeQueryMetadata({
         request: { "team.name": "finance", team: "finance" },
      });
      expect(resolved.metadata).toEqual({ team: "finance" });
      expect(resolved.drops).toEqual([
         { name: "team.name", reason: "invalid_name" },
      ]);
   });

   it("sanitizes an unrenderable value instead of failing", () => {
      const resolved = mergeQueryMetadata({
         request: { note: 'a"b\nc' },
      });
      expect(resolved.metadata).toEqual({ note: "a_b_c" });
   });

   it("truncates an over-long value", () => {
      const resolved = mergeQueryMetadata({
         request: { note: "x".repeat(MAX_PROPERTY_VALUE_LENGTH + 50) },
      });
      expect(resolved.metadata?.note).toHaveLength(MAX_PROPERTY_VALUE_LENGTH);
   });

   it("enforces the property cap by dropping author properties, keeping context", () => {
      const request: Record<string, string> = {};
      for (let i = 0; i < MAX_PROPERTIES; i++) request[`p${i}`] = "v";
      const resolved = mergeQueryMetadata({
         request,
         context: { queryClass: "index", package: "sales" },
      });
      expect(Object.keys(resolved.metadata ?? {})).toHaveLength(MAX_PROPERTIES);
      // Context survived; the overflow came out of the author's properties.
      expect(resolved.metadata?.class).toBe("index");
      expect(resolved.metadata?.package).toBe("sales");
      expect(resolved.drops.map((d) => d.reason)).toContain("property_cap");
   });

   it("keeps the serialized bag inside Snowflake's tag limit", () => {
      // db-snowflake slices an over-long JSON tag, which leaves it unparseable —
      // so whole properties come out here instead.
      const request: Record<string, string> = {};
      for (let i = 0; i < 12; i++) {
         request[`p${i}`] = "x".repeat(MAX_PROPERTY_VALUE_LENGTH);
      }
      const resolved = mergeQueryMetadata({
         request,
         context: { queryClass: "interactive" },
      });
      expect(JSON.stringify(resolved.metadata).length).toBeLessThanOrEqual(
         2000,
      );
      expect(resolved.drops.map((d) => d.reason)).toContain("serialized_cap");
      expect(resolved.metadata?.class).toBe("interactive");
   });

   it("attaches nothing when PUBLISHER_QUERY_METADATA=off", () => {
      process.env.PUBLISHER_QUERY_METADATA = "off";
      expect(
         mergeQueryMetadata({
            request: { team: "finance" },
            context: { queryClass: "interactive", package: "sales" },
         }),
      ).toEqual({ drops: [] });
   });
});

describe("parseSuppliedQueryMetadata", () => {
   it("returns the bag when it conforms, undefined when empty", () => {
      expect(parseSuppliedQueryMetadata({ team: "finance" })).toEqual({
         team: "finance",
      });
      expect(parseSuppliedQueryMetadata({})).toBeUndefined();
      expect(parseSuppliedQueryMetadata(undefined)).toBeUndefined();
   });

   it("throws listing every violation, for the caller's 400", () => {
      expect(() =>
         parseSuppliedQueryMetadata({ "team.name": "finance", note: 'a"b' }),
      ).toThrow(/team.name.*note|note.*team.name/s);
   });
});

describe("parseQueryClass", () => {
   it("accepts the four classes and passes through absence", () => {
      expect(parseQueryClass("materialize")).toBe("materialize");
      expect(parseQueryClass(undefined)).toBeUndefined();
      expect(parseQueryClass(null)).toBeUndefined();
   });

   it("throws on an unrecognized class rather than ignoring it", () => {
      expect(() => parseQueryClass("backfill")).toThrow(/queryClass/);
      expect(() => parseQueryClass(7)).toThrow(/queryClass/);
   });
});
