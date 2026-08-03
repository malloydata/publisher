import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import {
   CONTEXT_SHED_ORDER,
   MAX_PROPERTIES,
   MAX_PROPERTY_VALUE_LENGTH,
   mergeQueryMetadata,
   mintCorrelationId,
   parseQueryClass,
   parseSuppliedQueryMetadata,
   queryContextProperties,
   RESERVED_CONTEXT_PROPERTIES,
   queryMetadataAdvisoryWarnings,
   queryMetadataBudgetWarning,
   queryMetadataViolations,
} from "./query_metadata";

const originalMode = process.env.PUBLISHER_QUERY_METADATA;

// The feature ships dark, so every test of the resolution logic has to turn it
// on. The one test that asserts what `off` does sets it back for itself.
beforeEach(() => {
   process.env.PUBLISHER_QUERY_METADATA = "on";
});

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

describe("queryMetadataAdvisoryWarnings", () => {
   it("flags a name BigQuery will silently drop", () => {
      // Legal per the contract, kept by every other backend, gone on BigQuery.
      expect(queryMetadataAdvisoryWarnings({ _team: "finance" })).toHaveLength(
         1,
      );
      expect(
         queryMetadataAdvisoryWarnings({ "2team": "finance" }),
      ).toHaveLength(1);
      expect(queryMetadataAdvisoryWarnings({ team2: "finance" })).toEqual([]);
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

describe("queryMetadataBudgetWarning", () => {
   it("warns when a declaration leaves no room for the server context", () => {
      // 20 is the size of the whole bag, and the server adds its own on top, so
      // a declaration that fills the contract quietly loses properties later.
      expect(queryMetadataBudgetWarning(MAX_PROPERTIES)).toContain(
         "the server adds up to",
      );
      expect(queryMetadataBudgetWarning(1)).toBeUndefined();
   });

   it("is a count, not a bag, so a caller can sum the maps that share the budget", () => {
      // A connection declares a default AND an enforced map; 6 + 6 is over
      // budget while neither map is.
      const authorBudget = MAX_PROPERTIES - RESERVED_CONTEXT_PROPERTIES;
      expect(queryMetadataBudgetWarning(authorBudget)).toBeUndefined();
      expect(queryMetadataBudgetWarning(authorBudget + 1)).toBeDefined();
   });
});

describe("CONTEXT_SHED_ORDER", () => {
   it("covers every context property exactly once", () => {
      // The shed order also sizes the author budget the declaration warning
      // uses, so a context property added to one list and not the other would
      // quietly move that threshold.
      const every = queryContextProperties({
         queryClass: "interactive",
         environment: "e",
         package: "p",
         version: "v",
         model: "m.malloy",
         source: "s",
         trigger: "publish",
         runId: "r",
         correlationId: "q",
      });
      expect([...CONTEXT_SHED_ORDER].sort()).toEqual(Object.keys(every).sort());
      expect(RESERVED_CONTEXT_PROPERTIES).toBe(Object.keys(every).length);
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

   it("lets an enforced property beat every declaration", () => {
      // The connection API is administered; a package annotation and a request
      // are not. A tenant must not be able to relabel the traffic its host is
      // billed for.
      const resolved = mergeQueryMetadata({
         connection: { org: "acme" },
         model: { org: "not_acme" },
         request: { org: "definitely_not_acme" },
         enforced: { org: "acme" },
      });
      expect(resolved.metadata?.org).toBe("acme");
   });

   it("still lets the server's own context beat an enforced property", () => {
      // Context describes what the server is actually doing; an admin cannot
      // declare a query into a different class.
      const resolved = mergeQueryMetadata({
         enforced: { class: "interactive", org: "acme" },
         context: { queryClass: "materialize" },
      });
      expect(resolved.metadata).toEqual({ class: "materialize", org: "acme" });
   });

   it("sheds enforced properties after every declared one", () => {
      // The point of enforcing a property is that it is still there at the end:
      // twenty declared properties must not be able to push it out of the bag.
      const request: Record<string, string> = {};
      for (let i = 0; i < MAX_PROPERTIES; i++) request[`p${i}`] = "v";
      const resolved = mergeQueryMetadata({
         request,
         enforced: { org: "acme" },
         context: { queryClass: "interactive" },
      });
      expect(resolved.metadata?.org).toBe("acme");
      expect(resolved.metadata?.class).toBe("interactive");
      expect(resolved.drops.every((d) => d.name.startsWith("p"))).toBe(true);
   });

   it("keeps a property named __proto__, which the contract allows", () => {
      // `__proto__` passes the name rule, so it reaches the merge — and on an
      // object literal the assignment hits the prototype setter, which ignores
      // a string and drops the property with no drop record and no metric. It
      // is the one silent loss in a module whose whole argument is that a lost
      // property is always accounted for.
      // JSON.parse, not a literal: `__proto__:` in source sets the prototype,
      // while a parsed request body carries it as an own property — which is
      // how it arrives here.
      const resolved = mergeQueryMetadata({
         request: JSON.parse('{"__proto__":"surprise","team":"finance"}'),
      });
      expect(Object.keys(resolved.metadata ?? {}).sort()).toEqual([
         "__proto__",
         "team",
      ]);
      expect(resolved.drops).toEqual([]);
      // A string property, not a reshaped bag.
      expect(JSON.stringify(resolved.metadata)).toContain('"__proto__"');
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

   it("sheds the least specific author property first", () => {
      // The caller's own per-request property is the last one standing: it is
      // most likely its join key, and precedence says the connection-wide
      // default is the cheapest thing to lose.
      const connection: Record<string, string> = {};
      for (let i = 0; i < MAX_PROPERTIES - 2; i++) connection[`c${i}`] = "v";
      const resolved = mergeQueryMetadata({
         connection,
         model: { modelProp: "m" },
         request: { request_id: "7f3a" },
         context: { queryClass: "interactive", package: "sales" },
      });
      expect(resolved.metadata?.request_id).toBe("7f3a");
      expect(resolved.metadata?.modelProp).toBe("m");
      expect(resolved.drops.every((d) => d.name.startsWith("c"))).toBe(true);
   });

   it("sheds by the layer whose value won, not the first that mentioned it", () => {
      // `tier` is declared on the connection but the request's value is what
      // survives, so it must be shed as a request property, i.e. last.
      const connection: Record<string, string> = { tier: "bronze" };
      for (let i = 0; i < MAX_PROPERTIES; i++) connection[`c${i}`] = "v";
      const resolved = mergeQueryMetadata({
         connection,
         request: { tier: "gold" },
         context: { queryClass: "interactive" },
      });
      expect(resolved.metadata?.tier).toBe("gold");
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

   it("attaches nothing by default: the feature ships dark", () => {
      // The release default. Every statement leaves exactly as Malloy compiled
      // it until a deployment opts in, because this is the one feature that
      // touches all of them — and on the comment-carrying backends it changes
      // the statement text.
      delete process.env.PUBLISHER_QUERY_METADATA;
      expect(
         mergeQueryMetadata({
            request: { team: "finance" },
            context: { queryClass: "interactive", correlationId: "corr-1" },
         }),
      ).toEqual({ drops: [] });
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
