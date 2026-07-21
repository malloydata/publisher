import { afterEach, describe, expect, it } from "bun:test";
import sinon from "sinon";
import type { Connection } from "@malloydata/malloy";
import { logger } from "../logger";
import type { FreshnessManifest } from "../storage/DatabaseInterface";
import { Model } from "./model";
import { Package } from "./package";

// Unit coverage for Package.quoteBoundTableNames — the bind-time step that
// mirrors the CREATE side's quoting onto manifest table paths (PR #904). The
// integration test proves quote chars appear in real DuckDB SQL, but DuckDB is
// not case-folding and never exhibited the bug; these tests pin the actual
// quoted contract per dialect and exercise every branch (dialect selection,
// no-connection passthrough, already-quoted passthrough, and the
// unresolvable-connection fallback) without needing a live warehouse. The
// quoting itself runs through the real `quoteTablePath`, so this is not a
// re-implementation of the oracle — it drives the shipping code.
describe("service/package quoteBoundTableNames", () => {
   afterEach(() => sinon.restore());

   function makePackage(): Package {
      return new Package(
         "testEnv",
         "testPackage",
         "/tmp/does-not-matter",
         { name: "testPackage", description: "Test package" },
         [],
         new Map<string, Model>(),
      );
   }

   // Reach the private method the same way package.spec.ts reaches private
   // statics — this is the smallest seam that drives the real branch logic.
   function quote(
      pkg: Package,
      entries: FreshnessManifest,
   ): Promise<FreshnessManifest> {
      // @ts-expect-error Accessing private method for testing
      return pkg.quoteBoundTableNames(entries);
   }

   function stubDialect(pkg: Package, dialectName: string): void {
      sinon
         .stub(pkg, "getMalloyConnection")
         .resolves({ dialectName } as Connection);
   }

   it("double-quotes each path segment for a case-folding dialect (Snowflake)", async () => {
      const pkg = makePackage();
      stubDialect(pkg, "snowflake");

      const out = await quote(pkg, {
         s1: { tableName: "schema.order_summary__g0", connectionName: "sf" },
      });

      expect(out.s1.tableName).toBe('"schema"."order_summary__g0"');
      // connectionName and any other fields survive the rewrite.
      expect(out.s1.connectionName).toBe("sf");
   });

   it("double-quotes for other double-quote dialects (Postgres)", async () => {
      const pkg = makePackage();
      stubDialect(pkg, "postgres");

      const out = await quote(pkg, {
         s1: { tableName: "public.daily", connectionName: "pg" },
      });

      expect(out.s1.tableName).toBe('"public"."daily"');
   });

   it("backticks each segment for a backtick dialect (BigQuery)", async () => {
      const pkg = makePackage();
      stubDialect(pkg, "standardsql");

      const out = await quote(pkg, {
         s1: { tableName: "my-proj.ds.events", connectionName: "bq" },
      });

      expect(out.s1.tableName).toBe("`my-proj`.`ds`.`events`");
   });

   it("binds verbatim when the producer recorded no connectionName", async () => {
      const pkg = makePackage();
      const spy = sinon.stub(pkg, "getMalloyConnection");

      const out = await quote(pkg, {
         s1: { tableName: "schema.bare_builder_table" },
      });

      expect(out.s1.tableName).toBe("schema.bare_builder_table");
      // No connection lookup attempted — this is the benign default path.
      expect(spy.called).toBe(false);
   });

   it("passes an already-quoted name through without double-quoting", async () => {
      const pkg = makePackage();
      const spy = sinon.stub(pkg, "getMalloyConnection");

      const out = await quote(pkg, {
         s1: {
            tableName: '"schema"."already_canonical"',
            connectionName: "sf",
         },
      });

      expect(out.s1.tableName).toBe('"schema"."already_canonical"');
      expect(spy.called).toBe(false);
   });

   it("passes an already-backticked name through without re-quoting", async () => {
      const pkg = makePackage();
      const spy = sinon.stub(pkg, "getMalloyConnection");

      const out = await quote(pkg, {
         s1: { tableName: "`ds`.`events`", connectionName: "bq" },
      });

      expect(out.s1.tableName).toBe("`ds`.`events`");
      expect(spy.called).toBe(false);
   });

   it("degrades one entry (unquoted) and logs an actionable error when its connection is unresolvable", async () => {
      const pkg = makePackage();
      sinon
         .stub(pkg, "getMalloyConnection")
         .rejects(new Error("no connection named 'gone'"));
      const errSpy = sinon.stub(logger, "error");

      const out = await quote(pkg, {
         s1: { tableName: "schema.orphaned", connectionName: "gone" },
      });

      // The one entry degrades to the unquoted name rather than throwing out
      // of the loop and failing the whole package bind.
      expect(out.s1.tableName).toBe("schema.orphaned");
      // ...but the misconfiguration is a distinct, loud, actionable signal —
      // not folded silently into the benign no-connection default.
      expect(errSpy.calledOnce).toBe(true);
      // winston's error() is overloaded, so widen the recorded args to assert
      // the (message, context) contract this branch emits.
      const [message, ctx] = errSpy.firstCall.args as unknown as [
         string,
         Record<string, unknown>,
      ];
      expect(message).toContain("gone");
      expect(message).toContain("Fix:");
      expect(ctx).toMatchObject({
         sourceEntityId: "s1",
         connectionName: "gone",
      });
   });

   it("quotes resolvable entries and degrades an unresolvable one in the same bind", async () => {
      const pkg = makePackage();
      const lookup = sinon.stub(pkg, "getMalloyConnection");
      lookup
         .withArgs("sf")
         .resolves({ dialectName: "snowflake" } as Connection);
      lookup.withArgs("gone").rejects(new Error("missing"));
      sinon.stub(logger, "error");

      const out = await quote(pkg, {
         good: { tableName: "schema.kept", connectionName: "sf" },
         bad: { tableName: "schema.orphaned", connectionName: "gone" },
      });

      expect(out.good.tableName).toBe('"schema"."kept"');
      expect(out.bad.tableName).toBe("schema.orphaned");
   });
});
