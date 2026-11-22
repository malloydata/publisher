// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Candidates over one `#(materialize)` upstream share one compile over its
 * stored table. When that shared compile fails because of one candidate, each
 * is retried alone: the others still build over the table and only the failing
 * one declines (builds from its definition, never refused). No real model
 * makes the shared compile fail while a lone one succeeds, so the failure is
 * injected by wrapping the companion generator. `mock.module` is process-wide,
 * which is why this spec sits in a file of its own.
 */
import { describe, expect, it, mock } from "bun:test";
import * as realReadText from "../text/read_text";

const calls: { names: string[]; failed: boolean }[] = [];
const real = { ...realReadText };
mock.module("../text/read_text", () => ({
   ...real,
   generatePersistedResultFile: (
      ...args: Parameters<typeof real.generatePersistedResultFile>
   ) => {
      const [views, queries, , importOf] = args;
      const text = real.generatePersistedResultFile(...args);
      // Only the over-stored compile passes importOf; break it when it holds "bad".
      if (importOf === undefined) return text;
      const names = [
         ...views.map((v) => String(v.buildSourceName)),
         ...queries.map((q) => String(q.buildSourceID)),
      ];
      const failed = names.some((n) => n.includes("bad"));
      calls.push({ names, failed });
      return failed && text
         ? `${text}\nsource: probe_broken is no_such_source_anywhere\n`
         : text;
   },
}));

const { planPackages } = await import(
   "../../../test_helpers/materialization_fixture"
);
const packages = planPackages();

const RAW = `##! experimental { persistence }
source: raw is duckdb.sql("""select * from (values ('a', 1.5), ('b', 2.5)) as t(cat, amt)""")
`;

describe("a shared over-stored compile that fails", () => {
   it("retries each candidate alone, so only the failing one declines", async () => {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const { c }: any = await packages.plan({
         "m.malloy": `${RAW}
#(materialize)
source: a is raw extend { dimension: cat2 is cat }
source: s is a extend {
  #@ persist name="good_t"
  view: good is { group_by: cat; aggregate: total is amt.sum() }
  #@ persist name="bad_t"
  view: bad is { group_by: cat2; aggregate: total is amt.sum() }
}
`,
      });
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const table = (name: string): any =>
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         Object.values(c.tables).find((t: any) => t.source.name === name);

      expect(calls[0]).toEqual({
         names: expect.arrayContaining([
            expect.stringContaining("good"),
            expect.stringContaining("bad"),
         ]),
         failed: true,
      });
      expect(calls.some((k) => k.names.length === 1 && !k.failed)).toBe(true);
      expect(
         table("s__good__view")?.overStoredBase?.baseBuildSource,
      ).toBeDefined();
      expect(table("s__bad__view")).toBeDefined();
      expect(table("s__bad__view").overStoredBase).toBeUndefined();
   });
});
