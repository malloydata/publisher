// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
} from "@malloydata/malloy";
import {
   isCrossConnectionRefusal,
   leaveOutCrossConnectionJoins,
} from "./cross_connection_text";
import { definitionCutAt } from "./definition_cut";

const URL_ = "file:///p/m.malloy";

function compileWith(text: string) {
   const crm = new DuckDBConnection("crm", ":memory:");
   const product = new DuckDBConnection("product", ":memory:");
   return async (overlay: ReadonlyMap<string, string>) => {
      const runtime = new Runtime({
         urlReader: new InMemoryURLReader(
            new Map([[URL_, overlay.get(URL_) ?? text]]),
         ),
         connections: new FixedConnectionMap(
            new Map([
               ["crm", crm],
               ["product", product],
            ]),
            "crm",
         ),
      });
      return runtime.loadModel(new URL(URL_)).getModel();
   };
}

/** Compile `files` (by URL), entered at {@link URL_}, with an overlay. */
function compileFiles(files: Record<string, string>) {
   const crm = new DuckDBConnection("crm", ":memory:");
   const product = new DuckDBConnection("product", ":memory:");
   return async (overlay: ReadonlyMap<string, string>) => {
      const runtime = new Runtime({
         urlReader: new InMemoryURLReader(
            new Map(
               Object.entries(files).map(([u, t]) => [u, overlay.get(u) ?? t]),
            ),
         ),
         connections: new FixedConnectionMap(
            new Map([
               ["crm", crm],
               ["product", product],
            ]),
            "crm",
         ),
      });
      return runtime.loadModel(new URL(URL_)).getModel();
   };
}

type Compiled = Awaited<ReturnType<ReturnType<typeof compileWith>>>;

/**
 * {@link leaveOutCrossConnectionJoins} over `load`, with the model the last
 * successful compile returned; undefined when nothing is left out.
 */
async function leaveOut(args: {
   firstError: unknown;
   readText: (url: string) => string | undefined;
   load: (overlay: ReadonlyMap<string, string>) => Promise<Compiled>;
}) {
   let result: Compiled | undefined;
   const kept = await leaveOutCrossConnectionJoins({
      firstError: args.firstError,
      readText: args.readText,
      compile: async (overlay) => {
         result = await args.load(overlay);
      },
   });
   return kept && result ? { ...kept, result } : undefined;
}

const model = `source: accounts is crm.sql("select 1 as id, 'a' as account_name") extend { primary_key: id }
source: usage is product.sql("select 1 as account_id, 5 as events")
#(doc) "Usage by account"
source: account_usage is accounts extend {
  #(doc) "The account's usage rows"
  join_many: u is usage on u.account_id = id
  dimension:
    upper_name is upper(account_name)
    first_events is u.events
  measure:
    n is count()
    total_events is u.events.sum()
  view: by_account is { group_by: account_name; aggregate: total_events }
  view: counts is { group_by: account_name; aggregate: n }
}
`;

describe("leaving out joins across connections", () => {
   it("recognizes Malloy's refusal by its code", async () => {
      const error = await compileWith(model)(new Map()).catch((e) => e);
      expect(isCrossConnectionRefusal(error)).toBe(true);
      expect(isCrossConnectionRefusal(new Error("x"))).toBe(false);
   });

   it("leaves out the join and every definition that reads it, keeping positions", async () => {
      const compile = compileWith(model);
      const firstError = await compile(new Map()).catch((e) => e);
      const view = await leaveOut({
         firstError,
         readText: () => model,
         load: compile,
      });
      expect(view).toBeDefined();
      expect(
         view?.leftOut.map((l) => [
            l.keyword,
            l.name,
            l.declaration,
            l.crossConnection,
         ]),
      ).toEqual([
         ["join_many", "u", "account_usage", true],
         ["dimension", "first_events", "account_usage", false],
         ["measure", "total_events", "account_usage", false],
         ["view", "by_account", "account_usage", false],
      ]);
      const text = view?.overlay.get(URL_) as string;
      expect(text.length).toBe(model.length);
      expect(text.split("\n").length).toBe(model.split("\n").length);
      // The join's own annotation goes with it; the source's stays.
      expect(text).not.toContain("usage rows");
      expect(text).toContain('#(doc) "Usage by account"');
      const fields = (
         view?.result._modelDef.contents.account_usage as {
            fields: { name: string; as?: string }[];
         }
      ).fields.map((f) => f.as ?? f.name);
      expect(fields).toContain("upper_name");
      expect(fields).toContain("counts");
      expect(fields).not.toContain("u");
      expect(fields).not.toContain("total_events");
   });

   it("leaves a file with any other error alone", async () => {
      const broken = model.replace("upper(account_name)", "upper(nope)");
      const compile = compileWith(broken);
      const firstError = await compile(new Map()).catch((e) => e);
      expect(
         await leaveOut({
            firstError,
            readText: () => broken,
            load: compile,
         }),
      ).toBeUndefined();
   });

   it("cuts one definition of several, with its comma", () => {
      const text =
         "source: s is t extend {\n  dimension: a is 1, b is 2, c is 3\n}\n";
      const at = text.indexOf("b is");
      const cut = definitionCutAt(text, at);
      expect(cut?.left).toEqual({
         keyword: "dimension",
         name: "b",
         declaration: "s",
      });
      const out = text.slice(0, cut?.start) + text.slice(cut?.end);
      expect(out.replace(/\s+/g, " ")).toBe(
         "source: s is t extend { dimension: a is 1, c is 3 } ",
      );
   });

   it("names the whole statement on a cut of one definition of several", () => {
      const text =
         "source: s is t extend {\n  join_one: a is u with k, b is u with k\n}\n";
      const one = definitionCutAt(text, text.indexOf("a is"));
      const other = definitionCutAt(text, text.indexOf("b is"));
      expect(one?.statement).toEqual(other?.statement);
      expect(one?.statement?.definitions).toBe(2);
      const whole = one!.statement!;
      expect(
         (text.slice(0, whole.start) + text.slice(whole.end)).replace(
            /\s+/g,
            " ",
         ),
      ).toBe("source: s is t extend { } ");
   });

   it("cuts the top-level declaration around an offset when asked", () => {
      const text =
         "source: s is t extend {\n  join_one: a is u with k\n}\nsource: z is t\n";
      const cut = definitionCutAt(text, text.indexOf("a is"), true);
      expect(cut?.left).toEqual({ keyword: "source", name: "s" });
      expect(text.slice(0, cut?.start) + text.slice(cut?.end)).toBe(
         "\nsource: z is t\n",
      );
   });

   it("finds nothing to cut in a where:", () => {
      const text = "source: s is t extend {\n  where: x > 1\n}\n";
      const cut = definitionCutAt(text, text.indexOf("x > 1"));
      // The innermost definition statement holding a where: is its source.
      expect(cut?.left.keyword).toBe("source");
   });

   it("keeps each kept definition's own annotations where a statement holds several", () => {
      const text =
         "source: s is t extend {\n  dimension:\n    # a\n    a is u.x\n    # b\n    b is y\n}\n";
      const cut = definitionCutAt(text, text.indexOf("a is"));
      const out = (text.slice(0, cut?.start) + text.slice(cut?.end)).replace(
         /\s+/g,
         " ",
      );
      expect(out).toBe("source: s is t extend { dimension: # b b is y } ");
   });

   it("leaves a model annotation above a source it leaves out", () => {
      const text = '##! experimental.x\n## doc="m"\n# s\nsource: s is t\n';
      const cut = definitionCutAt(text, text.indexOf("s is"));
      const out = text.slice(0, cut?.start) + text.slice(cut?.end);
      expect(out).toBe('##! experimental.x\n## doc="m"\n\n');
   });

   it("leaves out an importer's own refused join, hidden on the first compile by its import's", async () => {
      const base = "file:///p/base.malloy";
      const files: Record<string, string> = {
         [base]: model,
         [URL_]: `import "base.malloy"
source: own is product.sql("select 1 as account_id, 5 as events") extend {
  join_one: v is accounts on v.id = account_id
}
source: reads_own is own -> { group_by: account_id, n is v.account_name }
source: kept is accounts extend { measure: c is count() }
`,
      };
      const compile = compileFiles(files);
      const firstError = await compile(new Map()).catch((e) => e);
      const view = await leaveOut({
         firstError,
         readText: (u) => files[u],
         load: compile,
      });
      expect(
         view?.leftOut
            .filter((l) => l.crossConnection)
            .map((l) => [l.url, l.name]),
      ).toEqual([
         [base, "u"],
         [URL_, "v"],
      ]);
      expect(view?.leftOut.map((l) => l.name)).toContain("reads_own");
      expect(Object.keys(view?.result._modelDef.contents ?? {})).toContain(
         "kept",
      );
   });

   it("follows a join left out of an imported file into its importer", async () => {
      const base = "file:///p/base.malloy";
      const files: Record<string, string> = {
         [base]: model,
         [URL_]: `import "base.malloy"
source: report is account_usage extend {
  view: by_events is { group_by: account_name; aggregate: total_events }
  view: kept is { group_by: upper_name; aggregate: n }
}
`,
      };
      const compile = compileFiles(files);
      const firstError = await compile(new Map()).catch((e) => e);
      const view = await leaveOut({
         firstError,
         readText: (u) => files[u],
         load: compile,
      });
      expect(view?.leftOut.map((l) => [l.url, l.name])).toEqual([
         [base, "u"],
         [base, "first_events"],
         [base, "total_events"],
         [base, "by_account"],
         [URL_, "by_events"],
      ]);
   });

   it("leaves the join out of a file an index exports from", async () => {
      const base = "file:///p/base.malloy";
      const files: Record<string, string> = {
         [base]: model,
         [URL_]: `import "base.malloy"
export { account_usage, usage }
`,
      };
      const compile = compileFiles(files);
      const firstError = await compile(new Map()).catch((e) => e);
      // The importer's export fails only because its import did.
      expect(isCrossConnectionRefusal(firstError)).toBe(true);
      const view = await leaveOut({
         firstError,
         readText: (u) => files[u],
         load: compile,
      });
      expect(view?.leftOut.map((l) => [l.url, l.name])).toEqual([
         [base, "u"],
         [base, "first_events"],
         [base, "total_events"],
         [base, "by_account"],
      ]);
      expect(view?.overlay.get(URL_)).toBeUndefined();
   });

   it("leaves an index alone that exports a source the passes leave out", async () => {
      const base = "file:///p/base.malloy";
      const files: Record<string, string> = {
         [base]: `${model}source: per_usage is account_usage -> { group_by: u.events }
`,
         [URL_]: `import "base.malloy"
export { per_usage }
`,
      };
      const compile = compileFiles(files);
      const firstError = await compile(new Map()).catch((e) => e);
      expect(isCrossConnectionRefusal(firstError)).toBe(true);
      // Leaving out `per_usage` would leave its export naming nothing: the
      // export is never cut, so the file fails as it did.
      expect(
         await leaveOut({
            firstError,
            readText: (u) => files[u],
            load: compile,
         }),
      ).toBeUndefined();
   });

   it("leaves a file alone whose own error outlives the left-out joins", async () => {
      const base = "file:///p/base.malloy";
      const files: Record<string, string> = {
         [base]: model,
         [URL_]: `import "base.malloy"
source: report is account_usage extend {
  dimension: oops is nope
}
source: other is nowhere
`,
      };
      const compile = compileFiles(files);
      const firstError = await compile(new Map()).catch((e) => e);
      expect(
         await leaveOut({
            firstError,
            readText: (u) => files[u],
            load: compile,
         }),
      ).toBeUndefined();
   });
});
