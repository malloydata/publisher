// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
} from "@malloydata/malloy";
import {
   isCrossConnectionRefusal,
   loadLeavingOutCrossConnectionJoins,
} from "./cross_connection_joins";
import { definitionCutAt } from "../text/definition_cut";
import { installSettings } from "../../../test_helpers/settings_env";

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
   let restore: () => void;
   beforeAll(() => {
      restore = installSettings({
         env: { PUBLISHER_MATERIALIZE: "true" },
      }).restore;
   });
   afterAll(() => restore());

   it("recognizes Malloy's refusal by its code", async () => {
      const error = await compileWith(model)(new Map()).catch((e) => e);
      expect(isCrossConnectionRefusal(error)).toBe(true);
      expect(isCrossConnectionRefusal(new Error("x"))).toBe(false);
   });

   it("leaves out the join and every definition that reads it, keeping positions", async () => {
      const compile = compileWith(model);
      const firstError = await compile(new Map()).catch((e) => e);
      const view = await loadLeavingOutCrossConnectionJoins({
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
         await loadLeavingOutCrossConnectionJoins({
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
      const view = await loadLeavingOutCrossConnectionJoins({
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
         await loadLeavingOutCrossConnectionJoins({
            firstError,
            readText: (u) => files[u],
            load: compile,
         }),
      ).toBeUndefined();
   });

   it("loads nothing from a build view with the switch off", async () => {
      const off = installSettings({ env: { PUBLISHER_MATERIALIZE: "false" } });
      try {
         const compile = compileWith(model);
         const firstError = await compile(new Map()).catch((e) => e);
         expect(
            await loadLeavingOutCrossConnectionJoins({
               firstError,
               readText: () => model,
               load: compile,
            }),
         ).toBeUndefined();
      } finally {
         off.restore();
      }
   });
});
