// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { BadRequestError, NotQueryableError } from "../../errors";
import {
   answerErrorText,
   diffPlans,
   stagePackageTree,
   MAX_WHAT_IF_QUERIES,
   parseWhatIfQueries,
   storedSchemaOf,
} from "./what_if";

type Plan = Parameters<typeof diffPlans>[1];

function plan(
   sources: Record<string, { entity: string; storage?: string }>,
): Plan {
   return {
      graphs: [],
      sources: Object.fromEntries(
         Object.entries(sources).map(([id, s]) => [
            id,
            {
               name: id.split("@")[0],
               sourceID: id,
               connectionName: "duckdb",
               dialect: "duckdb",
               origin: "persist",
               sourceEntityId: s.entity,
               sql: "SELECT 1",
               columns: [],
               annotationFields: s.storage ? { storage: s.storage } : {},
            },
         ]),
      ),
   } as unknown as Plan;
}

describe("parseWhatIfQueries", () => {
   it("keeps ad-hoc and named queries", () => {
      expect(
         parseWhatIfQueries([
            {
               modelPath: "m.malloy",
               query: "run: a -> { aggregate: c is count() }",
            },
            { modelPath: "m.malloy", sourceName: "a", queryName: "v" },
         ]),
      ).toEqual([
         {
            modelPath: "m.malloy",
            query: "run: a -> { aggregate: c is count() }",
         },
         { modelPath: "m.malloy", sourceName: "a", queryName: "v" },
      ]);
   });

   it("refuses a missing, empty or oversized list, and a malformed entry", () => {
      for (const bad of [
         undefined,
         [],
         "q",
         [{ query: "x" }],
         [{ modelPath: "m.malloy", query: 3 }],
         [null],
         Array.from({ length: MAX_WHAT_IF_QUERIES + 1 }, () => ({
            modelPath: "m.malloy",
            query: "x",
         })),
      ]) {
         expect(() => parseWhatIfQueries(bad)).toThrow(BadRequestError);
      }
   });
});

describe("storedSchemaOf", () => {
   it("declares each Malloy type as the DuckDB type a build stores, and leaves the rest off", () => {
      expect(
         storedSchemaOf([
            { name: "s", type: "string" },
            { name: "n", type: "number" },
            { name: "b", type: "boolean" },
            { name: "d", type: "date" },
            { name: "t", type: "timestamp" },
            { name: "tz", type: "timestamptz" },
            { name: "j", type: "json" },
            { name: "x", type: "sql native" },
            { type: "string" },
         ]),
      ).toEqual([
         { name: "s", type: "VARCHAR" },
         { name: "n", type: "DOUBLE" },
         { name: "b", type: "BOOLEAN" },
         { name: "d", type: "DATE" },
         { name: "t", type: "TIMESTAMP" },
         { name: "tz", type: "TIMESTAMP WITH TIME ZONE" },
      ]);
   });
});

describe("diffPlans", () => {
   it("sorts the proposal's sources into added, removed, changed and unchanged", () => {
      const current = plan({
         "kept@m": { entity: "e1" },
         "moved@m": { entity: "e2" },
         "restored@m": { entity: "e3" },
         "dropped@m": { entity: "e4" },
      });
      const proposed = plan({
         "kept@m": { entity: "e1" },
         "moved@m": { entity: "e2x" },
         "restored@m": { entity: "e3", storage: "lake" },
         "new@m": { entity: "e5" },
      });
      const diff = diffPlans(current, proposed);
      expect(diff.added.map((s) => s.name)).toEqual(["new"]);
      expect(diff.removed.map((s) => s.name)).toEqual(["dropped"]);
      expect(diff.changed.map((s) => s.name)).toEqual(["moved", "restored"]);
      expect(diff.changed[1]).toMatchObject({ storage: "lake" });
      expect(diff.unchanged.map((s) => s.name)).toEqual(["kept"]);
   });

   it("reads every proposed source as added when nothing is planned now", () => {
      const diff = diffPlans(null, plan({ "a@m": { entity: "e" } }));
      expect(diff.added.map((s) => s.name)).toEqual(["a"]);
      expect(diff.removed).toEqual([]);
   });
});

describe("answerErrorText", () => {
   it("passes on what the query route would tell the caller", () => {
      expect(answerErrorText(new BadRequestError("bad given"))).toBe(
         "bad given",
      );
      expect(answerErrorText(new NotQueryableError("not queryable"))).toBe(
         "not queryable",
      );
   });

   it("answers an internal failure generically, never with its text", () => {
      const text = answerErrorText(
         new Error("IO Error: /var/lib/lake/catalog.db: s3://bucket/key"),
      );
      expect(text).not.toContain("s3://");
      expect(text).not.toContain("/var/lib");
   });
});

describe("stagePackageTree", () => {
   it("copies the files a proposal may edit and the manifest, links the rest, and skips what a load never reads", async () => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "what-if-stage-"));
      try {
         const from = path.join(root, "pkg");
         const to = path.join(root, "staged");
         fs.mkdirSync(path.join(from, "data"), { recursive: true });
         fs.mkdirSync(path.join(from, ".git"), { recursive: true });
         fs.writeFileSync(path.join(from, "model.malloy"), "source: x is y");
         fs.writeFileSync(path.join(from, "publisher.json"), "{}");
         fs.writeFileSync(path.join(from, "data", "rows.json"), "[]");
         fs.writeFileSync(path.join(from, ".git", "HEAD"), "ref");
         fs.symlinkSync(
            path.join(root, "nowhere"),
            path.join(from, "dangling.csv"),
         );

         await stagePackageTree(from, to);

         const isLink = (p: string) => fs.lstatSync(p).isSymbolicLink();
         expect(isLink(path.join(to, "model.malloy"))).toBe(false);
         expect(isLink(path.join(to, "publisher.json"))).toBe(false);
         // A JSON dataset can be large: linked, never copied.
         expect(isLink(path.join(to, "data", "rows.json"))).toBe(true);
         expect(fs.existsSync(path.join(to, ".git"))).toBe(false);
         expect(fs.existsSync(path.join(to, "dangling.csv"))).toBe(false);
      } finally {
         fs.rmSync(root, { recursive: true, force: true });
      }
   });
});
