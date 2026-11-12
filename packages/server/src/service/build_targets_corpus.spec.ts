// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The address-stability corpus: moving the plan off the deprecated
// `PersistSource.getSQL()` / `makeBuildId()` onto the build target's `sql` and
// `buildSQL()` must not move a single address.
//
// A source's address keys the incremental ledger, reuse, refresh streams and
// anchors (#6145 is what drift looks like): a moved address orphans the built
// table and re-seeds forever, silently.
// `build_targets_address_equality.spec.ts` pins the rule on one hand-written
// model; this runs it over every real model in the repo that declares a
// persisted source:
//
//  - the persist fixtures under `tests/fixtures/persist-*`, compiled as their
//    packages are, against their own data files;
//  - every hammer scenario's models, compiled against DuckDB with each
//    scenario's `## Data` tables created under the connection and schema the
//    model reads them by (hammer runs them on Postgres; the address rule does
//    not depend on the dialect, since both forms render through the same
//    compiler call, so DuckDB stands in for schema lookup only).
//
// For each planned source it compares the new address, compiler key, wire SQL
// and build SQL with the deprecated computation, and fails listing every source
// that moved. Kept permanently: a compiler bump is exactly what could move one.

import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   type PersistSource,
   Runtime,
} from "@malloydata/malloy";
import { afterAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { pathToFileURL } from "url";
import {
   addressSQL,
   addressText,
   buildSQLOf,
   compilerBuildId,
   computeSourceEntityId,
   plannedSources,
} from "./materialization/plan/build_plan";

const REPO = path.resolve(__dirname, "../../../..");
const FIXTURES = path.join(REPO, "packages/server/tests/fixtures");
const HAMMER = path.join(REPO, "hammer/scenarios");

const opened: DuckDBConnection[] = [];
const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "build-targets-corpus-"));
afterAll(async () => {
   for (const c of opened) await c.close().catch(() => {});
   fs.rmSync(scratch, { recursive: true, force: true });
});

/** One model to plan: its files, entry file, and connections. */
interface CorpusModel {
   label: string;
   files: Map<string, string>;
   entry: string;
   connections: Map<string, DuckDBConnection>;
}

/** What moved for one source, or nothing. */
interface Moved {
   model: string;
   source: string;
   what: string[];
}

function connection(name: string, workingDirectory?: string): DuckDBConnection {
   const dbPath = path.join(
      scratch,
      `${name}-${opened.length}-${Math.random().toString(36).slice(2)}.db`,
   );
   const c = workingDirectory
      ? new DuckDBConnection(name, dbPath, workingDirectory)
      : new DuckDBConnection(name, dbPath);
   opened.push(c);
   return c;
}

/** Compare every address and SQL form for each planned source of `model`. */
async function compare(
   model: CorpusModel,
): Promise<{ checked: string[]; moved: Moved[] } | undefined> {
   const runtime = new Runtime({
      urlReader: new InMemoryURLReader(model.files),
      connections: new FixedConnectionMap(
         new Map([...model.connections]),
         [...model.connections.keys()][0],
      ),
   });
   const compiled = await runtime.loadModel(new URL(model.entry)).getModel();
   const targets = await runtime.getBuildTargets(compiled);
   const digests: Record<string, string> = {};
   for (const [name, c] of model.connections)
      digests[name] = await c.getDigest();

   const checked: string[] = [];
   const moved: Moved[] = [];
   for (const source of plannedSources(targets)) {
      if (targets.unbuildable.some((u) => u.source === source)) {
         // Unkeyable now. It must have been unaddressable before too: a source
         // the deprecated getSQL() could render, and the target walk cannot
         // key, has lost its address, and its built table is orphaned.
         checked.push(source.name);
         if (legacyRenders(source)) {
            moved.push({
               model: model.label,
               source: source.name,
               what: ["lostAddress"],
            });
         }
         continue;
      }
      const what = movedFields(source, digests);
      checked.push(source.name);
      if (what.length > 0) {
         moved.push({ model: model.label, source: source.name, what });
      }
   }
   return { checked, moved };
}

/** Whether the deprecated `getSQL()` renders `source` at all. */
function legacyRenders(source: PersistSource): boolean {
   try {
      source.getSQL();
      return true;
   } catch {
      return false;
   }
}

/** Which forms differ between the target-based and deprecated computations. */
function movedFields(
   source: PersistSource,
   digests: Record<string, string>,
): string[] {
   const digest = digests[source.connectionName];
   const legacySQL = source.getSQL();
   const what: string[] = [];
   if (addressSQL(source) !== legacySQL) what.push("sql");
   if (
      computeSourceEntityId(source, digests) !==
      source.makeBuildId(digest, addressText(source, legacySQL))
   ) {
      what.push("sourceEntityId");
   }
   if (
      compilerBuildId(source, digests) !== source.makeBuildId(digest, legacySQL)
   ) {
      what.push("compilerBuildId");
   }
   const empty = { entries: {} };
   if (
      buildSQLOf(source, empty) !==
      source.getSQL({ buildManifest: empty, connectionDigests: digests })
   ) {
      what.push("buildSQL");
   }
   return what;
}

const persists = (text: string): boolean =>
   /^\s*##!\s*experimental[^\n]*persistence/m.test(text) &&
   /#@\s*(persist|preaggregate)/.test(text);

/** The persist fixtures, each compiled as its own package. */
function fixtureModels(): CorpusModel[] {
   const out: CorpusModel[] = [];
   for (const dir of fs.readdirSync(FIXTURES)) {
      if (!dir.startsWith("persist-")) continue;
      const root = path.join(FIXTURES, dir);
      const files = new Map<string, string>();
      const malloys = fs.readdirSync(root).filter((f) => f.endsWith(".malloy"));
      for (const f of malloys) {
         files.set(
            pathToFileURL(path.join(root, f)).href,
            fs.readFileSync(path.join(root, f), "utf8"),
         );
      }
      for (const f of malloys) {
         const text = fs.readFileSync(path.join(root, f), "utf8");
         if (!persists(text)) continue;
         out.push({
            label: `fixtures/${dir}/${f}`,
            files,
            entry: pathToFileURL(path.join(root, f)).href,
            connections: new Map([["duckdb", connection("duckdb", root)]]),
         });
      }
   }
   return out;
}

/** A hammer scenario's `## Data conn.table` tables and `## Model` files. */
interface Scenario {
   id: string;
   pkg: string;
   models: Map<string, string>;
   data: {
      conn: string;
      table: string;
      cols: [string, string][];
      rows: string[][];
   }[];
}

function parseScenario(dir: string): Scenario {
   // A Windows checkout has CRLF line endings; the section and table parsing
   // below splits on "\n", so a trailing "\r" would leave every header
   // unmatched and the scenario's models silently out of the corpus.
   const text = fs
      .readFileSync(path.join(dir, "scenario.md"), "utf8")
      .replace(/\r\n/g, "\n");
   const pkg = /^package:\s*(\S+)/m.exec(text)?.[1] ?? path.basename(dir);
   const models = new Map<string, string>();
   const data: Scenario["data"] = [];
   const sections = text.split(/^## /m).slice(1);
   for (const section of sections) {
      const [header, ...rest] = section.split("\n");
      const body = rest.join("\n");
      const model = /^Model\s+(?:\([^)]*\)\s*)?(\S+\.malloy)/.exec(header);
      if (model) {
         const code = /```malloy\n([\s\S]*?)```/.exec(body)?.[1];
         const rel = model[1].includes("/") ? model[1] : `${pkg}/${model[1]}`;
         if (code !== undefined && !models.has(rel)) models.set(rel, code);
         continue;
      }
      const seed = /^Data\s+(?:\([^)]*\)\s*)?([\w-]+)\.([\w.]+)/.exec(header);
      if (seed) {
         const lines = body
            .split("\n")
            .filter((l) => l.trim().startsWith("|"))
            .map((l) =>
               l
                  .trim()
                  .replace(/^\||\|$/g, "")
                  .split("|")
                  .map((c) => c.trim()),
            );
         if (lines.length < 2) continue;
         const cols = lines[0].map((c) => {
            const [name, type] = c.split(":");
            return [name, type ?? "str"] as [string, string];
         });
         const rows = lines.slice(2);
         data.push({ conn: seed[1], table: seed[2], cols, rows });
      }
   }
   return { id: path.basename(dir), pkg, models, data };
}

const DUCK_TYPES: Record<string, string> = {
   int: "BIGINT",
   num: "DOUBLE",
   float: "DOUBLE",
   date: "DATE",
   ts: "TIMESTAMP",
   timestamp: "TIMESTAMP",
   bool: "BOOLEAN",
};

const sqlString = (v: string) => `'${v.replace(/'/g, "''")}'`;

/** Each scenario's persisting models, over DuckDB stand-ins for its data. */
async function hammerModels(): Promise<{
   models: CorpusModel[];
   skipped: string[];
}> {
   const models: CorpusModel[] = [];
   const skipped: string[] = [];
   const dirs: string[] = [];
   for (const group of fs.readdirSync(HAMMER)) {
      const groupDir = path.join(HAMMER, group);
      if (!fs.statSync(groupDir).isDirectory()) continue;
      for (const s of fs.readdirSync(groupDir)) {
         const dir = path.join(groupDir, s);
         if (fs.existsSync(path.join(dir, "scenario.md"))) dirs.push(dir);
      }
   }
   for (const dir of dirs) {
      const scenario = parseScenario(dir);
      const persisting = [...scenario.models].filter(([, t]) => persists(t));
      if (persisting.length === 0) continue;
      try {
         const connections = new Map<string, DuckDBConnection>();
         const conn = (name: string) => {
            let c = connections.get(name);
            if (!c) {
               c = connection(name);
               connections.set(name, c);
            }
            return c;
         };
         conn("duckdb");
         for (const d of scenario.data) {
            const c = conn(d.conn);
            // Postgres's default schema, which the models name.
            const parts = d.table.includes(".")
               ? d.table.split(".")
               : ["public", d.table];
            const quoted = parts.map((p) => `"${p}"`).join(".");
            await c.runSQL(`CREATE SCHEMA IF NOT EXISTS "${parts[0]}"`);
            const cols = d.cols
               .map(([n, t]) => `"${n}" ${DUCK_TYPES[t] ?? "VARCHAR"}`)
               .join(", ");
            await c.runSQL(`CREATE OR REPLACE TABLE ${quoted} (${cols})`);
            for (const row of d.rows) {
               const values = row
                  .map((v, i) =>
                     v === "" || v === "null"
                        ? "NULL"
                        : `CAST(${sqlString(v)} AS ${DUCK_TYPES[d.cols[i]?.[1]] ?? "VARCHAR"})`,
                  )
                  .join(", ");
               await c.runSQL(`INSERT INTO ${quoted} VALUES (${values})`);
            }
         }
         // A model may name a connection only its scenario's server config
         // declares; give every connection a model reads a stand-in.
         for (const [, text] of scenario.models) {
            for (const m of text.matchAll(
               /\b([A-Za-z_]\w*)\.(?:table|sql)\(/g,
            )) {
               conn(m[1]);
            }
         }
         const files = new Map<string, string>();
         for (const [rel, text] of scenario.models) {
            files.set(`file:///hammer/${rel}`, text);
         }
         for (const [rel] of persisting) {
            models.push({
               label: `hammer/${scenario.id}/${rel}`,
               files,
               entry: `file:///hammer/${rel}`,
               connections,
            });
         }
      } catch (err) {
         skipped.push(
            `${scenario.id}: ${(err as Error).message.split("\n")[0]}`,
         );
      }
   }
   return { models, skipped };
}

describe("build-target addresses over the repo's persist corpus", () => {
   it(
      "no source's address, compiler key, wire SQL or build SQL moves",
      async () => {
         const hammer = await hammerModels();
         const corpus = [...fixtureModels(), ...hammer.models];
         const moved: Moved[] = [];
         const uncompiled: string[] = [...hammer.skipped];
         let checked = 0;
         for (const model of corpus) {
            try {
               const result = await compare(model);
               if (!result) continue;
               checked += result.checked.length;
               moved.push(...result.moved);
            } catch (err) {
               uncompiled.push(
                  `${model.label}: ${(err as Error).message.slice(0, 300).replace(/\n/g, " ")}`,
               );
            }
         }
         // The corpus must be real: a parser or loader regression that compiled
         // nothing would pass vacuously.
         expect(checked).toBeGreaterThan(100);
         if (uncompiled.length > 0) {
            console.log(
               `build-target corpus: ${uncompiled.length} model(s) did not compile ` +
                  `against the DuckDB stand-in and were not compared:\n  ${uncompiled.join("\n  ")}`,
            );
         }
         console.log(
            `build-target corpus: ${checked} sources compared across ${corpus.length - uncompiled.length} models`,
         );
         expect(moved).toEqual([]);
      },
      { timeout: 180000 },
   );
});
