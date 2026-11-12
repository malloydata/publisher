// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The repo's persist corpus, for specs that check a rule over every real model
 * that declares a persisted source:
 *
 *  - the persist fixtures under `tests/fixtures/persist-*`, compiled as their
 *    packages are, against their own data files;
 *  - every hammer scenario's models, compiled against DuckDB with each
 *    scenario's `## Data` tables created under the connection and schema the
 *    model reads them by.
 *
 * Call {@link closeCorpus} from the spec's `afterAll`.
 */

import { DuckDBConnection } from "@malloydata/db-duckdb";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { pathToFileURL } from "url";

const REPO = path.resolve(__dirname, "../../../..");
const FIXTURES = path.join(REPO, "packages/server/tests/fixtures");
const HAMMER = path.join(REPO, "hammer/scenarios");

// Shared by every spec file that loads the corpus: one test run keeps one
// module instance, so a spec's close must leave the helper usable by the next.
const opened: DuckDBConnection[] = [];
let scratch: string | undefined;

function scratchDir(): string {
   scratch ??= fs.mkdtempSync(path.join(os.tmpdir(), "persist-corpus-"));
   return scratch;
}

/** Close every connection the corpus opened and remove its scratch files. */
export async function closeCorpus(): Promise<void> {
   for (const c of opened.splice(0)) await c.close().catch(() => {});
   if (scratch) fs.rmSync(scratch, { recursive: true, force: true });
   scratch = undefined;
}

/** One model to plan: its files, entry file, and connections. */
export interface CorpusModel {
   label: string;
   files: Map<string, string>;
   entry: string;
   connections: Map<string, DuckDBConnection>;
}

function connection(name: string, workingDirectory?: string): DuckDBConnection {
   const dbPath = path.join(
      scratchDir(),
      `${name}-${opened.length}-${Math.random().toString(36).slice(2)}.db`,
   );
   const c = workingDirectory
      ? new DuckDBConnection(name, dbPath, workingDirectory)
      : new DuckDBConnection(name, dbPath);
   opened.push(c);
   return c;
}

const persists = (text: string): boolean =>
   /^\s*##!\s*experimental[^\n]*persistence/m.test(text) &&
   /#@\s*(persist|preaggregate)/.test(text);

/** The persist fixtures, each compiled as its own package. */
export function fixtureModels(): CorpusModel[] {
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
export async function hammerModels(): Promise<{
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
