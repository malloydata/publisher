// Markdown scenario interpreter (FitNesse-style). A scenario is a folder with a
// `scenario.md` that reads like a story — starting data as tables, Malloy in code
// blocks, a publish step, and query/expect pairs — plus an optional `hooks.ts`
// escape hatch for exotic steps (e.g. orchestrated/operator builds).
//
// A parsed scenario implements the same `Scenario` interface the TS scenarios did
// (`packages` + `sourceTables` for the orchestrator's up-front setup, `run()` as a
// step executor), so the lib/ + orchestrator layer is unchanged.
//
// Section grammar (ASCII-friendly, human-authored):
//   ## Publisher [<name>]               [body: `- PERSIST_STORAGE_MODE: on`, plus any
//                                       `- SOME_ENV: value` bullets passed as extra env]  -> (re)start
//                                       the publisher at that mode; following steps inherit it
//   ## Data <conn>.<table>              + a GFM table (headers `name:type`)  -> seed
//   ## Mutate <conn>.<table>            + a GFM table (append) OR a ```sql block
//   ## Model [<pkg>/]<path.malloy>      + a ```malloy block                  -> write model
//   ## Publish [<pkg>] [(forceRefresh, sources=a[+b])]  [body: `expect binding: src -> conn`]
//   ## Delete [<pkg>]                    -> unload + DELETE the package from serving
//   ## Reclaim [<pkg>]                   -> DELETE the latest materialization with dropTables
//                                        (destination-aware physical-table drop / GC)
//   ## Build refused [<pkg>]             [body: `cites: <substring>`, `excludes: <substring>`;
//                                        `${pg.password}` / `${pg.user}` / `${pg.host}` are
//                                        substituted with the throwaway container's values, so a
//                                        redaction check can name a secret only the harness knows]
//   ## Compile <label> (pkg=P[, refused]) + ```malloy (appended source) + `cites:` -> /compile
//   ## Warns [<pkg>]                     [body: `cites: <substring>`]  -> package warning
//   ## Republish [refused] [<pkg>]       [body: `cites: <substring>`]  -> POST /packages (the
//                                        author-in-the-loop publish gate); `refused` asserts a 4xx
//   ## Bind [<pkg>] [(empty|clear|bad|from=<publisher>)]  -> orchestrator manifestLocation PATCH
//   ## Query <label> [(again|refused)]   + ```malloy (or reuse last) + Expect: GFM table
//                                        [body: `givens: NAME=v; OTHER=v` to supply runtime
//                                        givens; `columns: exact` to assert the Expect table's
//                                        columns are the COMPLETE result column set]
//   ## Build targets [(pkg=P)]           + GFM table `source | writes [| entity]` -> assert the
//                                        compiled build plan's persist sources and the physical
//                                        name each writes. An `entity` column groups by content
//                                        address: same label = same sourceEntityId, different
//                                        labels must differ (the label is arbitrary).
//   ## Note | ## Attention              -> a prose callout (`> …` blockquote),
//                                          surfaced in the report's attention block
//   ## Connection <name> (type=postgres|ducklake)  -> DECLARE a connection wired into the
//                                        config (pre-pass; a postgres points at the source
//                                        warehouse, a ducklake gets its own catalog+storage).
//                                        WITHOUT type= (and with a ```sql block) it RUNS SQL —
//                                        add an Expect: table to compare its rows, or `(rows=<n>)`
//                                        to assert only the count (an order-independent claim).
//   ## Hook <exportName>                -> call hooks.ts export(api, assert)
// Server-facing steps (Publish/Query/Build/Compile/Warns/Bind/Connection/Rejected)
// accept `(pub=<name>)` to target a specific started publisher instead of the
// active one — e.g. query p1 and p2 side by side without switching active — and
// `(env=<name>)` to target a specific environment (default: the primary env; a
// publisher serves all configured environments). `## Model <pkg>/<path> (env=…)`
// registers the package under that environment.
// Front matter: `id`, `package`, `title`, `tags: a, b`, `requires: dialect:x`.

import path from "path";
import type { PersistStorageMode } from "./server";
import { Rest } from "./rest";
import { sleep } from "./util";

/** The default environment every scenario runs in unless a step says `(env=…)`. */
const PRIMARY_ENV = "default";
import { Assert, type ConnectionDecl, type PackageSpec, type Scenario, type ScenarioContext, type SourceTable } from "../scenarios/framework";

interface Col {
   name: string;
   type: string;
}
interface Table {
   cols: Col[];
   rows: string[][];
}

// Server-facing steps carry an optional `env` (from `(env=…)`), selecting which
// environment the step runs against; it defaults to PRIMARY_ENV. A publisher
// process serves every configured environment, so env is orthogonal to `pub`.
type Step =
   | { kind: "model"; env: string; pkg: string; path: string; malloy: string }
   | { kind: "publish"; pub?: string; env: string; pkg: string; mode: PersistStorageMode; bindings: { source: string; conn: string }[]; forceRefresh: boolean; sourceNames?: string[]; async: boolean; label?: string }
   | { kind: "await"; label?: string }
   | { kind: "orchestratedBuild"; pub?: string; env: string; pkg: string; mode: PersistStorageMode; refused: boolean; strict: boolean; sources: { src: string; name: string; dest: string }[]; references: { src: string; from?: string }[]; cites?: string; excludes?: string }
   | { kind: "delete"; pub?: string; env: string; pkg: string; mode: PersistStorageMode }
   | { kind: "reclaim"; pub?: string; env: string; pkg: string; mode: PersistStorageMode }
   | { kind: "buildRefused"; pub?: string; env: string; pkg: string; mode: PersistStorageMode; cites: string; excludes?: string }
   | { kind: "query"; pub?: string; env: string; pkg: string; label: string; mode: PersistStorageMode; malloy?: string; again: boolean; refused: boolean; cites?: string; expect?: Table; givens?: Record<string, string>; exactColumns: boolean }
   | { kind: "buildTargets"; pub?: string; env: string; pkg: string; mode: PersistStorageMode; expect: Table }
   | { kind: "mutate"; conn: string; table: string; rows?: Table; sql?: string }
   | { kind: "sql"; label: string; sql: string; expect: Table }
   | { kind: "operator"; conn: string; mode: PersistStorageMode; sql: string }
   | { kind: "connection"; pub?: string; env: string; conn: string; mode: PersistStorageMode; sql: string; refused: boolean; cites?: string; expect?: Table; expectRows?: number }
   | { kind: "rejected"; pub?: string; env: string; pkg: string; mode: PersistStorageMode; cites?: string }
   | { kind: "warns"; pub?: string; env: string; pkg: string; mode: PersistStorageMode; cites: string }
   | { kind: "compile"; pub?: string; env: string; pkg: string; label: string; mode: PersistStorageMode; source: string; refused: boolean; cites?: string }
   | { kind: "bind"; pub?: string; env: string; pkg: string; mode: PersistStorageMode; variant: "full" | "empty" | "clear" | "bad"; from?: string; fresh?: number; asof?: string; fallback?: string }
   | { kind: "publisher"; mode: PersistStorageMode; name?: string; extraEnv?: Record<string, string> }
   | { kind: "republish"; pub?: string; env: string; pkg: string; mode: PersistStorageMode; refused: boolean; cites?: string }
   | { kind: "restart"; mode: PersistStorageMode; init: boolean }
   | { kind: "hook"; name: string };

interface ParsedMd {
   id: string;
   title: string;
   tags: string[];
   requires: string[];
   note?: { since?: string; text: string };
   defaultPackage: string;
   steps: Step[];
   dataSeeds: { conn: string; table: string; data: Table }[];
   connectionDecls: ConnectionDecl[];
}

/**
 * Per-section grammar: the header attributes `(k=v, flag)` and the body keys
 * `key: value` each section kind actually READS. Anything else is a parse error.
 *
 * This exists because the failure it prevents is invisible: a misspelled
 * `excldues:` or `(row=1)` is simply not read, the assertion it was supposed to
 * carry never runs, and the scenario passes — reporting success for a check that
 * does not exist. The same goes for a key that is valid on a DIFFERENT kind (an
 * `excludes:` on a step whose branch never consumed it, which is precisely how a
 * silently-passing redaction check reached this suite). Strictness here converts
 * every one of those into a loud failure at load, before anything runs.
 *
 * Adding a handler means adding its keys here — the parse error names the section
 * and lists what is legal, so the next author is told rather than left guessing.
 */
const UNIVERSAL_ATTRS = ["pub", "env"] as const;
const SECTION_SPEC: Record<string, { attrs?: string[]; keys?: string[] }> = {
   note: { attrs: ["since"] },
   attention: { attrs: ["since"] },
   // A bare mode reads better than `mode=off` and is already in use; both work.
   publisher: { attrs: ["mode", "off", "write-only", "on"] },
   data: {},
   mutate: {},
   model: {},
   publish: { attrs: ["sources", "forcerefresh", "async", "label"] },
   await: { attrs: ["label"] },
   delete: {},
   reclaim: {},
   build: {
      attrs: ["orchestrated", "strict", "pkg"],
      // `reference:` is an orchestrated-build body line, not an assertion key.
      keys: ["cites", "excludes", "reference"],
   },
   query: { attrs: ["again", "refused", "pkg"], keys: ["cites", "givens", "columns"] },
   sql: {},
   operator: {},
   connection: { attrs: ["type", "refused", "rows"], keys: ["cites"] },
   rejected: { keys: ["cites"] },
   warns: { keys: ["cites"] },
   republish: { attrs: ["refused"], keys: ["cites"] },
   compile: { attrs: ["pkg", "refused"], keys: ["cites"] },
   bind: { attrs: ["bad", "empty", "clear", "from", "fresh", "asof", "fallback"] },
   restart: { attrs: ["init"] },
   hook: {},
};

/**
 * Step kinds that legitimately assert NOTHING — they exist for their side effect
 * (write a model, seed rows, boot a publisher, run operator DDL, PATCH a manifest).
 * Every other kind must contribute at least one check, or the step ran and verified
 * nothing: a `## Connection` with neither an `Expect:` table nor `(rows=N)` executes
 * its SQL and passes silently, which reads in the report exactly like a real check.
 *
 * Measured at RUN time as a delta on the check count, not statically: several steps
 * assert only through helpers (`compareRows`), so counting `assert.` calls in the
 * source would undercount and exempt the wrong kinds.
 *
 * `publish` is exempt for now because `expect binding:` lines are optional and most
 * scenarios publish purely to trigger a build. That leaves the analogous hole a
 * misspelled `expect bindng:` would fall into — see the note in the commit; it wants
 * a line-shape check on the publish body rather than an accounting rule.
 *
 * `hook` is exempt deliberately: a hook is sometimes pure setup (dropping a catalog),
 * and requiring a check there only teaches authors to write a tautological
 * `assert.ok(..., true)` to satisfy the rule, which is worse than nothing.
 */
export function stepMustAssert(kind: string): boolean {
   return !SIDE_EFFECT_ONLY_STEPS.has(kind);
}

const SIDE_EFFECT_ONLY_STEPS = new Set([
   "model",
   "mutate",
   "operator",
   "publisher",
   "restart",
   "bind",
   "hook",
   "publish",
]);

/**
 * Candidate body keys in a section: a lowercase `word:` at the start of a line.
 * Fenced code blocks are skipped (Malloy's `group_by:` is not a body key), as are
 * blockquotes, table rows, and bullets (`- PERSIST_STORAGE_MODE: on` is publisher
 * env, and `expect binding: …` has a space before the colon so it never matches).
 * Prose is excluded in practice by the lowercase-first-word rule.
 */
function bodyKeys(body: string[]): string[] {
   const keys = new Set<string>();
   let inFence = false;
   for (const raw of body) {
      if (/^\s*```/.test(raw)) {
         inFence = !inFence;
         continue;
      }
      if (inFence) continue;
      if (/^\s*[>|\-*]/.test(raw)) continue;
      const m = raw.match(/^ {0,3}([a-z][a-z0-9_]*)\s*:/);
      if (m) keys.add(m[1].toLowerCase());
   }
   return [...keys];
}

/** Reject unknown attributes / body keys on a section (see {@link SECTION_SPEC}). */
function validateSection(
   kind: string,
   header: string,
   attrs: Record<string, string | boolean>,
   body: string[],
): void {
   const spec = SECTION_SPEC[kind];
   if (!spec) return; // unknown kind is reported by the switch's default
   const legalAttrs = new Set<string>([...(spec.attrs ?? []), ...UNIVERSAL_ATTRS]);
   for (const a of Object.keys(attrs)) {
      if (!legalAttrs.has(a)) {
         throw new Error(
            `## ${header}: unknown attribute "${a}". Valid for "${kind}": ` +
               `${[...legalAttrs].sort().join(", ")}`,
         );
      }
   }
   const legalKeys = new Set(spec.keys ?? []);
   for (const k of bodyKeys(body)) {
      if (!legalKeys.has(k)) {
         throw new Error(
            `## ${header}: unknown body key "${k}:". ` +
               (legalKeys.size
                  ? `Valid for "${kind}": ${[...legalKeys].sort().join(", ")}`
                  : `"${kind}" takes no body keys`),
         );
      }
   }
}

/** Split a `key: a, b, c` front-matter value into a trimmed, non-empty list. */
function csv(value: string | undefined): string[] {
   return (value ?? "")
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
}

// ─────────────────────────── parsing ───────────────────────────

function parseMarkdown(text: string, fallbackId: string): ParsedMd {
   const lines = text.split("\n");
   let i = 0;

   // Optional YAML-ish front matter: a leading `---` … `---` block of key: value.
   const fm: Record<string, string> = {};
   if (lines[0]?.trim() === "---") {
      i = 1;
      for (; i < lines.length && lines[i].trim() !== "---"; i++) {
         const m = lines[i].match(/^([a-zA-Z_][\w-]*)\s*:\s*(.+)$/);
         if (m) fm[m[1].toLowerCase()] = m[2].trim();
      }
      i++; // skip the closing ---
   }

   const id = fm.id ?? fallbackId;
   const tags = csv(fm.tags);
   const requires = csv(fm.requires);
   const defaultPackage = fm.package ?? id.toLowerCase();
   let title = fm.title ?? id;

   // Title (H1) — everything else before the first `## ` is prose.
   for (; i < lines.length && !lines[i].startsWith("## "); i++) {
      const h1 = lines[i].trim().match(/^#\s+(.*)$/);
      if (h1) title = h1[1].trim();
   }

   // Split the remainder into sections.
   const sections: { header: string; body: string[] }[] = [];
   let cur: { header: string; body: string[] } | null = null;
   for (; i < lines.length; i++) {
      const line = lines[i];
      if (line.startsWith("## ")) {
         if (cur) sections.push(cur);
         cur = { header: line.slice(3).trim(), body: [] };
      } else if (cur) {
         cur.body.push(line);
      }
   }
   if (cur) sections.push(cur);

   const steps: Step[] = [];
   const dataSeeds: { conn: string; table: string; data: Table }[] = [];
   const connectionDecls: ConnectionDecl[] = [];

   // PERSIST_STORAGE_MODE is a server-level setting fixed at process start — you
   // change it by RESTARTING the publisher, not per request. So it is not a
   // per-action attribute: a `## Publisher` section (re)starts the publisher at a
   // declared mode, and every subsequent step runs against that publisher until
   // the next `## Publisher`. `currentMode` tracks the running publisher's mode
   // as we walk the sections in order; steps inherit it (there is no per-step
   // `(mode=…)` override — switching modes means a new `## Publisher`).
   let currentMode: PersistStorageMode = (fm.mode as PersistStorageMode) ?? "on";
   let note: { since?: string; text: string } | undefined;

   for (const sec of sections) {
      const { kind, arg, attrs } = parseHeader(sec.header);
      validateSection(kind, sec.header, attrs, sec.body);
      validateSubstitutions(sec.header, sec.body);
      if (kind === "note" || kind === "attention") {
         // A prose callout, not a step. Strip leading `> ` blockquote markers so
         // the stored note is clean; the .md keeps them for nicer rendering.
         // `## Note (since=YYYY-MM-DD)` records when the concern was raised, so
         // the report can age it (surface follow-ups ignored for too long).
         const text = sec.body
            .map((l) => l.replace(/^\s*>\s?/, "").trimEnd())
            .join("\n")
            .trim();
         const since = attrs.since as string | undefined;
         if (text) {
            note = note
               ? { since: note.since ?? since, text: `${note.text}\n\n${text}` }
               : { since, text };
         }
         continue;
      }
      if (kind === "publisher") {
         // `## Publisher (off)` — a bare mode flag. Resolved here as well as from
         // `mode=` and the body bullet, so the header form is not silently
         // decorative (it was, until the strict parse surfaced it).
         const bareMode = (["off", "write-only", "on"] as const).find(
            (candidate) => attrs[candidate] === true,
         );
         const m =
            (attrs.mode as PersistStorageMode) ??
            bareMode ??
            extractPublisherMode(sec.body);
         if (m) currentMode = m;
         const extraEnv = extractPublisherEnv(sec.body);
         steps.push({
            kind: "publisher",
            mode: currentMode,
            name: arg.trim() || undefined,
            extraEnv: Object.keys(extraEnv).length ? extraEnv : undefined,
         });
         continue;
      }
      const mode = currentMode;
      // `(pub=<name>)` targets a specific (already-started) publisher for this
      // step, instead of the active one — so a scenario can query p1 and p2
      // side by side without switching the active publisher between them.
      const pub = attrs.pub as string | undefined;
      // `(env=<name>)` targets a specific environment (a publisher serves all of
      // them); defaults to the primary environment. Model declarations use it to
      // register the package under that environment.
      const env = (attrs.env as string) || PRIMARY_ENV;
      switch (kind) {
         case "data": {
            const [conn, table] = splitConnTable(arg);
            dataSeeds.push({ conn, table, data: requireDataTable(sec.body, sec.header) });
            break;
         }
         case "mutate": {
            const [conn, table] = splitConnTable(arg);
            const sql = extractCode(sec.body, "sql");
            if (sql) steps.push({ kind: "mutate", conn, table, sql });
            else steps.push({ kind: "mutate", conn, table, rows: requireDataTable(sec.body, sec.header) });
            break;
         }
         case "model": {
            const { pkg, rel } = splitPkgPath(arg, defaultPackage);
            const malloy = extractCode(sec.body, "malloy");
            if (!malloy) throw new Error(`## Model ${arg}: missing a \`\`\`malloy block`);
            steps.push({ kind: "model", env, pkg, path: rel, malloy });
            break;
         }
         case "publish": {
            const pkg = arg.trim() || defaultPackage;
            const bindings = parseBindings(sec.body);
            // `(sources=a)` builds ONLY the named persist source(s) — the
            // `sourceNames` build filter. Multiple names are `+`-separated
            // (comma is the attribute delimiter). Omitted = build them all.
            const sourceNames = attrs.sources
               ? String(attrs.sources).split("+").map((s) => s.trim()).filter(Boolean)
               : undefined;
            // `(async[, label=x])` fires the build WITHOUT awaiting completion, so a
            // following step can observe it in flight (e.g. a conflicting build);
            // `## Await x` (or scenario teardown) drains it. Bindings can't be
            // asserted on an async publish — it hasn't finished.
            steps.push({ kind: "publish", pub, env, pkg, mode, bindings, forceRefresh: !!attrs.forcerefresh, sourceNames, async: !!attrs.async, label: attrs.label as string | undefined });
            break;
         }
         case "await": {
            steps.push({ kind: "await", label: arg.trim() || (attrs.label as string) || undefined });
            break;
         }
         case "delete": {
            steps.push({ kind: "delete", pub, env, pkg: arg.trim() || defaultPackage, mode });
            break;
         }
         case "reclaim": {
            steps.push({ kind: "reclaim", pub, env, pkg: arg.trim() || defaultPackage, mode });
            break;
         }
         case "build": {
            // "## Build targets" asserts what the compiled build plan CONTAINS
            // (a source -> written-name table); it runs nothing.
            // "## Build refused …" (arg begins with "refused") asserts a build
            // fails/refuses. "## Build (orchestrated, …)" runs a caller-instructed
            // build; combine as "## Build refused (orchestrated, …)".
            if (/^targets\b/i.test(arg)) {
               steps.push({
                  kind: "buildTargets",
                  pub,
                  env,
                  pkg: (attrs.pkg as string) ?? defaultPackage,
                  mode,
                  expect: requireExpectTable(sec.body, sec.header),
               });
               break;
            }
            const refused = /^refused\b/i.test(arg);
            const pkg =
               (attrs.pkg as string) ??
               (arg.replace(/^refused/i, "").trim() || defaultPackage);
            if (attrs.orchestrated) {
               const { sources, references } = parseOrchestratedBody(sec.body);
               steps.push({
                  kind: "orchestratedBuild",
                  pub,
                  env,
                  pkg,
                  mode,
                  refused,
                  strict: !!attrs.strict,
                  sources,
                  references,
                  cites: firstKey(sec.body, "cites"),
                  excludes: firstKey(sec.body, "excludes"),
               });
            } else {
               const cites = firstKey(sec.body, "cites") ?? "";
               steps.push({ kind: "buildRefused", pub, env, pkg, mode, cites, excludes: firstKey(sec.body, "excludes") });
            }
            break;
         }
         case "query": {
            const again = !!attrs.again;
            const refused = !!attrs.refused;
            const pkg = (attrs.pkg as string) ?? defaultPackage;
            const malloy = extractCode(sec.body, "malloy");
            // `givens: NAME=value; OTHER=value` — runtime givens supplied with the
            // query, so a scenario can exercise a given-carrying request in
            // markdown instead of a hook.
            const givens = parseKeyValues(firstKey(sec.body, "givens"));
            // `columns: exact` — the Expect table's columns are the COMPLETE result
            // column set. Needed because compareRows only checks the columns it was
            // given, so an unexpected EXTRA column is otherwise invisible.
            const exactColumns = /^exact$/i.test(firstKey(sec.body, "columns") ?? "");
            if (refused) {
               steps.push({ kind: "query", pub, env, pkg, label: arg.trim(), mode, malloy, again, refused: true, cites: firstKey(sec.body, "cites"), givens, exactColumns });
            } else {
               steps.push({ kind: "query", pub, env, pkg, label: arg.trim(), mode, malloy, again, refused: false, expect: requireExpectTable(sec.body, sec.header), givens, exactColumns });
            }
            break;
         }
         case "sql": {
            const sql = extractCode(sec.body, "sql");
            if (!sql) throw new Error(`## SQL ${arg}: missing a \`\`\`sql block`);
            steps.push({ kind: "sql", label: arg.trim() || "sql", sql, expect: requireExpectTable(sec.body, sec.header) });
            break;
         }
         case "operator": {
            const sql = extractCode(sec.body, "sql");
            if (!sql) throw new Error(`## Operator ${arg}: missing a \`\`\`sql block`);
            steps.push({ kind: "operator", conn: arg.trim(), mode, sql });
            break;
         }
         case "connection": {
            // Two forms share this header. With a `(type=…)` attribute it DECLARES
            // a connection to wire into the config (a pre-pass artifact, like a
            // package — not a runtime step); otherwise it RUNS SQL against an
            // existing connection (the original behavior).
            if (attrs.type) {
               const kind = String(attrs.type).toLowerCase();
               if (kind !== "postgres" && kind !== "ducklake" && kind !== "duckdb") {
                  throw new Error(
                     `## Connection ${arg}: unsupported type="${attrs.type}" (expected postgres | ducklake | duckdb)`,
                  );
               }
               connectionDecls.push({ env, name: arg.trim(), kind });
               break;
            }
            const sql = extractCode(sec.body, "sql");
            if (!sql) throw new Error(`## Connection ${arg}: missing a \`\`\`sql block`);
            // A non-refused `## Connection` can assert its result: a GFM table
            // compares rows, or `rows=<n>` asserts only the count (for an
            // order-independent claim like "exactly one of these tables exists").
            steps.push({
               kind: "connection", pub, env, conn: arg.trim(), mode, sql,
               refused: !!attrs.refused, cites: firstKey(sec.body, "cites"),
               expect: attrs.refused ? undefined : parseExpectTable(sec.body),
               expectRows: attrs.rows !== undefined ? Number(attrs.rows) : undefined,
            });
            break;
         }
         case "rejected": {
            steps.push({ kind: "rejected", pub, env, pkg: arg.trim() || defaultPackage, mode, cites: firstKey(sec.body, "cites") });
            break;
         }
         case "warns": {
            steps.push({ kind: "warns", pub, env, pkg: arg.trim() || defaultPackage, mode, cites: firstKey(sec.body, "cites") ?? "" });
            break;
         }
         case "republish": {
            // Re-publish a package through the POST /packages endpoint (the
            // author-in-the-loop gate), distinct from `## Publish` (a build). With
            // `(refused)` it asserts the publish is rejected (e.g. a collision
            // under PERSIST_COLLISION_ENFORCE) and cites the reason.
            steps.push({
               kind: "republish",
               pub,
               env,
               pkg: arg.replace(/^refused/i, "").trim() || defaultPackage,
               mode,
               refused: /^refused\b/i.test(arg) || !!attrs.refused,
               cites: firstKey(sec.body, "cites"),
            });
            break;
         }
         case "compile": {
            // Compile-check a model on demand (POST /compile), without loading the
            // package into the serving set — the deterministic way to assert a
            // model does/doesn't compile. `source` is appended to the target model
            // for namespace context; omit it to compile the model as-is.
            const pkg = (attrs.pkg as string) ?? defaultPackage;
            const source = extractCode(sec.body, "malloy") ?? "";
            steps.push({ kind: "compile", pub, env, pkg, label: arg.trim() || "compile", mode, source, refused: !!attrs.refused, cites: firstKey(sec.body, "cites") });
            break;
         }
         case "bind": {
            // Simulate the orchestrator binding a manifest: full = re-serve the
            // last build's manifest via manifestLocation; empty = a present-but-
            // empty manifest (host says "nothing to serve" → live); clear =
            // manifestLocation:null (revert to the publisher's local-store rebind).
            const variant = attrs.bad ? "bad" : attrs.empty ? "empty" : attrs.clear ? "clear" : "full";
            // `fresh=<seconds>` / `asof=<iso>` / `fallback=<live|stale_ok|fail>`
            // stamp freshness fields onto each bound entry — for exercising the
            // freshness gate (age = now - asof vs the window).
            steps.push({
               kind: "bind",
               pub,
               env,
               pkg: arg.trim() || defaultPackage,
               mode,
               variant,
               from: attrs.from as string | undefined,
               fresh: attrs.fresh !== undefined ? Number(attrs.fresh) : undefined,
               asof: attrs.asof as string | undefined,
               fallback: attrs.fallback as string | undefined,
            });
            break;
         }
         case "restart": {
            // `(init)` reboots with --init: re-copies packages (picks up a mid-run
            // `## Model` edit) and resets the store. Bare `## Restart` preserves the
            // materialization store (no --init).
            steps.push({ kind: "restart", mode, init: !!attrs.init });
            break;
         }
         case "hook": {
            steps.push({ kind: "hook", name: arg.trim() });
            break;
         }
         default:
            throw new Error(`Unknown section kind "${kind}" in header: ## ${sec.header}`);
      }
   }

   return { id, title, tags, requires, note, defaultPackage, steps, dataSeeds, connectionDecls };
}

/**
 * The publisher's `PERSIST_STORAGE_MODE` from a `## Publisher` section body.
 * Accepts a `- PERSIST_STORAGE_MODE: on` bullet, a `KEY: value` line, or a
 * `| PERSIST_STORAGE_MODE | on |` table row — whatever reads cleanest.
 */
function extractPublisherMode(body: string[]): PersistStorageMode | undefined {
   const m = body
      .join("\n")
      .match(/PERSIST_STORAGE_MODE\s*[:|]?\s*\|?\s*(off|write-only|on)\b/i);
   return m ? (m[1].toLowerCase() as PersistStorageMode) : undefined;
}

/**
 * Extra environment variables from a `## Publisher` body — every
 * `- KEY: value` (or `KEY: value`) bullet whose key is a SCREAMING_SNAKE env name,
 * EXCEPT `PERSIST_STORAGE_MODE` (which is the mode, handled separately). Lets a
 * scenario boot a publisher with a deployment flag fixed at process start, e.g.
 * `- PERSIST_COLLISION_ENFORCE: true`.
 */
function extractPublisherEnv(body: string[]): Record<string, string> {
   const env: Record<string, string> = {};
   for (const raw of body) {
      const m = raw.match(/^\s*-?\s*([A-Z][A-Z0-9_]*)\s*[:=]\s*(.+?)\s*$/);
      if (m && m[1] !== "PERSIST_STORAGE_MODE") env[m[1]] = m[2].trim();
   }
   return env;
}

function parseHeader(header: string): { kind: string; arg: string; attrs: Record<string, string | boolean> } {
   // Trailing "(k=v, flag, ...)" is attributes.
   const attrs: Record<string, string | boolean> = {};
   let h = header;
   const paren = h.match(/\(([^)]*)\)\s*$/);
   if (paren) {
      h = h.slice(0, paren.index).trim();
      for (const part of paren[1].split(",")) {
         const kv = part.trim();
         if (!kv) continue;
         const eq = kv.indexOf("=");
         if (eq >= 0) attrs[kv.slice(0, eq).trim().toLowerCase()] = kv.slice(eq + 1).trim();
         else attrs[kv.toLowerCase()] = true;
      }
   }
   const sp = h.indexOf(" ");
   const kind = (sp < 0 ? h : h.slice(0, sp)).toLowerCase();
   const arg = sp < 0 ? "" : h.slice(sp + 1).trim();
   return { kind, arg, attrs };
}

function splitConnTable(arg: string): [string, string] {
   const dot = arg.indexOf(".");
   if (dot < 0) throw new Error(`expected <conn>.<table>, got "${arg}"`);
   return [arg.slice(0, dot).trim(), arg.slice(dot + 1).trim()];
}

function splitPkgPath(arg: string, def: string): { pkg: string; rel: string } {
   const slash = arg.indexOf("/");
   if (slash < 0) return { pkg: def, rel: arg.trim() };
   return { pkg: arg.slice(0, slash).trim(), rel: arg.slice(slash + 1).trim() };
}

function extractCode(body: string[], lang: string): string | undefined {
   const open = body.findIndex((l) => l.trim() === "```" + lang || l.trim().startsWith("```" + lang));
   if (open < 0) return undefined;
   const close = body.findIndex((l, idx) => idx > open && l.trim() === "```");
   if (close < 0) return undefined;
   return body.slice(open + 1, close).join("\n").trim();
}

/**
 * Parse the first CONTIGUOUS run of GFM table rows in `lines`.
 *
 * Contiguity matters: the previous implementation filtered EVERY table-ish line out
 * of a section body and treated them as one table, so a second table in the same
 * body was merged into the first — its header and separator arriving as data rows.
 * A story is allowed to include an illustrative table, so a table block ends at the
 * first line that is not a table row.
 */
function parseTableBlock(lines: string[]): Table | undefined {
   const isRow = (l: string): boolean =>
      l.trim().startsWith("|") && l.trim().endsWith("|");
   const start = lines.findIndex(isRow);
   if (start < 0) return undefined;
   let end = start;
   while (end < lines.length && isRow(lines[end])) end++;
   const rowsRaw = lines.slice(start, end).map((l) => l.trim());
   if (rowsRaw.length < 2) return undefined;
   const cells = (l: string): string[] =>
      l.slice(1, -1).split("|").map((c) => c.trim());
   const cols: Col[] = cells(rowsRaw[0]).map((h) => {
      const c = h.indexOf(":");
      return c < 0
         ? { name: h.toLowerCase(), type: "text" }
         : { name: h.slice(0, c).trim().toLowerCase(), type: h.slice(c + 1).trim().toLowerCase() };
   });
   // rowsRaw[1] is the |---|---| separator.
   const rows = rowsRaw.slice(2).map((l) => cells(l));
   return { cols, rows };
}

/** Body lines with fenced code blocks removed (a ```sql body can contain pipes). */
function outsideFences(body: string[]): string[] {
   const out: string[] = [];
   let inFence = false;
   for (const raw of body) {
      if (/^\s*```/.test(raw)) {
         inFence = !inFence;
         continue;
      }
      if (!inFence) out.push(raw);
   }
   return out;
}

/**
 * An INPUT table (`## Data`, `## Mutate`) — the section's payload, so it needs no
 * label. The first table block wins.
 */
function parseDataTable(body: string[]): Table | undefined {
   return parseTableBlock(outsideFences(body));
}

/**
 * An ASSERTION table: the one a result must match. It must follow an `Expect:` line,
 * which is what lets a section also carry an illustrative table in its prose — only
 * the labelled one is compared. Every assertion table in the suite already carried
 * the label; this makes it load-bearing instead of decorative.
 */
function parseExpectTable(body: string[]): Table | undefined {
   const lines = outsideFences(body);
   const at = lines.findIndex((l) => /^\s*expect\s*:?\s*$/i.test(l));
   if (at < 0) return undefined;
   return parseTableBlock(lines.slice(at + 1));
}

function requireDataTable(body: string[], header: string): Table {
   const t = parseDataTable(body);
   if (!t) throw new Error(`section "## ${header}" requires a GFM table`);
   return t;
}

function requireExpectTable(body: string[], header: string): Table {
   const t = parseExpectTable(body);
   if (!t) {
      throw new Error(
         `section "## ${header}" requires an "Expect:" line followed by a GFM ` +
            `table. A table NOT preceded by "Expect:" is treated as prose and is ` +
            `not compared.`,
      );
   }
   return t;
}

/**
 * Parse an orchestrated-build body: `- <src> -> <physicalName> @ <dest>` lines
 * (the sources this build produces, with caller-assigned/generational names) and
 * `reference: <upstreamSrc> [(from=<pub>)]` lines (upstreams to reuse, resolved by
 * source name at run time). References are collected package-wide (they map to the
 * build's `referenceManifest`), not nested under a source.
 */
function parseOrchestratedBody(body: string[]): {
   sources: { src: string; name: string; dest: string }[];
   references: { src: string; from?: string }[];
} {
   const sources: { src: string; name: string; dest: string }[] = [];
   const references: { src: string; from?: string }[] = [];
   for (const raw of body) {
      const line = raw.trim();
      const s = line.match(/^-\s*(\S+)\s*->\s*(\S+)\s*@\s*(\S+)\s*$/);
      if (s) {
         sources.push({ src: s[1], name: s[2], dest: s[3] });
         continue;
      }
      const r = line.match(/^reference:\s*([^\s(]+)\s*(?:\(from=([^)]+)\))?\s*$/i);
      if (r) references.push({ src: r[1], from: r[2]?.trim() || undefined });
   }
   return { sources, references };
}

function parseBindings(body: string[]): { source: string; conn: string }[] {
   const out: { source: string; conn: string }[] = [];
   for (const raw of body) {
      const m = raw.match(/expect\s+binding\s*:\s*(\S+)\s*->\s*(\S+)/i);
      if (m) out.push({ source: m[1], conn: m[2] });
   }
   return out;
}

/**
 * Parse a `NAME=value; OTHER=value` body value into a map. Semicolon-separated so
 * a value may contain commas (the header attribute syntax splits on those).
 * Returns undefined for an absent/empty value, so callers can omit the field.
 */
function parseKeyValues(
   raw: string | undefined,
): Record<string, string> | undefined {
   if (!raw?.trim()) return undefined;
   const out: Record<string, string> = {};
   for (const part of raw.split(";")) {
      const eq = part.indexOf("=");
      if (eq < 0) continue;
      const name = part.slice(0, eq).trim();
      if (name) out[name] = part.slice(eq + 1).trim();
   }
   return Object.keys(out).length > 0 ? out : undefined;
}

/** Body keys whose value is substituted at run time (see {@link HARNESS_TOKENS}). */
const SUBSTITUTED_KEYS = ["excludes"];

/** The `${…}` tokens a scenario may use, and where each value comes from. */
const HARNESS_TOKENS: Record<string, (ctx: ScenarioContext) => string> = {
   "pg.password": (ctx) => ctx.pg.password,
   "pg.user": (ctx) => ctx.pg.user,
   "pg.host": (ctx) => ctx.pg.host,
};

/**
 * Substitute harness-runtime tokens into an expected/needle string, so a scenario
 * can assert against a value only the harness knows (e.g. the throwaway container's
 * password, for a secret-redaction check).
 *
 * A leftover `${…}` throws rather than being compared literally: an unsubstituted
 * token can never match, so an `excludes:` carrying one would pass unconditionally —
 * a redaction check that always says "no leak" regardless of the truth. Token names
 * are validated at parse time ({@link validateSubstitutions}); this is the backstop.
 */
function substituteHarnessTokens(raw: string, ctx: ScenarioContext): string {
   let out = raw;
   for (const [token, read] of Object.entries(HARNESS_TOKENS)) {
      out = out.split(`\${${token}}`).join(read(ctx));
   }
   if (out.includes("${")) {
      throw new Error(
         `unsubstituted token in "${raw}" — known tokens: ` +
            `${Object.keys(HARNESS_TOKENS).map((k) => `\${${k}}`).join(", ")}`,
      );
   }
   return out;
}

/**
 * Reject a `${…}` token that is unknown, or one used in a body key whose value is
 * never substituted. Both cases produce a needle that cannot match, so the
 * assertion carrying it would pass no matter what the server did.
 */
function validateSubstitutions(header: string, body: string[]): void {
   let inFence = false;
   for (const raw of body) {
      if (/^\s*```/.test(raw)) {
         inFence = !inFence;
         continue;
      }
      if (inFence) continue;
      const m = raw.match(/^ {0,3}([a-z][a-z0-9_]*)\s*:\s*(.+)$/);
      if (!m) continue;
      const [, key, value] = m;
      for (const token of value.match(/\$\{([^}]*)\}/g) ?? []) {
         const name = token.slice(2, -1);
         if (!(name in HARNESS_TOKENS)) {
            throw new Error(
               `## ${header}: unknown substitution "${token}" in "${key}:". Known: ` +
                  `${Object.keys(HARNESS_TOKENS).map((k) => `\${${k}}`).join(", ")}`,
            );
         }
         if (!SUBSTITUTED_KEYS.includes(key)) {
            throw new Error(
               `## ${header}: "${key}:" is not substituted, so "${token}" would be ` +
                  `compared literally and could never match. Substitution is ` +
                  `supported in: ${SUBSTITUTED_KEYS.join(", ")}`,
            );
         }
      }
   }
}

function firstKey(body: string[], key: string): string | undefined {
   for (const raw of body) {
      const m = raw.match(new RegExp(`^\\s*${key}\\s*:\\s*(.+)$`, "i"));
      if (m) return m[1].trim();
   }
   return undefined;
}

// ─────────────────────────── SQL generation ───────────────────────────

const PG_TYPE: Record<string, string> = {
   int: "int",
   integer: "int",
   bigint: "bigint",
   num: "numeric",
   number: "numeric",
   numeric: "numeric",
   decimal: "numeric",
   float: "double precision",
   double: "double precision",
   date: "date",
   timestamp: "timestamp",
   text: "text",
   string: "text",
   varchar: "text",
   bool: "boolean",
   boolean: "boolean",
};

function sqlLiteral(value: string, type: string): string {
   const v = value.trim();
   if (v === "" || v.toLowerCase() === "null") return "NULL";
   switch (type) {
      case "date":
         return `DATE '${v}'`;
      case "timestamp":
         return `TIMESTAMP '${v}'`;
      case "int":
      case "integer":
      case "bigint":
      case "num":
      case "number":
      case "numeric":
      case "decimal":
      case "float":
      case "double":
         return v;
      case "bool":
      case "boolean":
         return v.toLowerCase();
      default:
         return `'${v.replace(/'/g, "''")}'`;
   }
}

function createAndInsert(table: string, data: Table): string {
   const colDefs = data.cols
      .map((c) => `${c.name} ${PG_TYPE[c.type] ?? "text"}`)
      .join(", ");
   const values = data.rows
      .map((r) => `(${r.map((cell, i) => sqlLiteral(cell, data.cols[i].type)).join(", ")})`)
      .join(",\n  ");
   return (
      `DROP TABLE IF EXISTS ${table};\n` +
      `CREATE TABLE ${table} (${colDefs});\n` +
      (data.rows.length ? `INSERT INTO ${table} VALUES\n  ${values};\n` : "")
   );
}

function insertRows(table: string, data: Table): string {
   const values = data.rows
      .map((r) => `(${r.map((cell, i) => sqlLiteral(cell, data.cols[i].type)).join(", ")})`)
      .join(",\n  ");
   return `INSERT INTO ${table} VALUES\n  ${values};\n`;
}

// ─────────────────────────── value comparison ───────────────────────────

function normalizeCell(v: unknown): string | number | null {
   if (v == null || v === "") return null;
   const s = String(v).trim();
   if (/^-?\d+(\.\d+)?$/.test(s)) return Number(s);
   const dm = s.match(/^(\d{4}-\d{2}-\d{2})/);
   if (dm) return dm[1];
   return s;
}

/**
 * Assert the result's column set is EXACTLY the expected table's columns.
 * {@link compareRows} only inspects the columns it was given, so an unexpected
 * extra column (a physical column leaking into a served shape, say) passes it
 * silently; this closes that gap for a scenario that says `columns: exact`.
 */
function assertExactColumns(
   assert: Assert,
   label: string,
   expect: Table,
   actual: Record<string, unknown>[],
): void {
   if (actual.length === 0) return;
   const got = Object.keys(actual[0]).sort();
   const want = expect.cols.map((c) => c.name).sort();
   assert.eq(`${label}: exact column set`, got, want);
}

/** Compare actual query rows against an expected GFM table (ordered, on the expected columns). */
function compareRows(
   assert: Assert,
   label: string,
   expect: Table,
   actual: Record<string, unknown>[],
): void {
   const cols = expect.cols.map((c) => c.name);
   if (expect.rows.length !== actual.length) {
      assert.fail(
         `${label}: row count`,
         `expected ${expect.rows.length} rows, got ${actual.length}: ${JSON.stringify(actual).slice(0, 200)}`,
      );
      return;
   }
   for (let r = 0; r < expect.rows.length; r++) {
      for (let c = 0; c < cols.length; c++) {
         const exp = normalizeCell(expect.rows[r][c]);
         const act = normalizeCell(actual[r]?.[cols[c]]);
         if (JSON.stringify(exp) !== JSON.stringify(act)) {
            assert.fail(
               `${label}: row ${r + 1} col ${cols[c]}`,
               `expected ${JSON.stringify(exp)}, got ${JSON.stringify(act)}`,
            );
            return;
         }
      }
   }
   assert.ok(`${label}: ${expect.rows.length} row(s) match`, true);
}

// ─────────────────────────── build helpers ───────────────────────────

/** A caller-instructed build body derived from an orchestrated `## Build`. */
interface OrchestratedBody {
   buildInstructions: {
      sources: {
         sourceEntityId: string;
         materializedTableId: string;
         physicalTableName: string;
         realization: string;
         destination: string;
      }[];
      referenceManifest?: { sourceEntityId: string; physicalTableName: string }[];
      strictUpstreams: boolean;
   };
}

/**
 * Turn an orchestrated `## Build` step into a build body, resolving names to ids
 * at run time: each built source's `sourceEntityId` from the target's build plan,
 * and each `reference:`d upstream's id + physical table from a publisher's latest
 * manifest (the target, or `(from=<pub>)`). Authors name sources, never ids.
 */
async function buildOrchestratedBody(
   ctx: ScenarioContext,
   rest: Rest,
   step: {
      pkg: string;
      strict: boolean;
      sources: { src: string; name: string; dest: string }[];
      references: { src: string; from?: string }[];
   },
): Promise<OrchestratedBody> {
   const eids = await rest.sourceEntityIds(step.pkg);
   const sources = step.sources.map((s) => {
      const eid = eids[s.src];
      if (!eid) {
         throw new Error(
            `## Build (orchestrated): source '${s.src}' not in ${step.pkg} build plan (have: ${Object.keys(eids).join(", ")})`,
         );
      }
      return {
         sourceEntityId: eid,
         materializedTableId: `mt-${s.name}`,
         physicalTableName: s.name,
         realization: "COPY",
         destination: s.dest,
      };
   });
   const referenceManifest: { sourceEntityId: string; physicalTableName: string }[] = [];
   for (const ref of step.references) {
      const refRest = ref.from ? ctx.restOf(ref.from) : rest;
      const refEid = (await refRest.sourceEntityIds(step.pkg))[ref.src];
      if (!refEid) {
         throw new Error(
            `## Build (orchestrated): reference '${ref.src}' not in ${step.pkg} build plan`,
         );
      }
      const entry = (await refRest.latestManifestEntries(step.pkg))[refEid] as
         | { physicalTableName?: string }
         | undefined;
      referenceManifest.push({
         sourceEntityId: refEid,
         physicalTableName: entry?.physicalTableName ?? "",
      });
   }
   return {
      buildInstructions: {
         sources,
         ...(referenceManifest.length ? { referenceManifest } : {}),
         strictUpstreams: step.strict,
      },
   };
}

/**
 * Run a build expected to be refused, tolerating BOTH failure modes: a build that
 * reaches FAILED, and a `createMaterialization` that itself throws (the package
 * won't load / 4xx). Returns whether it refused and a detail string for `cites`.
 */
async function refusedOutcome(
   rest: Rest,
   pkg: string,
   body?: Record<string, unknown>,
): Promise<{ refused: boolean; detail: string }> {
   try {
      const { id } = await rest.createMaterialization(pkg, body ?? {});
      const rec = await rest.pollMaterialization(pkg, id);
      return { refused: rec.status === "FAILED", detail: JSON.stringify(rec) };
   } catch (e) {
      return { refused: true, detail: (e as Error).message };
   }
}

/**
 * Assert a package is NOT served: `getPackage` must fail (the package never
 * entered the serving set). This is the divergence backstop behind an invalid
 * model — even when `/compile` reports errors, the publisher must not ALSO report
 * the package as served ("compile errors, yet it thinks it's serving fine"). Uses
 * `getPackage` (a durable per-package probe) rather than `/status` loadErrors
 * (which is pruned after the first call, so it's unreliable mid-session).
 */
async function assertNotServed(
   rest: Rest,
   pkg: string,
   assert: Assert,
   label: string,
): Promise<void> {
   let served = false;
   try {
      await rest.getPackage(pkg);
      served = true;
   } catch {
      // getPackage failed ⇒ not served — the expected outcome for an invalid model.
   }
   assert.ok(
      `${label}: not served`,
      !served,
      served
         ? `'${pkg}' does not compile, yet the publisher reports it as served — the compile and load paths diverged`
         : undefined,
   );
}

// ─────────────────────────── the Scenario ───────────────────────────

type Hooks = Record<string, (api: HookApi, assert: Assert) => Promise<void>>;

/** What a hooks.ts export receives — the full runtime, so it can do exotic things. */
export interface HookApi extends ScenarioContext {
   modelPath(pkg?: string, env?: string): string;
   /**
    * Mutable state shared across all `## Hook` steps in one scenario, so a tiny
    * hook can stash a value (e.g. a captured sourceEntityId) that a later hook
    * reads/asserts — keeping the flow in markdown and hooks small + interleaved.
    */
   state: Record<string, unknown>;
}

/**
 * Test-only door onto {@link parseMarkdown}, so the grammar's strict parse can be
 * exercised without a scenario directory on disk. See scenario_md.spec.ts.
 */
export function parseMarkdownForTest(text: string, fallbackId: string): ParsedMd {
   return parseMarkdown(text, fallbackId);
}

export async function parseScenarioFile(dir: string): Promise<Scenario> {
   const fallbackId = path.basename(dir);
   const md = await Bun.file(path.join(dir, "scenario.md")).text();
   const parsed = parseMarkdown(md, fallbackId);

   let hooks: Hooks = {};
   const hooksPath = path.join(dir, "hooks.ts");
   if (await Bun.file(hooksPath).exists()) {
      hooks = (await import(hooksPath)) as Hooks;
      // An exported hook no `## Hook` step names is dead code — most often a
      // renamed step or a deleted one, leaving its assertions in the file and never
      // running them. That reads like coverage and is not, so it fails at load.
      // (The reverse — a `## Hook` naming a missing export — already throws.)
      const referenced = new Set(
         parsed.steps.filter((s) => s.kind === "hook").map((s) => s.name),
      );
      const orphans = Object.entries(hooks)
         .filter(([name, fn]) => typeof fn === "function" && !referenced.has(name))
         .map(([name]) => name);
      if (orphans.length) {
         throw new Error(
            `hooks.ts exports ${orphans.map((o) => `"${o}"`).join(", ")} that no ` +
               `"## Hook" step references — reference them or delete them`,
         );
      }
   }

   // Pre-pass: packages (first Model per env+pkg) + source seeds for up-front
   // setup. A package name can appear in more than one environment, each with its
   // own model, so packages are grouped by (env, pkg).
   const pkgKey = (env: string, pkg: string): string => `${env}:${pkg}`;
   const pkgModels = new Map<
      string,
      { env: string; name: string; models: Map<string, string> }
   >();
   const primaryModel = new Map<string, string>(); // (env,pkg) -> first model path
   for (const step of parsed.steps) {
      if (step.kind === "model") {
         const k = pkgKey(step.env, step.pkg);
         if (!pkgModels.has(k)) {
            pkgModels.set(k, { env: step.env, name: step.pkg, models: new Map() });
         }
         const entry = pkgModels.get(k)!;
         if (!entry.models.has(step.path)) entry.models.set(step.path, step.malloy);
         if (!primaryModel.has(k)) primaryModel.set(k, step.path);
      }
   }
   const packages: PackageSpec[] = [...pkgModels.values()].map((e) => ({
      name: e.name,
      env: e.env,
      models: [...e.models.entries()].map(([p, text]) => ({ path: p, text })),
   }));
   const sourceTables: SourceTable[] = parsed.dataSeeds.map((d) => ({
      sql: createAndInsert(d.table, d.data),
   }));

   const run = async (ctx: ScenarioContext, assert: Assert): Promise<void> => {
      // `(again)` re-runs the most recent query WITH THE SAME LABEL — so
      // intervening queries of other labels don't hijack the reuse.
      const malloyByLabel = new Map<string, string>();
      // Async publishes fire without awaiting; `## Await <label>` (or teardown)
      // drains them. Keyed by label (or an auto key when unlabeled).
      const pendingBuilds = new Map<
         string,
         { rest: Rest; pkg: string; id: string }
      >();
      // Shared mutable state for hooks — lets a tiny `## Hook` stash a value (e.g.
      // a captured sourceEntityId) that a later `## Hook` reads/asserts, so the
      // markdown carries the flow and hooks stay small and interleaved.
      const hookState: Record<string, unknown> = {};
      // The ACTIVE publisher — set by each `## Publisher` step and used by every
      // subsequent step. Steps run against whichever publisher is active, so a
      // multi-publisher scenario just switches focus with `## Publisher <name>`.
      let activeName = "default";
      let activeMode: PersistStorageMode = "on";
      let activeRest: Rest | null = null;
      const active = async (): Promise<Rest> => {
         if (!activeRest) activeRest = await ctx.usePublisher(activeName, activeMode);
         return activeRest;
      };
      // The server a step runs against: an explicit `(pub=<name>)` target (it must
      // already be started via `## Publisher <name>`), else the active publisher —
      // bound to the step's environment. One server process serves every
      // configured environment, so an env-targeted step just rebinds the REST
      // client to that env against the same base URL.
      const serverFor = async (pub?: string, env?: string): Promise<Rest> => {
         const base = pub ? ctx.restOf(pub) : await active();
         const target = env ?? PRIMARY_ENV;
         return target === base.env ? base : new Rest(base.baseUrl, target);
      };
      const modelPath = (pkg?: string, env?: string): string =>
         primaryModel.get(pkgKey(env ?? PRIMARY_ENV, pkg ?? parsed.defaultPackage)) ??
         `${parsed.defaultPackage}.malloy`;

      for (const step of parsed.steps) {
         const checksBefore = assert.checks.length;
         switch (step.kind) {
            case "model":
               await ctx.editPackageModel(step.pkg, step.path, step.malloy, step.env);
               break;
            case "publish": {
               const rest = await serverFor(step.pub, step.env);
               const buildBody = {
                  ...(step.forceRefresh ? { forceRefresh: true } : {}),
                  ...(step.sourceNames ? { sourceNames: step.sourceNames } : {}),
               };
               if (step.async) {
                  // Fire and DON'T await — a following step observes it in flight.
                  const { id } = await rest.createMaterialization(step.pkg, buildBody);
                  const key = step.label ?? `${step.pkg}#${pendingBuilds.size}`;
                  pendingBuilds.set(key, { rest, pkg: step.pkg, id });
                  break;
               }
               await rest.build(step.pkg, buildBody);
               if (step.bindings.length) {
                  // The build returns at MANIFEST_FILE_READY, but the serve
                  // binding is re-established by an async package reload that
                  // reads the latest successful materialization — it is eventually
                  // consistent, not synchronous with the build response. Poll so
                  // the assertion is deterministic rather than racing the rebind.
                  type Binding = { sourceName: string; storageConnectionName: string };
                  const has = (bindings: Binding[], b: { source: string; conn: string }): boolean =>
                     bindings.some((x) => x.sourceName === b.source && x.storageConnectionName === b.conn);
                  let bindings: Binding[] = [];
                  for (let attempt = 0; attempt < 40; attempt++) {
                     const pkg = (await rest.getPackage(step.pkg)) as {
                        storageServeBindings?: Binding[];
                     };
                     bindings = pkg.storageServeBindings ?? [];
                     if (step.bindings.every((b) => has(bindings, b))) break;
                     await sleep(250);
                  }
                  for (const b of step.bindings) {
                     assert.ok(
                        `binding ${b.source} -> ${b.conn}`,
                        has(bindings, b),
                        JSON.stringify(bindings),
                     );
                  }
               }
               break;
            }
            case "await": {
               // Drain a specific async build (or, if unlabeled, the oldest
               // pending one) and assert it completed successfully.
               const key = step.label ?? [...pendingBuilds.keys()][0];
               const pending = key ? pendingBuilds.get(key) : undefined;
               if (!pending) {
                  assert.fail(`await ${step.label ?? ""}`, `no pending build to await`);
                  break;
               }
               pendingBuilds.delete(key!);
               const rec = await pending.rest.pollMaterialization(pending.pkg, pending.id);
               assert.eq(`await ${key}: completes`, rec.status, "MANIFEST_FILE_READY");
               break;
            }
            case "buildRefused": {
               const rest = await serverFor(step.pub, step.env);
               const outcome = await refusedOutcome(rest, step.pkg);
               assert.ok(
                  `build refused (${step.pkg})`,
                  outcome.refused,
                  outcome.detail.slice(0, 200),
               );
               if (step.cites && outcome.refused)
                  assert.includes(`refusal cites "${step.cites}"`, outcome.detail.toLowerCase(), step.cites.toLowerCase());
               if (step.excludes) {
                  const needle = substituteHarnessTokens(step.excludes, ctx);
                  assert.excludes(`refusal must not leak "${step.excludes}"`, outcome.detail, needle);
               }
               break;
            }
            case "orchestratedBuild": {
               const rest = await serverFor(step.pub, step.env);
               const body = await buildOrchestratedBody(ctx, rest, step);
               const wire = body as unknown as Record<string, unknown>;
               if (step.refused) {
                  const outcome = await refusedOutcome(rest, step.pkg, wire);
                  assert.ok(
                     `orchestrated build refused (${step.pkg})`,
                     outcome.refused,
                     outcome.detail.slice(0, 200),
                  );
                  if (step.cites && outcome.refused)
                     assert.includes(`refusal cites "${step.cites}"`, outcome.detail.toLowerCase(), step.cites.toLowerCase());
                  if (step.excludes) {
                     const needle = substituteHarnessTokens(step.excludes, ctx);
                     assert.excludes(`refusal must not leak "${step.excludes}"`, outcome.detail, needle);
                  }
               } else {
                  const rec = await rest.build(step.pkg, wire);
                  const entries = (rec.manifest as { entries?: Record<string, { physicalTableName?: string }> } | null)?.entries ?? {};
                  // Verify each built source landed in the caller-assigned name.
                  for (const s of body.buildInstructions.sources) {
                     assert.eq(
                        `built ${s.physicalTableName}`,
                        entries[s.sourceEntityId]?.physicalTableName,
                        s.physicalTableName,
                     );
                  }
               }
               break;
            }
            case "query": {
               const rest = await serverFor(step.pub, step.env);
               const malloy = step.malloy ?? (step.again ? malloyByLabel.get(step.label) : undefined);
               if (!malloy)
                  throw new Error(
                     `## Query ${step.label}: no malloy block and no prior query labeled "${step.label}" to reuse`,
                  );
               malloyByLabel.set(step.label, malloy);
               if (step.refused) {
                  const res = await rest.tryQuery(step.pkg, modelPath(step.pkg, step.env), { query: malloy, givens: step.givens });
                  assert.ok(
                     `${step.label}: refused`,
                     !res.ok,
                     res.ok ? `expected failure, but query succeeded: ${JSON.stringify(res.outcome.rows).slice(0, 150)}` : undefined,
                  );
                  if (step.cites && !res.ok)
                     assert.includes(`${step.label}: cites`, res.error.toLowerCase(), step.cites.toLowerCase());
               } else {
                  const out = await rest.query(step.pkg, modelPath(step.pkg, step.env), { query: malloy, givens: step.givens });
                  compareRows(assert, step.label, step.expect!, out.rows);
                  if (step.exactColumns) assertExactColumns(assert, step.label, step.expect!, out.rows);
               }
               break;
            }
            case "buildTargets": {
               const rest = await serverFor(step.pub, step.env);
               const pkg = (await rest.getPackage(step.pkg)) as {
                  buildPlan?: {
                     sources?: Record<
                        string,
                        { name?: string; sourceEntityId?: string; annotationFields?: { name?: string } }
                     >;
                  };
               };
               const sources = Object.values(pkg.buildPlan?.sources ?? {});
               // One row per DISTINCT source name: `source | writes` (the resolved
               // `#@ persist name=`, else the source name). Sorted both sides so the
               // comparison is order-independent — build iteration order is not a
               // contract.
               const actual = [
                  ...new Map(
                     sources
                        .filter((s) => s.name)
                        .map((s) => [s.name!, { source: s.name!, writes: s.annotationFields?.name ?? s.name! }]),
                  ).values(),
               ].sort((a, b) => a.source.localeCompare(b.source));
               // `entity` is a grouping DIRECTIVE, not a value to compare (the real
               // ids are hashes) — drop it before the row comparison and handle it
               // separately below.
               const entityCol = step.expect.cols.findIndex((c) => c.name === "entity");
               const keep = step.expect.cols.map((_, idx) => idx).filter((idx) => idx !== entityCol);
               const expectSorted: Table = {
                  cols: keep.map((idx) => step.expect.cols[idx]),
                  rows: [...step.expect.rows]
                     .sort((a, b) => String(a[0]).localeCompare(String(b[0])))
                     .map((row) => keep.map((idx) => row[idx])),
               };
               compareRows(assert, `build targets (${step.pkg})`, expectSorted, actual as unknown as Record<string, unknown>[]);
               // An `entity` column groups sources by content address: rows sharing
               // a label must share one sourceEntityId, and different labels must
               // differ. The label itself is arbitrary — the ids are hashes.
               if (entityCol >= 0) {
                  const idByName = new Map(sources.filter((s) => s.name).map((s) => [s.name!, s.sourceEntityId]));
                  const groups = new Map<string, string[]>();
                  for (const row of step.expect.rows) {
                     const label = String(row[entityCol]).trim();
                     if (!groups.has(label)) groups.set(label, []);
                     groups.get(label)!.push(String(row[0]).trim());
                  }
                  const repr = new Map<string, string | undefined>();
                  for (const [label, names] of groups) {
                     const ids = names.map((n) => idByName.get(n));
                     assert.ok(
                        `entity ${label}: ${names.join(" + ")} share one content address`,
                        ids.every((id) => id && id === ids[0]),
                        `ids: ${JSON.stringify(ids)}`,
                     );
                     repr.set(label, ids[0]);
                  }
                  const labels = [...repr.keys()];
                  for (let a = 0; a < labels.length; a++)
                     for (let b = a + 1; b < labels.length; b++)
                        assert.ne(
                           `entity ${labels[a]} differs from ${labels[b]}`,
                           repr.get(labels[a]),
                           repr.get(labels[b]),
                        );
               }
               break;
            }
            case "mutate": {
               const table = step.table;
               if (step.sql) await ctx.pg.sql(ctx.sourceDb, step.sql);
               else if (step.rows) await ctx.pg.sql(ctx.sourceDb, insertRows(table, step.rows));
               break;
            }
            case "sql": {
               const rows = await ctx.pg.query(ctx.sourceDb, step.sql);
               compareRows(assert, step.label, step.expect, rows);
               break;
            }
            case "operator": {
               // Ensure a server is up in the requested mode (some flows expect
               // one running), then run the operator's read-write DDL out-of-band
               // (via the operator's OWN DuckLake client, not the publisher).
               await active();
               await ctx.operatorSql(step.conn, step.sql);
               break;
            }
            case "connection": {
               // Runs THROUGH the publisher's connection sqlQuery endpoint (what a
               // caller can reach). For a storage destination this attach is
               // read-only, so `refused` asserts DDL is rejected.
               const rest = await serverFor(step.pub, step.env);
               if (step.refused) {
                  let threw = false;
                  let err = "";
                  try {
                     await rest.connectionSql(step.conn, step.sql);
                  } catch (e) {
                     threw = true;
                     err = (e as Error).message;
                  }
                  assert.ok(
                     `connection ${step.conn}: refused`,
                     threw,
                     threw ? undefined : "expected the connection SQL to be refused, but it succeeded",
                  );
                  if (step.cites && threw)
                     assert.includes(`connection ${step.conn}: cites`, err.toLowerCase(), step.cites.toLowerCase());
               } else {
                  const raw = (await rest.connectionSql(step.conn, step.sql)) as { data?: string };
                  const rows = (JSON.parse(raw.data ?? '{"rows":[]}') as { rows?: Record<string, unknown>[] }).rows ?? [];
                  if (step.expect) compareRows(assert, `connection ${step.conn}`, step.expect, rows);
                  if (step.expectRows !== undefined)
                     assert.eq(`connection ${step.conn}: row count`, rows.length, step.expectRows);
               }
               break;
            }
            case "rejected": {
               // The package's model is invalid: assert it is NOT served (durable
               // probe), and — if a `cites:` is given — confirm the diagnostic via
               // /compile (the durable source of the error text; /status loadErrors
               // is pruned after the first call, so it isn't used here).
               const rest = await serverFor(step.pub, step.env);
               await assertNotServed(rest, step.pkg, assert, `package ${step.pkg}`);
               if (step.cites) {
                  const res = await rest.compile(step.pkg, modelPath(step.pkg, step.env), "");
                  const problems = JSON.stringify(res.problems ?? []).toLowerCase();
                  assert.includes(
                     `${step.pkg} compile error cites`,
                     problems,
                     step.cites.toLowerCase(),
                  );
               }
               break;
            }
            case "warns": {
               // Assert the package surfaces an operator warning citing a
               // substring (getPackageMetadata().warnings). Forces the package to
               // load via getPackage, so the warning is present deterministically.
               const rest = await serverFor(step.pub, step.env);
               const pkg = (await rest.getPackage(step.pkg)) as {
                  warnings?: { model?: string; target?: string; message?: string }[];
               };
               const blob = JSON.stringify(pkg.warnings ?? []).toLowerCase();
               assert.includes(
                  `package ${step.pkg} warns: "${step.cites}"`,
                  blob,
                  step.cites.toLowerCase(),
               );
               break;
            }
            case "compile": {
               const rest = await serverFor(step.pub, step.env);
               const res = await rest.compile(step.pkg, modelPath(step.pkg, step.env), step.source);
               const problems = JSON.stringify(res.problems ?? []).toLowerCase();
               if (step.refused) {
                  // Framework does the dance: an invalid model must ALSO not be
                  // served (backstop against the compile/load paths diverging) …
                  await assertNotServed(rest, step.pkg, assert, step.label);
                  // … and /compile reports the diagnostic text.
                  assert.ok(
                     `${step.label}: compile refused`,
                     res.status === "error",
                     res.status === "error" ? undefined : `expected a compile error, got status=${res.status} problems=${problems.slice(0, 200)}`,
                  );
                  if (step.cites && res.status === "error")
                     assert.includes(`${step.label}: cites`, problems, step.cites.toLowerCase());
               } else {
                  assert.ok(
                     `${step.label}: compiles`,
                     res.status === "success",
                     res.status === "success" ? undefined : `expected success, got problems=${problems.slice(0, 200)}`,
                  );
               }
               break;
            }
            case "bind": {
               const rest = await serverFor(step.pub, step.env);
               if (step.variant === "clear") {
                  // manifestLocation:null → drop the orchestrator binding; the publisher
                  // reverts to its own local-store rebind (from the latest build).
                  await rest.patchPackage(step.pkg, { manifestLocation: null });
               } else if (step.variant === "bad") {
                  // Bind an unreachable manifestLocation (a file:// URI that does
                  // not exist). The publisher must fetch-fail and fall back to
                  // serving live rather than erroring the package — the degraded
                  // path in bindManifest ("serving live" on fetch failure).
                  await rest.patchPackage(step.pkg, {
                     manifestLocation: `file:///hammer/nonexistent/${step.pkg}-missing.json`,
                  });
               } else {
                  // `from=<publisher>` sources the manifest from ANOTHER publisher's
                  // build — the cluster pattern: one worker builds the table, the
                  // orchestrator distributes that manifest to the others. Default is
                  // self (the active publisher's own latest build).
                  const source = step.from ? ctx.restOf(step.from) : rest;
                  const entries =
                     step.variant === "empty"
                        ? {}
                        : await source.latestManifestEntries(step.pkg);
                  // Stamp freshness fields onto each entry when requested, so the
                  // scenario can drive the age-vs-window gate (dataAsOf is the age
                  // anchor; the window + fallback decide stale handling).
                  if (step.asof || step.fresh !== undefined || step.fallback) {
                     for (const e of Object.values(entries)) {
                        const entry = e as Record<string, unknown>;
                        if (step.asof) entry.dataAsOf = step.asof;
                        if (step.fresh !== undefined) entry.freshnessWindowSeconds = step.fresh;
                        if (step.fallback) entry.freshnessFallback = step.fallback;
                     }
                  }
                  const uri = await ctx.writeManifest(
                     `${step.pkg}-${activeName}-${step.variant}`,
                     entries,
                  );
                  await rest.patchPackage(step.pkg, { manifestLocation: uri });
               }
               break;
            }
            case "reclaim": {
               // Reclaim the latest successful materialization: DELETE it with
               // dropTables (the destination-aware read-write drop of the physical
               // table). A following `## Restart` re-establishes serving from the
               // store — now empty for this source — so it reverts to live.
               const rest = await serverFor(step.pub, step.env);
               const id = await rest.reclaimLatest(step.pkg);
               assert.ok(`reclaim ${step.pkg}`, !!id, `reclaimed ${id}`);
               break;
            }
            case "delete": {
               // Unload + delete the package from the serving set. A following
               // `## Query (refused)` proves serving stopped; here we assert the
               // package no longer resolves.
               const rest = await serverFor(step.pub, step.env);
               await rest.deletePackage(step.pkg);
               let gone = false;
               try {
                  await rest.getPackage(step.pkg);
               } catch {
                  gone = true;
               }
               assert.ok(
                  `package ${step.pkg} unloaded after delete`,
                  gone,
                  "getPackage still resolves after DELETE",
               );
               break;
            }
            case "publisher": {
               // Make <name> the active publisher, (re)starting it at this mode.
               // usePublisher boots it (or restarts on a mode change), so the
               // narrative shows each mode as a distinct publisher process — which
               // is what changing PERSIST_STORAGE_MODE actually requires — and a
               // named publisher is addressable for a multi-publisher scenario.
               activeName = step.name ?? "default";
               activeMode = step.mode;
               activeRest = await ctx.usePublisher(activeName, activeMode, {
                  extraEnv: step.extraEnv,
               });
               break;
            }
            case "republish": {
               // POST /packages to re-publish through the author-in-the-loop gate
               // (addPackage), which — unlike startup/reload — is strict: under
               // PERSIST_COLLISION_ENFORCE a colliding package is rejected here.
               const rest = await serverFor(step.pub, step.env);
               const res = await rest.addPackage(step.pkg);
               if (step.refused) {
                  assert.ok(
                     `republish ${step.pkg}: refused`,
                     !res.ok,
                     res.ok ? "expected the publish to be rejected, but it succeeded" : undefined,
                  );
                  if (step.cites && !res.ok)
                     assert.includes(`republish ${step.pkg}: cites`, res.error.toLowerCase(), step.cites.toLowerCase());
               } else {
                  assert.ok(
                     `republish ${step.pkg}: accepted`,
                     res.ok,
                     res.ok ? undefined : `expected the publish to succeed, got: ${res.error.slice(0, 200)}`,
                  );
               }
               break;
            }
            case "restart": {
               // Reboot the active publisher. Bare `## Restart` preserves the
               // materialization store (no --init), so serving is re-established
               // from the persisted store on load; `## Restart (init)` re-copies
               // packages (picking up a mid-run `## Model` edit) and resets it.
               activeRest = await ctx.reboot({
                  name: activeName,
                  mode: activeMode,
                  init: step.init,
               });
               break;
            }
            case "hook": {
               const fn = hooks[step.name];
               if (!fn) throw new Error(`## Hook ${step.name}: no export named "${step.name}" in hooks.ts`);
               // Hooks run in document order (interleaved with markdown steps) and
               // share `state`, so a small hook can stash a value another reads.
               await fn({ ...ctx, modelPath, state: hookState }, assert);
               break;
            }
         }
         // A step that verified nothing is a false green in the making: it looks
         // like coverage in the report and is not. See SIDE_EFFECT_ONLY_STEPS.
         if (stepMustAssert(step.kind) && assert.checks.length === checksBefore) {
            assert.fail(
               `step "${step.kind}" asserted nothing`,
               `this step ran but contributed no check — it needs an Expect: table, ` +
                  `a cites:/excludes: key, or (rows=N), or it is not verifying anything`,
            );
         }
      }

      // Drain any async publishes the scenario didn't explicitly `## Await`, so a
      // background build doesn't outlive the scenario (best-effort; not asserted).
      for (const { rest, pkg, id } of pendingBuilds.values()) {
         await rest.pollMaterialization(pkg, id).catch(() => undefined);
      }
   };

   return {
      id: parsed.id,
      tags: parsed.tags,
      requires: parsed.requires,
      note: parsed.note,
      title: parsed.title,
      packages,
      sourceTables,
      connections: parsed.connectionDecls,
      run,
   };
}
