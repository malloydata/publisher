// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * @fileoverview End-to-end test for ``extractEntitiesFromExplores`` that
 * exercises the cross-file join code path: an entry model that imports a
 * source whose own joins are defined in another file.
 *
 * Pins the contract that join code is read from the file that DEFINES the
 * join (via the file-aware ``extractCodeFromEntity``), not the entry
 * file's docString — otherwise imported joins are mis-parsed as
 * direct-table joins and emit ghost ``<parent>.<join>::<dim>`` entities.
 *
 * The model files are written to a temp dir that lives through the entire
 * test, because ``extractCodeFromEntity`` needs to read those files off
 * disk after compilation completes.
 */
import { describe, test, beforeEach, afterEach } from "bun:test";
import assert from "node:assert";
import { mkdtemp, readFile, rm, writeFile } from "fs/promises";
import { tmpdir } from "os";
import path from "path";
import { Malloy } from "@malloydata/malloy";
import { FileURLReader } from "./field_helpers";
import { ConnectionLookup } from "./test_support";
import { sourceExtraction } from "./source_extraction";

async function compileModel(
   workspaceRoot: string,
   relativeEntryPath: string,
): Promise<{
   model: import("@malloydata/malloy").Model;
   docString: string;
}> {
   const absolutePath = path.join(workspaceRoot, relativeEntryPath);
   const docString = await readFile(absolutePath, "utf-8");
   const importBaseURL = new URL("file://" + path.dirname(absolutePath) + "/");
   const modelUrl = new URL("file://" + absolutePath);
   const urlReader = new FileURLReader();
   const malloyLookup = await ConnectionLookup.getInstance([], workspaceRoot);
   const model = await Malloy.compile({
      urlReader,
      connections: malloyLookup,
      parse: await Malloy.parse({
         url: modelUrl,
         urlReader,
         options: { importBaseURL },
      }),
   });
   return { model, docString };
}

describe("extractEntitiesFromExplores (integration)", () => {
   let workspace: string;

   beforeEach(async () => {
      workspace = await mkdtemp(path.join(tmpdir(), "ext-entities-integ-"));
      await writeFile(
         path.join(workspace, "parent.csv"),
         "id,region\n1,us\n2,eu\n",
         "utf-8",
      );
      await writeFile(
         path.join(workspace, "child.csv"),
         "parent_id,kind,amount\n1,a,10\n1,b,20\n2,a,5\n",
         "utf-8",
      );
   });

   afterEach(async () => {
      await rm(workspace, { recursive: true, force: true });
   });

   test("imported source's join field reads its joinCode from the OWN file, not the entry docString", async () => {
      // ``inner.malloy``: defines a ``parent`` source whose own
      // ``join_one`` to ``child`` is a plain on-clause join (NOT
      // direct-table, NOT extend) — so the correct
      // ``is_nested_index`` is ``false`` and no recursion should ever
      // happen for it. The join's definition is positioned so that
      // ``child_join``'s ``location.range`` will be
      // ``{ start: { line: 7, character: 13 } }``  (0-indexed) — the
      // alias name ``child_join`` starts at column 13 of line 8
      // (1-indexed), after the ``  join_one: `` prefix.
      const innerModel = [
         `source: child is duckdb.table('child.csv') extend {`, // 1
         `  measure: child_count is count()`, // 2
         `}`, // 3
         ``, // 4
         `source: parent is duckdb.table('parent.csv') extend {`, // 5
         `  primary_key: id`, // 6
         `  // Plain on-clause join — must NOT be nested-index.`, // 7
         `  join_one: child_join is child on id = child_join.parent_id`, // 8
         `}`, // 9
         ``, // 10
      ].join("\n");

      // ``entry.malloy``: imports ``parent`` and extends it. We
      // carefully arrange entry.malloy so line 8 (the same 1-indexed
      // line as ``child_join``'s definition in inner.malloy) contains
      // a decoy that, when sliced from column 13 onward, matches the
      // OLD un-anchored regex AND tickles the ``dotPattern`` check.
      // Inside a ``//`` comment so Malloy parses entry.malloy cleanly
      // — the regex doesn't care about comments, it just slices raw
      // characters.
      //
      // Line layout: ``// pad pad pad`` is 14 chars long. We want
      // ``slice(13, ...)`` to start with a word character followed by
      // ``<word> is <word>.<rest>``. Pad to exactly 13 chars then put
      // ``decoy is bait.x``: that slice yields ``decoy is bait.x``
      // which (a) matches ``\b(\w+)\s+is\s+(\w+)`` → group1=``decoy``,
      // group2=``bait``, and (b) triggers ``\bbait\s*\.`` →
      // ``is_nested_index = true`` → spurious recursion → ghost
      // ``parent.child_join::<dim>`` entities.
      //
      // The join field's ``location.url`` points back to inner.malloy, so
      // its code is sliced from THAT file, not the entry's text → correct
      // classification.
      const decoyLine = "// pad pad padecoy is bait.x"; // 13-char prefix + decoy
      assert.strictEqual(
         decoyLine.length >= 28,
         true,
         "decoy line must be long enough to slice past column 13",
      );
      assert.strictEqual(
         decoyLine.slice(13),
         "decoy is bait.x",
         "decoy must produce the exact garbage shape that triggers dotPattern",
      );
      const entryModel = [
         `// padding line 1`, // 1
         `// padding line 2`, // 2
         `// padding line 3`, // 3
         `// padding line 4`, // 4
         `// padding line 5`, // 5
         `// padding line 6`, // 6
         `// padding line 7`, // 7
         decoyLine, // 8  ← aligns w/ child_join's line in inner.malloy
         ``, // 9
         `import "./inner.malloy"`, // 10
         ``,
         `source: extended is parent extend {`,
         `  measure: extended_count is count()`,
         `}`,
         ``,
      ].join("\n");

      await writeFile(
         path.join(workspace, "inner.malloy"),
         innerModel,
         "utf-8",
      );
      await writeFile(
         path.join(workspace, "entry.malloy"),
         entryModel,
         "utf-8",
      );

      const { model, docString } = await compileModel(
         workspace,
         "entry.malloy",
      );

      const entities = sourceExtraction.extractEntitiesFromExplores(
         model.explores,
         "entry.malloy",
         workspace,
         docString,
      );

      // Regression assertion #1: no entities namespaced under
      // ``parent.child_join`` (the spurious nested-recursion path).
      const ghosts = entities.filter((e) => e.source === "parent.child_join");
      assert.deepStrictEqual(
         ghosts,
         [],
         `No entities should have source="parent.child_join". ` +
            `Found ${ghosts.length}: ${JSON.stringify(
               ghosts.map((g) => ({ name: g.name, source: g.source })),
            )}`,
      );

      // Regression assertion #2: the legitimate ``child_join`` field
      // is emitted as a single join entity on the ``parent`` source
      // and its ``join_source_metadata`` correctly reflects ``child``
      // as the joined source (not the alias, not null).
      const childJoinEntity = entities.find(
         (e) => e.source === "parent" && e.name === "child_join",
      );
      assert.ok(
         childJoinEntity,
         `Expected join entity parent.child_join. ` +
            `Got entities: ${JSON.stringify(
               entities.map((e) => ({ source: e.source, name: e.name })),
            )}`,
      );
      assert.strictEqual(
         childJoinEntity.join_source_metadata?.original_source_name,
         "child",
         "join's original_source_name must be the joined source, not the alias",
      );
      assert.strictEqual(
         childJoinEntity.join_source_metadata?.is_nested_index,
         false,
         "plain on-clause join must NOT be flagged as nested-index",
      );
   }, 60_000);

   test("direct-table and extend joins are still flagged as nested-index", async () => {
      // Regression guard: this is the failure mode the broken
      // anchored-regex fix introduced. ``a is duckdb.table(...)`` and
      // ``a is b extend {...}`` MUST still be recursively walked.
      const entryModel = [
         `source: parent_t is duckdb.table('parent.csv') extend {`,
         `  measure: parent_count is count()`,
         `}`,
         ``,
         `source: events is duckdb.table('parent.csv') extend {`,
         `  primary_key: id`,
         ``,
         `  // Direct-table join — MUST be nested-index.`,
         `  join_one: a_direct is duckdb.table('child.csv')`,
         `    on id = a_direct.parent_id`,
         ``,
         `  // Inline extend — MUST be nested-index.`,
         `  join_one: a_extend is parent_t extend {`,
         `    measure: extra is count()`,
         `  } on id = a_extend.id`,
         ``,
         `  // Plain on-clause join — MUST NOT be nested-index.`,
         `  join_one: a_plain is parent_t on id = a_plain.id`,
         `}`,
         ``,
      ].join("\n");

      await writeFile(
         path.join(workspace, "entry.malloy"),
         entryModel,
         "utf-8",
      );

      const { model, docString } = await compileModel(
         workspace,
         "entry.malloy",
      );

      const entities = sourceExtraction.extractEntitiesFromExplores(
         model.explores,
         "entry.malloy",
         workspace,
         docString,
      );

      const findJoin = (name: string) =>
         entities.find((e) => e.source === "events" && e.name === name);

      const direct = findJoin("a_direct");
      const ext = findJoin("a_extend");
      const plain = findJoin("a_plain");

      assert.ok(direct, "a_direct join entity missing");
      assert.ok(ext, "a_extend join entity missing");
      assert.ok(plain, "a_plain join entity missing");

      assert.strictEqual(
         direct.join_source_metadata?.is_nested_index,
         true,
         "direct-table join MUST be is_nested_index=true (otherwise nested fields never get indexed)",
      );
      assert.strictEqual(
         direct.join_source_metadata?.original_source_name,
         null,
         "direct-table join has no named source",
      );

      assert.strictEqual(
         ext.join_source_metadata?.is_nested_index,
         true,
         "extend join MUST be is_nested_index=true (otherwise extended fields never get indexed)",
      );
      assert.strictEqual(
         ext.join_source_metadata?.original_source_name,
         "parent_t",
      );

      assert.strictEqual(
         plain.join_source_metadata?.is_nested_index,
         false,
         "plain on-clause join must NOT be is_nested_index",
      );
      assert.strictEqual(
         plain.join_source_metadata?.original_source_name,
         "parent_t",
      );

      // The nested-index recursion should have produced entities for
      // the direct-table and extend joins' fields. Leaf fields under a
      // nested-index chain are stored with the top-level runnable
      // source (``events``) plus a join-qualified ``name`` prefixed by
      // the join alias (``a_direct.<field>`` / ``a_extend.<field>``) —
      // not with a dotted, non-runnable ``source="events.a_direct"``.
      const directNestedFields = entities.filter(
         (e) =>
            e.source === "events" &&
            e.name.startsWith("a_direct.") &&
            e.field_type !== "join",
      );
      const extendNestedFields = entities.filter(
         (e) =>
            e.source === "events" &&
            e.name.startsWith("a_extend.") &&
            e.field_type !== "join",
      );
      assert.ok(
         directNestedFields.length > 0,
         "expected nested entities prefixed `a_direct.` under source `events` (nested-index recursion)",
      );
      assert.ok(
         extendNestedFields.length > 0,
         "expected nested entities prefixed `a_extend.` under source `events` (nested-index recursion)",
      );
   }, 60_000);
});
