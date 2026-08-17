import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs/promises";
import * as os from "os";
import * as path from "path";

import { Environment } from "./environment";

/**
 * The publisher must surface a non-null `materialization` object on a loaded
 * package's metadata so the control plane can treat object-present as the
 * authoritative manifest policy ("this is what the manifest says") and
 * object-absent as "metadata not loaded this request" — never as a schedule
 * removal. A metadata PATCH must not drop that policy from the in-memory
 * metadata. Regression coverage for the re-materialization schedule self-wipe
 * (docs/bugs/materialization-schedule-self-wipe.md).
 *
 * Runs against a real `Environment` + real `Package.create` over temp dirs.
 */
describe("materialization schedule surfacing", () => {
   const MODEL = `source: ones is duckdb.sql("SELECT 1 as x")\n`;
   let rootDir: string;
   let envPath: string;

   async function writePackageDir(
      manifest: Record<string, unknown>,
   ): Promise<void> {
      const dir = path.join(envPath, "pkg");
      await fs.mkdir(dir, { recursive: true });
      await fs.writeFile(
         path.join(dir, "publisher.json"),
         JSON.stringify({ name: "pkg", description: "fixture", ...manifest }),
      );
      await fs.writeFile(path.join(dir, "model.malloy"), MODEL);
   }

   beforeEach(async () => {
      rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-mat-"));
      envPath = path.join(rootDir, "env");
      await fs.mkdir(envPath, { recursive: true });
   });

   afterEach(async () => {
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   it(
      "surfaces the manifest's materialization.schedule on load",
      async () => {
         const env = await Environment.create("testEnv", envPath, []);
         await writePackageDir({ materialization: { schedule: "0 6 * * *" } });
         await env.addPackage("pkg");

         const pkg = await env.getPackage("pkg", false);
         expect(pkg.getPackageMetadata().materialization).toEqual({
            schedule: "0 6 * * *",
            freshness: null,
            queryMetadata: null,
         });
      },
      { timeout: 20000 },
   );

   it(
      "surfaces the manifest's materialization.freshness on load",
      async () => {
         const env = await Environment.create("testEnv", envPath, []);
         await writePackageDir({
            materialization: {
               freshness: { window: "24h", fallback: "stale_ok" },
            },
         });
         await env.addPackage("pkg");

         // Freshness rides the same channel as schedule: present-and-verbatim
         // when declared, null when unset — the control plane owns the
         // scheduling/gating logic and only needs the values surfaced.
         const pkg = await env.getPackage("pkg", false);
         expect(pkg.getPackageMetadata().materialization).toEqual({
            schedule: null,
            freshness: { window: "24h", fallback: "stale_ok" },
            queryMetadata: null,
         });
      },
      { timeout: 20000 },
   );

   it(
      "surfaces a non-null materialization object even when the manifest declares none",
      async () => {
         const env = await Environment.create("testEnv", envPath, []);
         await writePackageDir({});
         await env.addPackage("pkg");

         // Object present with a null schedule — NOT a null object — so the
         // control plane reads it as an authoritative "no schedule declared",
         // not as "metadata unavailable".
         const pkg = await env.getPackage("pkg", false);
         expect(pkg.getPackageMetadata().materialization).toEqual({
            schedule: null,
            freshness: null,
         });
      },
      { timeout: 20000 },
   );

   it(
      "preserves the materialization policy across a metadata PATCH",
      async () => {
         const env = await Environment.create("testEnv", envPath, []);
         await writePackageDir({ materialization: { schedule: "0 6 * * *" } });
         await env.addPackage("pkg");

         // A description-only PATCH must not wipe the schedule from the
         // in-memory metadata. The bug: setPackageMetadata replaces the whole
         // object, so a later getPackage reported no schedule and the control
         // plane misread the gap as a removal and cleared the cadence.
         const updated = await env.updatePackage("pkg", {
            name: "pkg",
            description: "updated",
         });
         expect(updated.materialization).toEqual({
            schedule: "0 6 * * *",
            freshness: null,
            queryMetadata: null,
         });

         const pkg = await env.getPackage("pkg", false);
         expect(pkg.getPackageMetadata().materialization).toEqual({
            schedule: "0 6 * * *",
            freshness: null,
            queryMetadata: null,
         });
      },
      { timeout: 20000 },
   );

   async function readManifest(): Promise<Record<string, unknown>> {
      return JSON.parse(
         await fs.readFile(path.join(envPath, "pkg", "publisher.json"), "utf8"),
      );
   }

   it(
      "writes scope to both homes so the manifest it authors still loads",
      async () => {
         // A scope PATCH used to write only the manifest root. With scope also
         // living in the materialization block, that authored a manifest whose
         // two homes disagreed — which the loader refuses, so the package
         // survived the PATCH and then failed on the next restart.
         const env = await Environment.create("testEnv", envPath, []);
         await writePackageDir({
            materialization: {
               scope: "package",
               queryMetadata: { team: "fin" },
            },
         });
         await env.addPackage("pkg");

         await env.updatePackage("pkg", { name: "pkg", scope: "version" });

         const manifest = await readManifest();
         expect(manifest.scope).toBe("version");
         expect(manifest.materialization).toMatchObject({
            scope: "version",
            queryMetadata: { team: "fin" },
         });

         // The real assertion: it loads again.
         const reloaded = await Environment.create("testEnv", envPath, []);
         await reloaded.addPackage("pkg");
         const pkg = await reloaded.getPackage("pkg", false);
         expect(pkg.getPackageMetadata().scope).toBe("version");
      },
      { timeout: 20000 },
   );

   it(
      "keeps the envelope scope a materialization PATCH cannot express",
      async () => {
         // The wire materialization block has no `scope`, so a PATCH that sends
         // one must not be read as "the author dropped it".
         const env = await Environment.create("testEnv", envPath, []);
         await writePackageDir({ materialization: { scope: "version" } });
         await env.addPackage("pkg");

         await env.updatePackage("pkg", {
            name: "pkg",
            materialization: { schedule: "0 6 * * *" },
         });

         const manifest = await readManifest();
         expect(manifest.materialization).toMatchObject({
            scope: "version",
            schedule: "0 6 * * *",
         });
         expect(manifest.scope).toBe("version");
      },
      { timeout: 20000 },
   );

   it(
      "keeps queryMetadata a schedule PATCH said nothing about",
      async () => {
         // The block is replaced wholesale, which is right for the policy the
         // caller is setting — but queryMetadata is orthogonal to it, so a
         // client that sets a schedule without re-sending the tags would
         // silently untag every statement the package's builds issue. The UI
         // and the control plane are separate clients; preserving it here fixes
         // all of them at once.
         const env = await Environment.create("testEnv", envPath, []);
         await writePackageDir({
            materialization: {
               scope: "version",
               queryMetadata: { team: "finance" },
            },
         });
         await env.addPackage("pkg");

         await env.updatePackage("pkg", {
            name: "pkg",
            materialization: { schedule: "0 6 * * *" },
         });

         expect((await readManifest()).materialization).toMatchObject({
            schedule: "0 6 * * *",
            queryMetadata: { team: "finance" },
         });
      },
      { timeout: 20000 },
   );

   it(
      "treats an explicit null as omitted, at the deprecated home too",
      async () => {
         // Null used to clear HERE while null on the canonical field preserved.
         // That asymmetry protected a client which serializes unset fields as
         // null only once it had migrated off this home — precisely the client
         // that has not. One rule at both homes now: null preserves, an empty
         // bag clears.
         const env = await Environment.create("testEnv", envPath, []);
         await writePackageDir({
            materialization: {
               scope: "version",
               queryMetadata: { team: "finance" },
            },
         });
         await env.addPackage("pkg");

         await env.updatePackage("pkg", {
            name: "pkg",
            materialization: { schedule: "0 6 * * *", queryMetadata: null },
         });

         expect(
            (await readManifest()).materialization as Record<string, unknown>,
         ).toMatchObject({
            schedule: "0 6 * * *",
            queryMetadata: { team: "finance" },
         });
      },
      { timeout: 20000 },
   );

   it(
      "surfaces a manifest's queryMetadata in both wire homes",
      async () => {
         // The canonical field is the one a client should read; the block is
         // populated alongside it only so an un-migrated client keeps working.
         const env = await Environment.create("testEnv", envPath, []);
         await writePackageDir({ queryMetadata: { team: "fin" } });
         await env.addPackage("pkg");

         const pkg = await env.getPackage("pkg", false);
         const metadata = pkg.getPackageMetadata();
         expect(metadata.queryMetadata).toEqual({ team: "fin" });
         expect(metadata.materialization).toMatchObject({
            queryMetadata: { team: "fin" },
         });
         expect(pkg.getDeclaredQueryMetadata()).toEqual({ team: "fin" });
      },
      { timeout: 20000 },
   );

   it(
      "preserves queryMetadata across a metadata PATCH",
      async () => {
         // Same failure mode as the schedule above: the whole metadata object is
         // replaced, so a description-only PATCH would silently untag every
         // query the package emits until the next reload.
         const env = await Environment.create("testEnv", envPath, []);
         await writePackageDir({ queryMetadata: { team: "fin" } });
         await env.addPackage("pkg");

         const updated = await env.updatePackage("pkg", {
            name: "pkg",
            description: "updated",
         });
         expect(updated.queryMetadata).toEqual({ team: "fin" });

         const pkg = await env.getPackage("pkg", false);
         expect(pkg.getDeclaredQueryMetadata()).toEqual({ team: "fin" });
      },
      { timeout: 20000 },
   );

   it(
      "accepts queryMetadata at either wire home on a PATCH",
      async () => {
         // A migrated client sends the canonical field; an un-migrated one sends
         // the block. Both have to reach the manifest, and both homes have to
         // agree in what is written — the manifest this authors is loaded again
         // on the next restart.
         const env = await Environment.create("testEnv", envPath, []);
         await writePackageDir({});
         await env.addPackage("pkg");

         await env.updatePackage("pkg", {
            name: "pkg",
            queryMetadata: { team: "fin" },
         });
         // Canonical home only. The deprecated block is mirrored into when it
         // already exists or the caller wrote to it — synthesizing one here
         // would introduce the shape being migrated AWAY from into a manifest
         // whose author only ever used the canonical field.
         const afterCanonical = await readManifest();
         expect(afterCanonical.queryMetadata).toEqual({ team: "fin" });
         expect(afterCanonical.materialization).toBeUndefined();

         await env.updatePackage("pkg", {
            name: "pkg",
            materialization: { queryMetadata: { team: "ops" } },
         });
         expect(await readManifest()).toMatchObject({
            queryMetadata: { team: "ops" },
            materialization: { queryMetadata: { team: "ops" } },
         });

         const reloaded = await Environment.create("testEnv", envPath, []);
         await reloaded.addPackage("pkg");
         expect(
            (
               await reloaded.getPackage("pkg", false)
            ).getDeclaredQueryMetadata(),
         ).toEqual({ team: "ops" });
      },
      { timeout: 20000 },
   );

   it(
      "clears a canonically-declared bag through the deprecated home, with an empty one",
      async () => {
         // The clear has to reach the root, not just the block it was sent in:
         // a manifest written in the canonical form carries the bag at the root,
         // so a PATCH that cleared only the block would leave the package tagged
         // by the very field the loader prefers.
         const env = await Environment.create("testEnv", envPath, []);
         await writePackageDir({ queryMetadata: { team: "finance" } });
         await env.addPackage("pkg");

         await env.updatePackage("pkg", {
            name: "pkg",
            materialization: { queryMetadata: {} },
         });

         const manifest = await readManifest();
         expect(manifest.queryMetadata).toEqual({});
         expect(manifest.materialization).toMatchObject({
            queryMetadata: {},
         });

         const reloaded = await Environment.create("testEnv", envPath, []);
         await reloaded.addPackage("pkg");
         expect(
            (
               await reloaded.getPackage("pkg", false)
            ).getDeclaredQueryMetadata(),
         ).toBeNull();
      },
      { timeout: 20000 },
   );

   it(
      "clears a bag through the canonical home with an empty one",
      async () => {
         // A null is preserve-on-absent here, like scope, so a client that
         // serializes unset fields as null cannot untag a package by accident.
         // An empty bag is the unambiguous clear, and no such client produces
         // one.
         const env = await Environment.create("testEnv", envPath, []);
         await writePackageDir({ queryMetadata: { team: "finance" } });
         await env.addPackage("pkg");

         await env.updatePackage("pkg", { name: "pkg", queryMetadata: null });
         expect((await readManifest()).queryMetadata).toEqual({
            team: "finance",
         });

         await env.updatePackage("pkg", { name: "pkg", queryMetadata: {} });
         expect((await readManifest()).queryMetadata).toEqual({});

         // An empty bag parses to "no tags", so the cleared package declares
         // nothing rather than an empty layer that merges to the same thing.
         const reloaded = await Environment.create("testEnv", envPath, []);
         await reloaded.addPackage("pkg");
         expect(
            (
               await reloaded.getPackage("pkg", false)
            ).getDeclaredQueryMetadata(),
         ).toBeNull();
      },
      { timeout: 20000 },
   );

   it(
      "keeps BOTH in-memory homes agreeing after a PATCH",
      async () => {
         // The two homes are read by different paths — the serve path takes the
         // canonical field through `getDeclaredQueryMetadata`, the build path
         // takes the block — so setting them independently made a PATCH that
         // touched one tag served queries with the new bag and builds with the
         // old one. Asserted against the SAME env, not a fresh one: a reload
         // resolves the manifest and would hide the divergence under test.
         const env = await Environment.create("testEnv", envPath, []);
         await writePackageDir({
            materialization: { queryMetadata: { team: "finance" } },
         });
         await env.addPackage("pkg");

         const updated = await env.updatePackage("pkg", {
            name: "pkg",
            queryMetadata: { team: "platform" },
         });

         expect(updated.queryMetadata).toEqual({ team: "platform" });
         expect(updated.materialization).toMatchObject({
            queryMetadata: { team: "platform" },
         });

         const pkg = await env.getPackage("pkg", false);
         expect(pkg.getDeclaredQueryMetadata()).toEqual({ team: "platform" });
         expect(pkg.getMaterializationConfig()?.queryMetadata).toEqual({
            team: "platform",
         });
      },
      { timeout: 20000 },
   );

   it(
      "does not untag the package when a PATCH edits only the schedule",
      async () => {
         // A materialization PATCH replaces the block wholesale, and the block
         // is what the BUILD path reads. Resolving the bag from the body alone
         // would drop it, so the next build's statements go out untagged for a
         // package that asked to be tagged.
         const env = await Environment.create("testEnv", envPath, []);
         await writePackageDir({
            scope: "version",
            queryMetadata: { team: "finance" },
         });
         await env.addPackage("pkg");

         await env.updatePackage("pkg", {
            name: "pkg",
            materialization: { schedule: "0 6 * * *" },
         });

         const pkg = await env.getPackage("pkg", false);
         expect(pkg.getDeclaredQueryMetadata()).toEqual({ team: "finance" });
         expect(pkg.getMaterializationConfig()).toMatchObject({
            schedule: "0 6 * * *",
            queryMetadata: { team: "finance" },
         });
      },
      { timeout: 20000 },
   );

   it(
      "warns about a package+model tag overflow with no source declaring one",
      async () => {
         // End-to-end over a real Environment: the floor has to be SIZED, not
         // just checkable. Sizing only the sources that declare their own bag
         // meant this package published clean while every query it serves shed
         // context properties.
         const env = await Environment.create("testEnv", envPath, []);
         const six = (prefix: string) =>
            Object.fromEntries(
               Array.from({ length: 6 }, (_, i) => [`${prefix}${i}`, "v"]),
            );
         const dir = path.join(envPath, "pkg");
         await fs.mkdir(dir, { recursive: true });
         await fs.writeFile(
            path.join(dir, "publisher.json"),
            JSON.stringify({
               name: "pkg",
               description: "fixture",
               queryMetadata: six("p"),
            }),
         );
         // Six more at the model file, and no `#@` on the source at all.
         await fs.writeFile(
            path.join(dir, "model.malloy"),
            Object.entries(six("m"))
               .map(([name, value]) => `## queryMetadata.${name}="${value}"`)
               .join("\n") +
               "\n" +
               MODEL,
         );
         await env.addPackage("pkg");

         const warnings =
            (await env.getPackage("pkg", false)).getPackageMetadata()
               .warnings ?? [];
         const overflow = warnings.filter((w) =>
            w.message?.includes("package + model file, merged"),
         );
         expect(overflow).toHaveLength(1);
         expect(overflow[0].model).toBe("model.malloy");
         expect(overflow[0].subject).toBeUndefined();
      },
      { timeout: 20000 },
   );
});
