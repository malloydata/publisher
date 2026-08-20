// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs";
import os from "os";
import path from "path";
import { describeWorkspaceSetup, jsonErrorPosition } from "./workspace_setup";

/** The phrase the missing-manifest branch uses, asserted against by name. */
const PACKAGE_MANIFEST_TEXT = "has no publisher.json";

/**
 * These run against real directories, because every branch here is a
 * filesystem fact and a mocked `fs` would pin the mock rather than the
 * behaviour the tool reports to an agent.
 */
let root: string;
const savedEnv = {
   configPath: process.env.PUBLISHER_CONFIG_PATH,
   bundled: process.env.PUBLISHER_USE_BUNDLED_DEFAULT,
};

beforeEach(() => {
   root = fs.mkdtempSync(path.join(os.tmpdir(), "wsetup-"));
   delete process.env.PUBLISHER_CONFIG_PATH;
   delete process.env.PUBLISHER_USE_BUNDLED_DEFAULT;
});

afterEach(() => {
   fs.rmSync(root, { recursive: true, force: true });
   if (savedEnv.configPath === undefined)
      delete process.env.PUBLISHER_CONFIG_PATH;
   else process.env.PUBLISHER_CONFIG_PATH = savedEnv.configPath;
   if (savedEnv.bundled === undefined)
      delete process.env.PUBLISHER_USE_BUNDLED_DEFAULT;
   else process.env.PUBLISHER_USE_BUNDLED_DEFAULT = savedEnv.bundled;
});

function writeConfig(contents: unknown): void {
   fs.writeFileSync(
      path.join(root, "publisher.config.json"),
      typeof contents === "string" ? contents : JSON.stringify(contents),
   );
}

/**
 * Windows chmod only toggles a read-only bit and cannot make a file
 * unreadable, and root bypasses the mode entirely, so the EACCES case cannot
 * be BUILT everywhere. Probed rather than keyed on a platform name, since the
 * blocker is the capability. The read-failure BRANCH is covered
 * unconditionally by the directory case, so this skip costs no coverage.
 */
function canMakeUnreadable(): boolean {
   const probe = path.join(os.tmpdir(), `wsetup-perm-${process.pid}`);
   try {
      fs.writeFileSync(probe, "x");
      fs.chmodSync(probe, 0o000);
      fs.readFileSync(probe);
      return false; // the read succeeded, so the mode did not take
   } catch {
      return true;
   } finally {
      try {
         fs.chmodSync(probe, 0o600);
         fs.rmSync(probe, { force: true });
      } catch {
         /* best effort */
      }
   }
}

/** Bound once: probing per test would create and delete a file every run. */
const itIfPerms = canMakeUnreadable() ? it : it.skip;

/** A fully-started server, which is the only state this tool diagnoses. */
function serving(
   environments: Array<{ name?: string; packages: unknown[] }> = [],
) {
   return { initialized: true, operationalState: "serving", environments };
}

describe("describeWorkspaceSetup", () => {
   it("returns nothing when a package is being served", async () => {
      const setup = await describeWorkspaceSetup(
         root,
         serving([{ name: "local", packages: ["mypkg"] }]),
      );
      expect(setup).toBeUndefined();
   });

   it("returns nothing when only one of several environments has a package", async () => {
      const setup = await describeWorkspaceSetup(
         root,
         serving([
            { name: "a", packages: [] },
            { name: "b", packages: ["served"] },
         ]),
      );
      expect(setup).toBeUndefined();
   });

   it("stays silent while the server is still starting up", async () => {
      // The MCP endpoint has no readiness gate and getStatus skips the
      // initialization wait, so a cold boot looks exactly like an un-set-up
      // directory. Telling an agent to restart mid-boot would be wrong.
      expect(
         await describeWorkspaceSetup(root, {
            initialized: false,
            operationalState: "initializing",
            environments: [],
         }),
      ).toBeUndefined();
      expect(
         await describeWorkspaceSetup(root, {
            initialized: true,
            operationalState: "initializing",
            environments: [],
         }),
      ).toBeUndefined();
   });

   it("names the missing server config when the directory is empty", async () => {
      const setup = await describeWorkspaceSetup(root, serving());
      expect(setup).toBeDefined();
      expect(setup!.configFile).toBeNull();
      expect(setup!.problem).toContain("publisher.config.json");
      expect(setup!.nextAction).toContain(
         "npm create @malloy-publisher/malloy-package@latest",
      );
      expect("unclaimedModelFiles" in setup!).toBe(false);
   });

   it("lists model files that no package claims, at the root and one level down", async () => {
      fs.writeFileSync(path.join(root, "sales.malloy"), "");
      fs.mkdirSync(path.join(root, "nested"));
      fs.writeFileSync(path.join(root, "nested", "orders.malloy"), "");
      const setup = await describeWorkspaceSetup(root, serving());
      expect(setup!.unclaimedModelFiles).toEqual(
         expect.arrayContaining(["sales.malloy", "nested/orders.malloy"]),
      );
      expect(setup!.problem).toContain(".malloy");
   });

   it("skips server-managed and dependency directories when scanning", async () => {
      fs.mkdirSync(path.join(root, "publisher_data"), { recursive: true });
      fs.writeFileSync(path.join(root, "publisher_data", "copy.malloy"), "");
      fs.mkdirSync(path.join(root, "node_modules"), { recursive: true });
      fs.writeFileSync(path.join(root, "node_modules", "dep.malloy"), "");
      const setup = await describeWorkspaceSetup(root, serving());
      expect("unclaimedModelFiles" in setup!).toBe(false);
   });

   it("reports the package source path and steers away from publisher_data", async () => {
      fs.mkdirSync(path.join(root, "mypkg"));
      writeConfig({
         environments: [
            {
               name: "local",
               packages: [{ name: "mypkg", location: "./mypkg" }],
            },
         ],
      });
      const setup = await describeWorkspaceSetup(
         root,
         serving([{ name: "local", packages: [] }]),
      );
      expect(setup!.unservedPackages).toHaveLength(1);
      const pkg = setup!.unservedPackages![0];
      expect(pkg.hasManifest).toBe(false);
      expect(pkg.sourcePath).toBe(path.resolve(root, "mypkg"));
      expect(pkg.sourcePath).not.toContain("publisher_data");
      expect(setup!.nextAction).toContain("publisher.json");
      expect(setup!.nextAction).toContain("publisher_data");
      // Same class as the generic branch: never point at a list that is not
      // in this payload. Reachable with no loadErrors when an environment is
      // skipped whole (a missing `name` key), so its package is never tried.
      expect(setup!.nextAction).not.toContain("named in loadErrors");
   });

   it("expands a ~/ package location the way the server does", async () => {
      writeConfig({
         environments: [
            {
               name: "local",
               packages: [{ name: "mypkg", location: "~/models/sales" }],
            },
         ],
      });
      const setup = await describeWorkspaceSetup(
         root,
         serving([{ name: "local", packages: [] }]),
      );
      const sourcePath = setup!.unservedPackages![0].sourcePath!;
      expect(sourcePath).toBe(path.join(os.homedir(), "models/sales"));
      // The bug this guards: resolving against the config dir yields a
      // directory literally named "~", which the user cannot act on.
      expect(sourcePath).not.toContain("~");
   });

   it("does not tell the user to create a manifest inside a remote location", async () => {
      writeConfig({
         environments: [
            {
               name: "local",
               packages: [
                  {
                     name: "remote",
                     location: "https://github.com/malloydata/publisher",
                  },
               ],
            },
         ],
      });
      const setup = await describeWorkspaceSetup(
         root,
         serving([{ name: "local", packages: [] }]),
      );
      const pkg = setup!.unservedPackages![0];
      expect(pkg.location).toBe("https://github.com/malloydata/publisher");
      // Neither key is present: a URL is not a path, and claiming the manifest
      // is missing would route a download failure into "create a file there".
      expect("sourcePath" in pkg).toBe(false);
      expect("hasManifest" in pkg).toBe(false);
      expect(setup!.problem).not.toContain(PACKAGE_MANIFEST_TEXT);
      expect(setup!.nextAction).toContain("remote location");
   });

   it("distinguishes a present manifest, pointing at the compile failure", async () => {
      fs.mkdirSync(path.join(root, "mypkg"));
      fs.writeFileSync(
         path.join(root, "mypkg", "publisher.json"),
         '{"name":"mypkg"}',
      );
      writeConfig({
         environments: [
            {
               name: "local",
               packages: [{ name: "mypkg", location: "./mypkg" }],
               connections: [{ name: "warehouse", type: "postgres" }],
            },
         ],
      });
      // A compile failure is a scenario that genuinely HAS load errors, so the
      // status must carry one; without it the advice correctly refuses to
      // promise a list that is not there.
      const setup = await describeWorkspaceSetup(root, {
         initialized: true,
         operationalState: "serving",
         environments: [{ name: "local", packages: [] }],
         loadErrors: [{ environment: "local", package: "mypkg", message: "x" }],
      });
      expect(setup!.unservedPackages![0].hasManifest).toBe(true);
      expect(setup!.problem).toContain("loadErrors");
      expect(setup!.configuredConnections).toEqual(["warehouse"]);
   });

   it("says so when the config declares no packages at all", async () => {
      writeConfig({ environments: [{ name: "local", packages: [] }] });
      const setup = await describeWorkspaceSetup(
         root,
         serving([{ name: "local", packages: [] }]),
      );
      expect(setup!.problem).toContain("declares no packages");
      expect("unservedPackages" in setup!).toBe(false);
   });

   it("reports a config that does not parse as a parse failure, not as empty", async () => {
      // getPublisherConfig swallows this and returns an empty environment list,
      // which is indistinguishable from a valid config declaring nothing. The
      // two need opposite advice.
      writeConfig(
         '{ "connections": [ { "password": "hunter2-Sup3rSecret" }, ] }',
      );
      const setup = await describeWorkspaceSetup(root, serving());
      expect(setup!.problem).toContain("could not be read as JSON");
      // Pins the CALL SITE, not just the sanitizer: the runtime's own prose is
      // what carries a window of raw source under V8, so none of it may appear.
      // Asserting on the secret alone would pass under Bun either way, since
      // JavaScriptCore does not echo source.
      expect(setup!.problem).not.toContain("hunter2");
      expect(setup!.problem).not.toContain("Unexpected");
      expect(setup!.problem).not.toContain("JSON Parse error");
      expect(setup!.nextAction).toContain("until the file parses");
      expect(setup!.problem).not.toContain("declares no packages");
   });

   it("blames the flag when --config names a file that is not there", async () => {
      process.env.PUBLISHER_CONFIG_PATH = path.join(root, "nope.json");
      const setup = await describeWorkspaceSetup(root, serving());
      expect(setup!.problem).toContain("--config");
      expect(setup!.problem).toContain("nope.json");
      expect(setup!.problem).not.toContain(
         "no environments are configured and",
      );
   });

   it("counts every declared package, not just the ones it lists", async () => {
      const packages = Array.from({ length: 25 }, (_, i) => ({
         name: `pkg${i}`,
         location: `./pkg${i}`,
      }));
      for (const p of packages) fs.mkdirSync(path.join(root, p.location));
      writeConfig({ environments: [{ name: "local", packages }] });
      const setup = await describeWorkspaceSetup(
         root,
         serving([{ name: "local", packages: [] }]),
      );
      expect(setup!.unservedPackages!.length).toBeLessThanOrEqual(10);
      expect(setup!.unservedPackagesTruncated).toBe(true);
      // The count must describe all 25, not the 10 that fit.
      expect(setup!.problem).toContain("25 of 25");
   });

   it("survives config shapes the validated loader would have rejected", async () => {
      writeConfig({
         environments: [
            null,
            { name: "local", packages: "not-an-array", connections: {} },
            { name: "ok", packages: [{ name: "p", location: 42 }] },
         ],
      });
      const setup = await describeWorkspaceSetup(
         root,
         serving([{ name: "local", packages: [] }]),
      );
      // The diagnosis must survive: this is exactly the config that needs it.
      expect(setup).toBeDefined();
      // A package with an unusable location is still a declared package.
      // Reporting "declares no packages" would deny one that is plainly there.
      expect(setup!.problem).not.toContain("declares no packages");
      expect(setup!.unservedPackages).toHaveLength(1);
   });

   it("counts a package whose location key is misspelled", async () => {
      writeConfig({
         environments: [
            {
               name: "local",
               packages: [{ name: "sales", locaton: "./sales" }],
            },
         ],
      });
      const setup = await describeWorkspaceSetup(
         root,
         serving([{ name: "local", packages: [] }]),
      );
      expect(setup!.problem).not.toContain("declares no packages");
   });

   it("separates a location that points at nothing from a missing manifest", async () => {
      // Creating publisher.json inside a path that does not exist would just
      // materialise the typo and leave the real directory unreferenced.
      fs.mkdirSync(path.join(root, "storefront"));
      writeConfig({
         environments: [
            {
               name: "local",
               packages: [{ name: "storefront", location: "./storefornt" }],
            },
         ],
      });
      const setup = await describeWorkspaceSetup(
         root,
         serving([{ name: "local", packages: [] }]),
      );
      expect(setup!.unservedPackages![0].sourcePathKind).toBe("missing");
      expect("hasManifest" in setup!.unservedPackages![0]).toBe(false);
      expect(setup!.problem).toContain("does not exist on disk");
      expect(setup!.nextAction).not.toContain("Create publisher.json");
   });

   it("does not treat ~user or a bare ~ as a local path", async () => {
      // resolvePackageLocation expands only the `~/` prefix, so these would
      // resolve to a directory literally named "~user".
      writeConfig({
         environments: [
            {
               name: "local",
               packages: [{ name: "p", location: "~user/models" }],
            },
         ],
      });
      const setup = await describeWorkspaceSetup(
         root,
         serving([{ name: "local", packages: [] }]),
      );
      const pkg = setup!.unservedPackages![0];
      expect("sourcePath" in pkg).toBe(false);
      expect(pkg.location).toBe("~user/models");
      expect(JSON.stringify(setup)).not.toContain("/~user");
   });

   it("reads the legacy projects key so a legacy config is not called empty", async () => {
      fs.mkdirSync(path.join(root, "mypkg"));
      writeConfig({
         projects: [
            {
               name: "local",
               packages: [{ name: "mypkg", location: "./mypkg" }],
            },
         ],
      });
      const setup = await describeWorkspaceSetup(
         root,
         serving([{ name: "local", packages: [] }]),
      );
      expect(setup!.problem).not.toContain("declares no packages");
      expect(setup!.unservedPackages![0].package).toBe("mypkg");
   });

   it("does not claim nothing is configured when environments came from stored state", async () => {
      // No config file, but the server is serving environments from its
      // database. "No environments are configured" would contradict the
      // environments array in the same payload.
      const setup = await describeWorkspaceSetup(
         root,
         serving([{ name: "from-db", packages: [] }]),
      );
      expect(setup!.problem).not.toContain("no environments are configured");
      expect(setup!.problem).toContain("stored state");
      // --init with no config to rebuild from would delete them outright.
      expect(setup!.nextAction).toContain("Do NOT restart with");
   });

   it("says what --init destroys wherever it recommends it", async () => {
      fs.mkdirSync(path.join(root, "mypkg"));
      writeConfig({
         environments: [
            {
               name: "local",
               packages: [{ name: "mypkg", location: "./mypkg" }],
            },
         ],
      });
      const setup = await describeWorkspaceSetup(
         root,
         serving([{ name: "local", packages: [] }]),
      );
      expect(setup!.nextAction).toContain("--init");
      // The cost must travel with the recommendation: --init removes
      // publisher_data and reloads from config alone, so anything created
      // over the REST API and absent from the config is gone.
      expect(setup!.nextAction).toContain("is lost");
      expect(setup!.nextAction).toContain("REST API");
   });

   it("does not call a --config directory a missing file", async () => {
      const dir = path.join(root, "configs");
      fs.mkdirSync(dir);
      process.env.PUBLISHER_CONFIG_PATH = dir;
      const setup = await describeWorkspaceSetup(root, serving());
      expect(setup!.problem).toContain("is not a file");
      expect(setup!.problem).not.toContain("does not exist");
   });

   it("demands --init only once the server has environments to reload", async () => {
      fs.mkdirSync(path.join(root, "mypkg"));
      writeConfig({
         environments: [
            {
               name: "local",
               packages: [{ name: "mypkg", location: "./mypkg" }],
            },
         ],
      });
      const withEnvs = await describeWorkspaceSetup(
         root,
         serving([{ name: "local", packages: [] }]),
      );
      expect(withEnvs!.nextAction).toContain("--init");
   });

   it("leads with a plain restart when the server has no environments yet", async () => {
      // A first boot in an empty directory: a plain restart reads the new
      // config, so --init must not be the primary instruction (it would
      // needlessly delete publisher_data). It stays available as a fallback,
      // because an empty served list is not proof the database is empty.
      fs.mkdirSync(path.join(root, "publisher_data"), { recursive: true });
      fs.writeFileSync(path.join(root, "sales.malloy"), "");
      const setup = await describeWorkspaceSetup(root, serving());
      const action = setup!.nextAction;
      expect(action).toContain("restart the server (");
      expect(action.indexOf("restart the server (")).toBeLessThan(
         action.indexOf("--init"),
      );
   });

   it("does not claim nothing is configured when the bundled sample config is in force", async () => {
      process.env.PUBLISHER_USE_BUNDLED_DEFAULT = "true";
      const setup = await describeWorkspaceSetup(
         root,
         serving([{ name: "examples", packages: [] }]),
      );
      expect(setup!.problem).not.toContain("no environments are configured");
      expect(setup!.problem).toContain("bundled sample config");
      expect(setup!.nextAction).toContain("loadErrors");
   });

   it("never emits connection attributes, only names", async () => {
      fs.mkdirSync(path.join(root, "mypkg"));
      writeConfig({
         environments: [
            {
               name: "local",
               packages: [{ name: "mypkg", location: "./mypkg" }],
               connections: [
                  {
                     name: "warehouse",
                     type: "postgres",
                     password: "hunter2-should-not-appear",
                  },
               ],
            },
         ],
      });
      const setup = await describeWorkspaceSetup(
         root,
         serving([{ name: "local", packages: [] }]),
      );
      expect(JSON.stringify(setup)).not.toContain("hunter2");
      expect(setup!.configuredConnections).toEqual(["warehouse"]);
   });

   it("substitutes ${ENV_VAR} in a location the way the loader does", async () => {
      // The loader runs every config string through substituteEnvVars before
      // resolving. Comparing the raw text against the filesystem would report
      // a perfectly good interpolated location as missing and tell the user to
      // "correct" a location that is already correct.
      process.env.WSETUP_TEST_ENV = "prod";
      try {
         fs.mkdirSync(path.join(root, "environments", "prod", "models"), {
            recursive: true,
         });
         fs.writeFileSync(
            path.join(root, "environments/prod/models", "publisher.json"),
            '{"name":"p"}',
         );
         writeConfig({
            environments: [
               {
                  name: "local",
                  packages: [
                     {
                        name: "p",
                        location: "./environments/${WSETUP_TEST_ENV}/models",
                     },
                  ],
               },
            ],
         });
         const setup = await describeWorkspaceSetup(
            root,
            serving([{ name: "local", packages: [] }]),
         );
         const pkg = setup!.unservedPackages![0];
         // Substitution still happens, and is still what makes the probe find
         // the real directory: sourcePathKind proves the resolved path was
         // correct. The resolved path itself is withheld, because it would
         // embed the variable's value in an unauthenticated response.
         expect(pkg.sourcePathKind).toBe("directory");
         expect(pkg.hasManifest).toBe(true);
         expect("sourcePath" in pkg).toBe(false);
         expect(setup!.problem).not.toContain("does not exist on disk");
         // The user edits the declared text, so that is what is echoed back.
         expect(pkg.location).toBe("./environments/${WSETUP_TEST_ENV}/models");
      } finally {
         delete process.env.WSETUP_TEST_ENV;
      }
   });

   it("never promises loadErrors that are not in the same payload", async () => {
      fs.mkdirSync(path.join(root, "mypkg"));
      fs.writeFileSync(
         path.join(root, "mypkg", "publisher.json"),
         '{"name":"mypkg"}',
      );
      writeConfig({
         environments: [
            {
               name: "local",
               packages: [{ name: "mypkg", location: "./mypkg" }],
            },
         ],
      });
      const without = await describeWorkspaceSetup(
         root,
         serving([{ name: "local", packages: [] }]),
      );
      expect(without!.problem).not.toContain("see loadErrors");
      expect(without!.problem).toContain("never attempted");

      const withErrors = await describeWorkspaceSetup(root, {
         initialized: true,
         operationalState: "serving",
         environments: [{ name: "local", packages: [] }],
         loadErrors: [{ environment: "local", message: "boom" }],
      });
      expect(withErrors!.problem).toContain("see loadErrors");
   });

   it("gives a package with no usable location its own diagnosis", async () => {
      writeConfig({
         environments: [
            {
               name: "local",
               packages: [{ name: "sales", locaton: "./sales" }],
            },
         ],
      });
      const setup = await describeWorkspaceSetup(
         root,
         serving([{ name: "local", packages: [] }]),
      );
      expect(setup!.problem).toContain('declares no usable "location"');
      // "" was a value the agent could not act on; the key is simply absent.
      expect("location" in setup!.unservedPackages![0]).toBe(false);
      expect(setup!.nextAction).toContain("locaton");
   });

   it("does not call a location that is a file a missing path", async () => {
      // Aiming a location at the model file rather than its directory is a
      // first-timer mistake; "does not exist" is false and sends them hunting.
      fs.writeFileSync(path.join(root, "sales.malloy"), "");
      writeConfig({
         environments: [
            {
               name: "local",
               packages: [{ name: "sales", location: "./sales.malloy" }],
            },
         ],
      });
      const setup = await describeWorkspaceSetup(
         root,
         serving([{ name: "local", packages: [] }]),
      );
      expect(setup!.problem).toContain("is not a directory");
      expect(setup!.problem).not.toContain("does not exist on disk");
      expect(setup!.nextAction).toContain("containing directory");
   });

   it("keeps --init reachable when a plain restart may not take effect", async () => {
      // An environment can be in the database yet absent from the served list
      // (it threw during restore), so an empty environments array is not proof
      // that a plain restart will re-read the config. The advice must not
      // imply it is, or the user loops.
      fs.writeFileSync(path.join(root, "sales.malloy"), "");
      const setup = await describeWorkspaceSetup(root, serving());
      expect(setup!.nextAction).toContain("--init");
      expect(setup!.nextAction).toContain("does not take effect");
   });

   it("does not point at a sourcePath it withheld", async () => {
      // The security fix withholds sourcePath for an interpolated location,
      // which left the missing-manifest advice telling the agent to look at a
      // field that is not on the entry. Same class as the loadErrors guard.
      process.env.WSETUP_DIRVAR = "prod";
      try {
         fs.mkdirSync(path.join(root, "prod"), { recursive: true });
         writeConfig({
            environments: [
               {
                  name: "local",
                  packages: [{ name: "p", location: "./${WSETUP_DIRVAR}" }],
               },
            ],
         });
         const setup = await describeWorkspaceSetup(
            root,
            serving([{ name: "local", packages: [] }]),
         );
         const pkg = setup!.unservedPackages![0];
         expect(pkg.hasManifest).toBe(false);
         expect("sourcePath" in pkg).toBe(false);
         expect(setup!.nextAction).not.toContain("sourcePath below");
         expect(setup!.nextAction).toContain("once its variable is expanded");
      } finally {
         delete process.env.WSETUP_DIRVAR;
      }
   });

   it("never echoes a resolved path built from a substituted location", async () => {
      // Sibling of the name leak, and it survived the first fix: the DECLARED
      // location is echoed safely, but sourcePath is the RESOLVED path and
      // embedded the variable's value. The kind and manifest facts are derived
      // from the same path and disclose nothing, so they stay.
      process.env.WSETUP_PATH_SECRET = "SECRET-IN-A-PATH";
      try {
         writeConfig({
            environments: [
               {
                  name: "local",
                  packages: [
                     { name: "p", location: "./${WSETUP_PATH_SECRET}/models" },
                  ],
               },
            ],
         });
         const setup = await describeWorkspaceSetup(
            root,
            serving([{ name: "local", packages: [] }]),
         );
         expect(JSON.stringify(setup)).not.toContain("SECRET-IN-A-PATH");
         const pkg = setup!.unservedPackages![0];
         expect("sourcePath" in pkg).toBe(false);
         expect(pkg.location).toBe("./${WSETUP_PATH_SECRET}/models");
         // Still diagnosed: the derived facts are safe to report.
         expect(pkg.sourcePathKind).toBe("missing");
      } finally {
         delete process.env.WSETUP_PATH_SECRET;
      }
   });

   it("never substitutes a name, because the value could be a secret", async () => {
      // A name is an arbitrary config string, so it can be written as ${VAR}.
      // Substituting it would put that variable's VALUE into an
      // unauthenticated response, and this block is emitted only when nothing
      // is being served, so no other surface has published that name: it would
      // be the sole discloser. Probed and confirmed leaking before this fix.
      process.env.WSETUP_SECRET = "SUPER-SECRET-TOKEN-VALUE";
      try {
         fs.mkdirSync(path.join(root, "mypkg"));
         writeConfig({
            environments: [
               {
                  name: "${WSETUP_SECRET}",
                  connections: [{ name: "${WSETUP_SECRET}" }],
                  packages: [{ name: "${WSETUP_SECRET}", location: "./mypkg" }],
               },
            ],
         });
         const setup = await describeWorkspaceSetup(
            root,
            serving([{ name: "x", packages: [] }]),
         );
         expect(JSON.stringify(setup)).not.toContain(
            "SUPER-SECRET-TOKEN-VALUE",
         );
         // Shown as written instead, which is the safe direction.
         expect(setup!.configuredConnections).toEqual(["${WSETUP_SECRET}"]);
         expect(setup!.unservedPackages![0].environment).toBe(
            "${WSETUP_SECRET}",
         );
         expect(setup!.unservedPackages![0].package).toBe("${WSETUP_SECRET}");
      } finally {
         delete process.env.WSETUP_SECRET;
      }
   });

   it("keeps a file location and a missing location distinguishable in one config", async () => {
      // Both used to report sourcePathExists false, so whichever branch won
      // gave advice that was wrong for the other package.
      fs.writeFileSync(path.join(root, "sales.malloy"), "");
      writeConfig({
         environments: [
            {
               name: "local",
               packages: [
                  { name: "asFile", location: "./sales.malloy" },
                  { name: "absent", location: "./not-here" },
               ],
            },
         ],
      });
      const setup = await describeWorkspaceSetup(
         root,
         serving([{ name: "local", packages: [] }]),
      );
      const byName = Object.fromEntries(
         setup!.unservedPackages!.map((p) => [p.package, p.sourcePathKind]),
      );
      expect(byName.asFile).toBe("file");
      expect(byName.absent).toBe("missing");
      // The winning branch must acknowledge the other fault exists.
      expect(setup!.nextAction).toContain("missing");
   });

   it("skips symlinks at both depths, not just the top level", async () => {
      // The invariant the module's docstring states. It originally held at the
      // top level only, because the second readdir took no withFileTypes, so a
      // symlinked model one level down was listed. Both halves are pinned here
      // because only the half that worked had ever been checked.
      const outside = fs.mkdtempSync(path.join(os.tmpdir(), "outside-"));
      try {
         fs.writeFileSync(path.join(outside, "target.malloy"), "");
         fs.writeFileSync(path.join(root, "real_root.malloy"), "");
         fs.symlinkSync(
            path.join(outside, "target.malloy"),
            path.join(root, "link_root.malloy"),
         );
         fs.symlinkSync(outside, path.join(root, "linked_dir"));
         fs.mkdirSync(path.join(root, "sub"));
         fs.writeFileSync(path.join(root, "sub", "real_sub.malloy"), "");
         fs.symlinkSync(
            path.join(outside, "target.malloy"),
            path.join(root, "sub", "link_sub.malloy"),
         );

         const setup = await describeWorkspaceSetup(root, serving());
         expect(setup!.unclaimedModelFiles).toEqual([
            "real_root.malloy",
            "sub/real_sub.malloy",
         ]);
      } finally {
         fs.rmSync(outside, { recursive: true, force: true });
      }
   });

   it("returns unclaimed model files in a stable order, capped after sorting", async () => {
      // readdir order is filesystem-dependent, so capping the raw order would
      // make WHICH files survive vary by platform.
      for (let i = 20; i >= 1; i--) {
         fs.writeFileSync(
            path.join(root, `m${String(i).padStart(2, "0")}.malloy`),
            "",
         );
      }
      const setup = await describeWorkspaceSetup(root, serving());
      const listed = setup!.unclaimedModelFiles!;
      expect(listed).toHaveLength(10);
      expect(listed).toEqual([...listed].sort());
      // Sorted BEFORE the cap, so it is the first ten by name, not whichever
      // ten the filesystem happened to hand back.
      expect(listed[0]).toBe("m01.malloy");
      expect(listed[9]).toBe("m10.malloy");
   });

   it("caps configuredConnections and says when it did", async () => {
      fs.mkdirSync(path.join(root, "mypkg"));
      writeConfig({
         environments: [
            {
               name: "local",
               connections: Array.from({ length: 25 }, (_, i) => ({
                  name: `tenant${String(i).padStart(2, "0")}`,
               })),
               packages: [{ name: "mypkg", location: "./mypkg" }],
            },
         ],
      });
      const setup = await describeWorkspaceSetup(
         root,
         serving([{ name: "local", packages: [] }]),
      );
      expect(setup!.configuredConnections!.length).toBe(10);
      expect(setup!.configuredConnectionsTruncated).toBe(true);
   });

   it("describes an unset variable without asserting the mechanism", async () => {
      // Wording must stay true whether one unset variable rejects the whole
      // config (today) or drops just its own environment (once substitution
      // attributes per entry). So it never claims the whole file is affected,
      // and it points at loadErrors only when there are any.
      fs.mkdirSync(path.join(root, "mypkg"));
      fs.writeFileSync(
         path.join(root, "mypkg", "publisher.json"),
         '{"name":"mypkg"}',
      );
      writeConfig({
         environments: [
            {
               name: "local",
               connections: [{ name: "wh", password: "${WSETUP_UNSET_VAR}" }],
               packages: [{ name: "mypkg", location: "./mypkg" }],
            },
         ],
      });

      const noErrors = await describeWorkspaceSetup(root, serving());
      expect(noErrors!.problem).toContain("WSETUP_UNSET_VAR");
      expect(noErrors!.problem).not.toContain("ENTIRE");
      expect(noErrors!.problem).toContain("before any environment was created");
      expect(noErrors!.nextAction).not.toContain("Every package in this file");

      const withErrors = await describeWorkspaceSetup(root, {
         initialized: true,
         operationalState: "serving",
         environments: [{ name: "local", packages: [] }],
         loadErrors: [{ environment: "local", message: "unset var" }],
      });
      expect(withErrors!.problem).toContain("loadErrors names which");
      expect(withErrors!.problem).not.toContain("before any environment");
      expect(withErrors!.nextAction).toContain("does not need editing");
   });

   it("returns nothing, and does not throw, when there is no server root", async () => {
      // Distinct from an unexpected fault: the caller logs a warning for the
      // second and should not for the first.
      expect(
         await describeWorkspaceSetup(undefined as never, serving()),
      ).toBeUndefined();
   });

   it("reports a config that cannot be read as a READ failure, not a syntax error", async () => {
      // A directory where the config file is expected fails readFileSync on
      // every platform, so this covers the branch with no capability probe.
      // The realistic case is EACCES, covered below where it can be built.
      fs.mkdirSync(path.join(root, "publisher.config.json"));
      const setup = await describeWorkspaceSetup(root, serving());
      expect(setup!.problem).toContain("could not be READ");
      expect(setup!.problem).not.toContain("JSON");
      expect(setup!.nextAction).toContain("permissions");
      expect(setup!.nextAction).toContain("Do not edit the JSON");
   });

   itIfPerms(
      "reports an unreadable config as a READ failure, not a syntax error",
      async () => {
         // The container mistake: config baked in root-owned, server running as a
         // non-root uid. The JSON is perfectly valid and "fix the JSON" sends the
         // operator to edit a correct file.
         const configPath = path.join(root, "publisher.config.json");
         writeConfig({ environments: [] });
         fs.chmodSync(configPath, 0o000);
         try {
            const setup = await describeWorkspaceSetup(root, serving());
            expect(setup!.problem).toContain("could not be READ");
            expect(setup!.problem).not.toContain("JSON");
            expect(setup!.nextAction).toContain("permissions");
            expect(setup!.nextAction).toContain("Do not edit the JSON");
         } finally {
            fs.chmodSync(configPath, 0o600);
         }
      },
   );

   it("names the shape when config entries are not objects", async () => {
      // A packages list of bare strings, or environments as strings or an
      // object map, all previously reported "declares no packages" with "add a
      // package to an environment", which is a dead end when there is no
      // environment object to add one to.
      writeConfig({ environments: ["local", "other"] });
      const setup = await describeWorkspaceSetup(root, serving());
      expect(setup!.problem).toContain("not objects");
      expect(setup!.problem).not.toContain("declares no packages");
      expect(setup!.nextAction).toContain("ARRAY OF OBJECTS");
   });

   it("counts a skipped package entry so the ratio does not lie", async () => {
      // One bare string plus one object with an unusable location is a file
      // declaring TWO packages; reporting 1 of 1 understates it.
      writeConfig({
         environments: [
            {
               name: "local",
               packages: ["sales", { name: "p", locaton: "./x" }],
            },
         ],
      });
      const setup = await describeWorkspaceSetup(
         root,
         serving([{ name: "local", packages: [] }]),
      );
      expect(setup!.problem).toContain("2");
   });

   it("treats a notebook as a model when listing unclaimed files", async () => {
      // Publisher serves notebooks and the bundled skills tell an agent to
      // author them, so a workspace whose only work is a .malloynb should get
      // the specific branch and the file list, not the generic sentence.
      fs.writeFileSync(path.join(root, "analysis.malloynb"), "");
      const setup = await describeWorkspaceSetup(root, serving());
      expect(setup!.unclaimedModelFiles).toEqual(["analysis.malloynb"]);
      expect(setup!.problem).toContain("not part of any package");
   });
});
describe("jsonErrorPosition", () => {
   // The leak only occurs under Node: V8 builds "Unexpected token" from a
   // window of the RAW SOURCE, and publisher.config.json is where a password
   // or private key is written literally. JavaScriptCore does not, so running
   // these tests under Bun can never reproduce it, and the published bin runs
   // under Node. So the real Node message is pinned here as a literal.
   it("keeps no source text from a V8 message that embeds a secret", () => {
      const nodeMessage =
         'Unexpected token \']\', ..."ret!" },\n ]\n}" is not valid JSON';
      const out = jsonErrorPosition(new Error(nodeMessage));
      expect(out).not.toContain("ret!");
      expect(out).not.toContain("Unexpected token");
      expect(out).toBe("");
   });

   it("keeps a position when V8 gives one, since that carries no content", () => {
      const out = jsonErrorPosition(
         new Error(
            "Expected property name or '}' in JSON at position 21 (line 1 column 22)",
         ),
      );
      expect(out).toBe(" (at position 21 (line 1 column 22))");
   });

   it("returns nothing for a non-Error", () => {
      expect(jsonErrorPosition("boom")).toBe("");
   });
});
