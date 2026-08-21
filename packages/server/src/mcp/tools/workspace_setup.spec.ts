import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs";
import os from "os";
import path from "path";
import { describeWorkspaceSetup } from "./workspace_setup";

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
      writeConfig('{ "environments": [ ], }');
      const setup = await describeWorkspaceSetup(root, serving());
      expect(setup!.problem).toContain("could not be read as JSON");
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
         expect(pkg.sourcePath).toBe(
            path.resolve(root, "environments/prod/models"),
         );
         expect(pkg.sourcePathKind).toBe("directory");
         expect(setup!.problem).not.toContain("does not exist on disk");
         // The user edits the declared text, so that is what is echoed back.
         expect(pkg.location).toBe("./environments/${WSETUP_TEST_ENV}/models");
      } finally {
         delete process.env.WSETUP_TEST_ENV;
      }
   });

   it("reports an unset variable as rejecting the whole file, not one package", async () => {
      // Verified live: substituteEnvVars throws, reloadEnvironmentManifest
      // catches it and returns zero environments, so NOTHING is attempted and
      // there are no loadErrors. The old wording said "see loadErrors for why
      // each one failed" beside a package it listed as perfectly healthy.
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
      const setup = await describeWorkspaceSetup(root, serving());
      expect(setup!.problem).toContain("WSETUP_UNSET_VAR");
      expect(setup!.problem).toContain("ENTIRE file");
      expect(setup!.problem).not.toContain("see loadErrors");
      // The healthy package must not be offered as the thing to go fix.
      expect(setup!.nextAction).toContain(
         "do not start by editing the packages",
      );
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

   it("interpolates every config string the loader interpolates, not just location", async () => {
      // processConfigValue substitutes EVERY string, so an environment,
      // connection or package name echoed back raw would not match the one in
      // loadErrors and environments, and the agent cannot join them up.
      process.env.WSETUP_ENV_NAME = "prod";
      process.env.WSETUP_CONN_NAME = "warehouse";
      process.env.WSETUP_PKG_NAME = "sales";
      try {
         writeConfig({
            environments: [
               {
                  name: "${WSETUP_ENV_NAME}",
                  connections: [{ name: "${WSETUP_CONN_NAME}" }],
                  packages: [
                     { name: "${WSETUP_PKG_NAME}", location: "./nope" },
                  ],
               },
            ],
         });
         const setup = await describeWorkspaceSetup(
            root,
            serving([{ name: "prod", packages: [] }]),
         );
         const pkg = setup!.unservedPackages![0];
         expect(pkg.environment).toBe("prod");
         expect(pkg.package).toBe("sales");
         expect(setup!.configuredConnections).toEqual(["warehouse"]);
         expect(JSON.stringify(setup)).not.toContain("WSETUP_ENV_NAME");
      } finally {
         delete process.env.WSETUP_ENV_NAME;
         delete process.env.WSETUP_CONN_NAME;
         delete process.env.WSETUP_PKG_NAME;
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
});
