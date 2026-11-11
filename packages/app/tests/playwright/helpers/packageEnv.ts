// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, test } from "@playwright/test";
import fs from "fs";
import os from "os";
import path from "path";
import { fileURLToPath } from "url";
import { AUTHORING_ENVS } from "./authoringEnvs";
import { tmpName } from "./fixtures";

const here = path.dirname(fileURLToPath(import.meta.url));

/** A package directory the server tests keep under `packages/server/tests/fixtures`. */
export const serverFixture = (name: string) =>
   path.resolve(here, "../../../../server/tests/fixtures", name);

/** The repository's bundled example packages. */
export const exampleFixture = (name: string) =>
   path.resolve(here, "../../../../../examples", name);

export interface PackageEnv {
   env: string;
   pkg: string;
   /** The version a published copy serves as; absent for one mounted in place. */
   versionId?: string;
   /**
    * The package's Console route, without the leading slash:
    * `<env>/<pkg>/<versionId>` for a published version, `<env>/<pkg>` for a
    * package mounted in place.
    */
   path: string;
   baseURL: string;
   /** A model file as the server serves it, read through the API. */
   readSource: (modelPath: string) => Promise<string>;
   dispose: () => Promise<void>;
}

/** A throwaway copy of `source`, with `remove` taken out and `files` written in. */
function copyFixture(
   prefix: string,
   source: string,
   files: Record<string, string>,
   remove: string[],
): string {
   const location = fs.mkdtempSync(
      path.join(os.tmpdir(), `publisher-${prefix}-`),
   );
   fs.cpSync(source, location, { recursive: true });
   for (const relative of remove)
      fs.rmSync(path.join(location, relative), {
         recursive: true,
         force: true,
      });
   for (const [relative, text] of Object.entries(files)) {
      const target = path.join(location, relative);
      fs.mkdirSync(path.dirname(target), { recursive: true });
      fs.writeFileSync(target, text);
   }
   return location;
}

/** The authoring environments a spec holds now (see AUTHORING_ENVS). */
const held = new Set<string>();

/**
 * Take a free authoring environment, removing whatever a run that did not
 * finish left under its name.
 */
async function claimAuthoringEnv(baseURL: string): Promise<string> {
   const env = AUTHORING_ENVS.find((name) => !held.has(name));
   if (!env) {
      throw new Error(
         `Every authoring environment is held (${AUTHORING_ENVS.join(", ")}); dispose one first, or add a name to AUTHORING_ENVS.`,
      );
   }
   held.add(env);
   await fetch(`${baseURL}/api/v0/environments/${env}`, {
      method: "DELETE",
   }).catch(() => undefined);
   return env;
}

/**
 * Registers a throwaway environment over a COPY of a package, because the
 * builders write into the package and a fixture in the repository is not the
 * place for that. The environment is one the server watches (AUTHORING_ENVS),
 * so it mounts the copy in place: every other package is a published version,
 * which no write or reload changes. A file is read back through the API
 * rather than from disk. Skips the suite on a server that refuses writes. A
 * spec that only reads a package publishes it instead (publishPackageEnv).
 */
export async function registerPackageEnv(
   baseURL: string,
   prefix: string,
   source: string,
   pkg: string,
   /** Files added to (or replacing ones in) the copy before the server loads it. */
   files: Record<string, string> = {},
   /** Paths removed from the copy first, to start a package with a section empty. */
   remove: string[] = [],
): Promise<PackageEnv> {
   const env = await claimAuthoringEnv(baseURL);
   const location = copyFixture(prefix, source, files, remove);
   const res = await fetch(`${baseURL}/api/v0/environments`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
         name: env,
         packages: [{ name: pkg, location }],
         connections: [],
      }),
   });
   test.skip(
      res.status === 405 || res.status === 403,
      "publisher is read-only",
   );
   expect(res.ok, await res.text()).toBe(true);
   const pkgUrl = `${baseURL}/api/v0/environments/${env}/packages/${pkg}`;
   // Mounted in place, the package has no versions. A version here means the
   // server was not started watching the authoring environments.
   const described = (await (await fetch(pkgUrl)).json()) as {
      versionId?: string;
   };
   expect(
      described.versionId,
      `start the server with PUBLISHER_WATCH=${AUTHORING_ENVS.join(",")}`,
   ).toBeUndefined();
   return {
      env,
      pkg,
      path: `${env}/${pkg}`,
      baseURL,
      readSource: async (modelPath) => {
         const r = await fetch(
            `${pkgUrl}/models/${encodeURIComponent(modelPath)}`,
         );
         const body = await r.text();
         expect(r.ok, body).toBe(true);
         return (JSON.parse(body) as { sourceText: string }).sourceText;
      },
      dispose: async () => {
         await fetch(`${baseURL}/api/v0/environments/${env}`, {
            method: "DELETE",
         }).catch(() => undefined);
         held.delete(env);
         fs.rmSync(location, { recursive: true, force: true });
      },
   };
}

/**
 * Publishes a throwaway copy of `source` as version `version` (written into the
 * copy's publisher.json) in an environment of its own, for a spec that reads a
 * package and writes nothing into it. The spec browses the version's own
 * routes, `/<env>/<pkg>/<versionId>/…`, as a published package is served. A
 * spec that writes into the package (the builders, a report save) needs
 * registerPackageEnv instead: a published version is immutable. Skips the
 * suite on a server that refuses writes.
 */
export async function publishPackageEnv(
   baseURL: string,
   prefix: string,
   source: string,
   pkg: string,
   /** Files added to (or replacing ones in) the copy before it is published. */
   files: Record<string, string> = {},
   /** Paths removed from the copy first, to start a package with a section empty. */
   remove: string[] = [],
   version = "1.0.0",
): Promise<PackageEnv> {
   const env = tmpName(prefix);
   const location = copyFixture(prefix, source, files, remove);
   const manifestPath = path.join(location, "publisher.json");
   const manifest = fs.existsSync(manifestPath)
      ? (JSON.parse(fs.readFileSync(manifestPath, "utf8")) as Record<
           string,
           unknown
        >)
      : {};
   fs.writeFileSync(
      manifestPath,
      JSON.stringify({ ...manifest, name: pkg, version }, null, 2),
   );
   const api = `${baseURL}/api/v0/environments`;
   const created = await fetch(api, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: env, packages: [], connections: [] }),
   });
   test.skip(
      created.status === 405 || created.status === 403,
      "publisher is read-only",
   );
   expect(created.ok, await created.text()).toBe(true);
   const published = await fetch(`${api}/${env}/packages`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: pkg, location }),
   });
   const body = await published.text();
   expect(published.ok, body).toBe(true);
   expect((JSON.parse(body) as { versionId?: string }).versionId).toBe(version);
   const pkgUrl = `${api}/${env}/packages/${pkg}`;
   return {
      env,
      pkg,
      versionId: version,
      path: `${env}/${pkg}/${version}`,
      baseURL,
      readSource: async (modelPath) => {
         const r = await fetch(
            `${pkgUrl}/models/${encodeURIComponent(modelPath)}?versionId=${encodeURIComponent(version)}`,
         );
         const text = await r.text();
         expect(r.ok, text).toBe(true);
         return (JSON.parse(text) as { sourceText: string }).sourceText;
      },
      dispose: async () => {
         await fetch(`${api}/${env}`, { method: "DELETE" }).catch(
            () => undefined,
         );
         fs.rmSync(location, { recursive: true, force: true });
      },
   };
}
