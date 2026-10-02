// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, test } from "@playwright/test";
import fs from "fs";
import os from "os";
import path from "path";
import { fileURLToPath } from "url";
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
   baseURL: string;
   /** A model file as the server serves it, read through the API. */
   readSource: (modelPath: string) => Promise<string>;
   dispose: () => Promise<void>;
}

/**
 * Registers a throwaway environment over a COPY of a package, because the
 * builders write into the package and a fixture in the repository is not the
 * place for that. The server then serves its own copy of that copy, so a file
 * is read back through the API rather than from disk. Skips the suite on a server that refuses writes.
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
   const env = tmpName(prefix);
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
   return {
      env,
      pkg,
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
         fs.rmSync(location, { recursive: true, force: true });
      },
   };
}
