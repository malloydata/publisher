// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The shared setup of the report integration specs: one environment of
 * fixture packages on the real REST app, and the few requests every one of
 * them makes. A spec that writes to its package asks for `copy`, so the
 * fixture in the repository is never the thing written to.
 */

import { createHash } from "crypto";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { startRestE2E } from "./rest_e2e";

const FIXTURES = path.resolve(__dirname, "../fixtures");

/** A fixture package's directory under `tests/fixtures`. */
export const fixture = (name: string): string => path.join(FIXTURES, name);

/** The bundled `examples/storefront` package. */
export const STOREFRONT = path.resolve(
   __dirname,
   "../../../../examples/storefront",
);

/** SHA-256 of a file's text, hex: what a write sends back as `expectedHash`. */
export const hashOf = (text: string): string =>
   createHash("sha256").update(text, "utf8").digest("hex");

/** Drop every `\r`: a Windows checkout gives fixture text CRLF line endings. */
export const lf = (text: string | undefined): string | undefined =>
   text?.replace(/\r/g, "");

/** A package warning as `GET …/packages/{pkg}` carries it. */
export interface Warning {
   model?: string | null;
   subject?: string;
   message?: string;
   severity?: string | null;
   code?: string;
}

/** The warnings that carry `code`, in the order the package reported them. */
export const warningsByCode = (warnings: Warning[], code: string): Warning[] =>
   warnings.filter((w) => w.code === code);

export interface PackageEnv {
   baseUrl: string;
   /** A URL under a package: `api("/reports")`. `pkg` defaults to the first package. */
   api(sub?: string, pkg?: string): string;
   /** GET a URL, expect `status`, and return the parsed body. */
   getJson<T>(url: string, status?: number): Promise<T>;
   /** POST a JSON body. */
   post(url: string, body: unknown): Promise<Response>;
   /** POST a query to `…/models/{modelPath}/query`. */
   query(modelPath: string, body: unknown, pkg?: string): Promise<Response>;
   /** PUT `…/models/{modelPath}`: the report write path. */
   put(modelPath: string, body: unknown, pkg?: string): Promise<Response>;
   /** The package's load warnings. */
   warnings(pkg?: string): Promise<Warning[]>;
   /** The directory a package was registered from: the copy, when copied. */
   location(pkg?: string): string;
   /** The package's own copy of a file, under `publisher_data`: where writes land. */
   served(modelPath: string, pkg?: string): string;
   /** Delete the environment, stop the server and remove any copies. */
   stop(): Promise<void>;
}

export interface PackageEnvOptions {
   /** Register a temporary copy of each fixture, for a spec that writes to it. */
   copy?: boolean;
   /** Runs on each copy before the environment is created. Implies `copy`. */
   prepare?: (dir: string, pkg: string) => Promise<void>;
   /** Runs once the server is up, before the environment is created: where a meter provider is swapped in. */
   beforeCreate?: () => Promise<void>;
}

const json = (method: string, body: unknown): RequestInit => ({
   method,
   headers: { "Content-Type": "application/json" },
   body: JSON.stringify(body),
});

/**
 * Start the REST app and register `packages` (name → directory) as the
 * environment `envName`, waiting until each package answers.
 */
export async function startPackageEnv(
   envName: string,
   packages: Record<string, string>,
   options: PackageEnvOptions = {},
): Promise<PackageEnv> {
   const names = Object.keys(packages);
   const first = names[0];
   const copyRoot =
      options.copy || options.prepare
         ? await fs.mkdtemp(path.join(os.tmpdir(), `publisher-${envName}-`))
         : undefined;
   const locations: Record<string, string> = {};
   for (const name of names) {
      if (!copyRoot) {
         locations[name] = packages[name];
         continue;
      }
      locations[name] = path.join(copyRoot, name);
      await fs.cp(packages[name], locations[name], { recursive: true });
      await options.prepare?.(locations[name], name);
   }

   const server = await startRestE2E();
   const { baseUrl } = server;
   const api = (sub = "", pkg = first) =>
      `${baseUrl}/api/v0/environments/${envName}/packages/${pkg}${sub}`;

   await options.beforeCreate?.();
   const created = await fetch(
      `${baseUrl}/api/v0/environments`,
      json("POST", {
         name: envName,
         packages: names.map((name) => ({ name, location: locations[name] })),
         connections: [],
      }),
   );
   if (!created.ok) {
      await server.stop();
      throw new Error(
         `Failed to create test environment (${created.status}): ${await created.text()}`,
      );
   }
   const deadline = Date.now() + 30_000;
   for (const name of names) {
      while (Date.now() < deadline) {
         const res = await fetch(api("", name)).catch(() => undefined);
         if (res?.ok) break;
         await new Promise((r) => setTimeout(r, 500));
      }
   }

   const getJson = async <T>(url: string, status = 200): Promise<T> => {
      const res = await fetch(url);
      if (res.status !== status) {
         throw new Error(
            `GET ${url} answered ${res.status}, not ${status}: ${await res.text()}`,
         );
      }
      return (await res.json()) as T;
   };

   return {
      baseUrl,
      api,
      getJson,
      post: (url, body) => fetch(url, json("POST", body)),
      query: (modelPath, body, pkg) =>
         fetch(api(`/models/${modelPath}/query`, pkg), json("POST", body)),
      put: (modelPath, body, pkg) =>
         fetch(api(`/models/${modelPath}`, pkg), json("PUT", body)),
      warnings: async (pkg) =>
         (await getJson<{ warnings?: Warning[] }>(api("", pkg))).warnings ?? [],
      location: (pkg = first) => locations[pkg],
      served: (modelPath, pkg = first) =>
         path.resolve(
            __dirname,
            "../../publisher_data",
            envName,
            pkg,
            modelPath,
         ),
      stop: async () => {
         await fetch(`${baseUrl}/api/v0/environments/${envName}`, {
            method: "DELETE",
         }).catch(() => undefined);
         await server.stop();
         if (copyRoot) await fs.rm(copyRoot, { recursive: true, force: true });
      },
   };
}
