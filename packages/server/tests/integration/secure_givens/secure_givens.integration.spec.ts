// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A package's `secureGivens` rides the responses a host reads after a load:
 * the publish (POST), a get, a reload, and a versioned publish. Over HTTP
 * against the real Express app and the real worker-pool load, because the
 * contract is that the load's own compile answers it, on the response body.
 * It follows the models it describes through a reload in place and a
 * reinstall, is absent while any file fails to compile, and a PATCH body
 * cannot set it.
 */

import { afterAll, afterEach, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import type { Package } from "../../../src/service/package";
import { installVariables } from "../../../src/test_helpers/settings_env";
import { startRestE2E } from "../../harness/rest_e2e";

const ENV_NAME = "secure-givens-env";
const FLAG = "PUBLISHER_PACKAGE_VERSIONING";
/** The variables this suite sets, installed over the process's own settings. */
const variables: Record<string, string | undefined> = {};
let restoreSettings: (() => void) | undefined;

/** Give the in-process server `name` at `value`, or unset, as a restart with it would. */
function setVariable(name: string, value: string | undefined): void {
   variables[name] = value;
   restoreSettings?.();
   restoreSettings = installVariables(variables);
}

const ROWS = `duckdb.sql("select * from (values (1, 'a'), (2, 'b')) as t(id, role)")`;

/** Declares the secure given; `orders.malloy` imports it. */
const SHARED = `##! experimental.givens
#(secure)
given: ROLES :: string[]

given: REGION :: string is "us"

source: base is ${ROWS} extend { measure: c is count() }
`;

const ORDERS = `##! experimental.givens
import "shared.malloy"

source: orders is base extend { dimension: tagged is concat(role, $REGION) }
`;

/** A notebook whose cells declare a secure given no model declares. */
const NOTEBOOK_ONLY = `>>>malloy
##! experimental.givens
#(secure)
given: NB_TENANT :: string[]

source: nb_rows is ${ROWS} extend { measure: c is count() }
`;

/** {@link SHARED} with a second secure given. */
const SHARED_WITH_TENANT = SHARED.replace(
   "given: REGION",
   "#(secure)\ngiven: TENANT :: string[]\n\ngiven: REGION",
);

/** {@link SHARED} with ROLES no longer `#(secure)`. */
const SHARED_WITHOUT_SECURE = SHARED.replace("#(secure)\n", "");

/** No givens at all, secure or otherwise. */
const PLAIN = `source: plain is ${ROWS} extend { measure: c is count() }
`;

type SecureGiven = { name: string; type: string; modelPaths: string[] };

describe("secureGivens on the package responses", () => {
   let env: Awaited<ReturnType<typeof startRestE2E>> | undefined;
   let baseUrl = "";
   let tempDir = "";

   function writePackage(
      name: string,
      files: Record<string, string>,
      manifest: Record<string, unknown> = {},
   ): string {
      const dir = path.join(tempDir, name);
      fs.mkdirSync(dir, { recursive: true });
      fs.writeFileSync(
         path.join(dir, "publisher.json"),
         JSON.stringify({ name, ...manifest }),
      );
      for (const [file, text] of Object.entries(files)) {
         fs.mkdirSync(path.dirname(path.join(dir, file)), { recursive: true });
         fs.writeFileSync(path.join(dir, file), text);
      }
      return dir;
   }

   async function publish(
      name: string,
      location: string,
   ): Promise<{ status: number; json: Record<string, unknown> }> {
      const res = await fetch(
         `${baseUrl}/api/v0/environments/${ENV_NAME}/packages`,
         {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ name, location }),
         },
      );
      return {
         status: res.status,
         json: (await res.json()) as Record<string, unknown>,
      };
   }

   async function getPackage(name: string): Promise<Package> {
      const { environmentStore } = await import("../../../src/main_server");
      const environment = await environmentStore.getEnvironment(
         ENV_NAME,
         false,
      );
      return environment.getPackage(name, false);
   }

   async function get(
      name: string,
      query = "",
   ): Promise<Record<string, unknown>> {
      const res = await fetch(
         `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${name}${query}`,
      );
      expect(res.status).toBe(200);
      return (await res.json()) as Record<string, unknown>;
   }

   beforeAll(async () => {
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "secure-givens-"));
      env = await startRestE2E();
      baseUrl = env.baseUrl;
      const created = await fetch(`${baseUrl}/api/v0/environments`, {
         method: "POST",
         headers: { "content-type": "application/json" },
         body: JSON.stringify({
            name: ENV_NAME,
            packages: [],
            connections: [],
         }),
      });
      expect(created.status).toBeLessThan(300);
   }, 180_000);

   afterEach(() => {
      setVariable(FLAG, undefined);
   });

   afterAll(async () => {
      restoreSettings?.();
      await fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}`, {
         method: "DELETE",
      }).catch(() => undefined);
      await env?.stop();
      fs.rmSync(tempDir, { recursive: true, force: true });
   });

   const EXPECTED: SecureGiven[] = [
      // Declared only by a notebook's cells, and still the host's to set.
      {
         name: "NB_TENANT",
         type: "string[]",
         modelPaths: ["nb_only.malloynb"],
      },
      {
         name: "ROLES",
         type: "string[]",
         // The importer surfaces the given too, and so does a notebook.
         modelPaths: ["nb.malloynb", "orders.malloy", "shared.malloy"],
      },
   ];

   const SECURE_FILES = {
      "shared.malloy": SHARED,
      "orders.malloy": ORDERS,
      "nb.malloynb": `>>>malloy\n${SHARED}`,
      "nb_only.malloynb": NOTEBOOK_ONLY,
   };

   const ROLES_ONLY: SecureGiven[] = [
      {
         name: "ROLES",
         type: "string[]",
         modelPaths: ["orders.malloy", "shared.malloy"],
      },
   ];

   it("the publish, get and reload responses carry the summary", async () => {
      // `shared.malloy` is off the surface and still counts.
      const location = writePackage("secure", SECURE_FILES, {
         explores: ["orders.malloy"],
      });
      const created = await publish("secure", location);
      expect(created.status).toBe(200);
      expect(created.json.secureGivens as SecureGiven[]).toEqual(EXPECTED);

      expect((await get("secure")).secureGivens as SecureGiven[]).toEqual(
         EXPECTED,
      );
      expect(
         (await get("secure", "?reload=true")).secureGivens as SecureGiven[],
      ).toEqual(EXPECTED);
   }, 120_000);

   it("a package with no secure given carries an empty array", async () => {
      const location = writePackage("plain", { "plain.malloy": PLAIN });
      const created = await publish("plain", location);
      expect(created.status).toBe(200);
      expect(created.json.secureGivens).toEqual([]);
      expect((await get("plain")).secureGivens).toEqual([]);
   }, 120_000);

   it("a versioned publish carries it too", async () => {
      setVariable(FLAG, "on");
      const location = writePackage("versioned", SECURE_FILES, {
         version: "1.0.0",
      });
      const created = await publish("versioned", location);
      expect(created.status).toBeLessThan(300);
      expect(created.json.versionId).toBe("1.0.0");
      expect(created.json.secureGivens as SecureGiven[]).toEqual(EXPECTED);
      expect((await get("versioned")).secureGivens as SecureGiven[]).toEqual(
         EXPECTED,
      );
   }, 120_000);
   it("follows the models through a reload in place and a reinstall", async () => {
      const location = writePackage("editable", {
         "shared.malloy": SHARED,
         "orders.malloy": ORDERS,
      });
      const created = await publish("editable", location);
      expect(created.status).toBe(200);
      expect(created.json.secureGivens as SecureGiven[]).toEqual(ROLES_ONLY);

      // In place: the same Package recompiles the tree it serves, as a
      // materialization refresh or a manifest rebind does, and the summary
      // must follow the models it installs rather than the ones it replaced.
      const pkg = await getPackage("editable");
      const installed = pkg.getPackagePath();
      fs.writeFileSync(
         path.join(installed, "shared.malloy"),
         SHARED_WITH_TENANT,
      );
      await pkg.reloadAllModels({});
      const withTenant: SecureGiven[] = [
         ...ROLES_ONLY,
         {
            name: "TENANT",
            type: "string[]",
            modelPaths: ["orders.malloy", "shared.malloy"],
         },
      ];
      expect((await get("editable")).secureGivens as SecureGiven[]).toEqual(
         withTenant,
      );

      // A reinstall from the location: ROLES is no longer secure.
      fs.writeFileSync(
         path.join(location, "shared.malloy"),
         SHARED_WITHOUT_SECURE,
      );
      expect(
         (await get("editable", "?reload=true")).secureGivens as SecureGiven[],
      ).toEqual([]);
      expect((await get("editable")).secureGivens).toEqual([]);
   }, 120_000);

   it("is absent while any model fails to compile, and back once it does", async () => {
      const location = writePackage("partial", {
         "shared.malloy": SHARED,
         "orders.malloy": ORDERS,
      });
      expect((await publish("partial", location)).status).toBe(200);
      const pkg = await getPackage("partial");
      const orders = path.join(pkg.getPackagePath(), "orders.malloy");

      // A reload in place keeps a placeholder for the broken file, so the
      // package serves with one model's givens unknown.
      fs.writeFileSync(orders, `${ORDERS}\nsource: oops is base extend {`);
      await pkg.reloadAllModels({});
      const partial = await get("partial");
      expect(partial).not.toHaveProperty("secureGivens");

      fs.writeFileSync(orders, ORDERS);
      await pkg.reloadAllModels({});
      expect((await get("partial")).secureGivens as SecureGiven[]).toEqual(
         ROLES_ONLY,
      );
   }, 120_000);

   it("a PATCH body cannot set it", async () => {
      const location = writePackage("patched", {
         "shared.malloy": SHARED,
         "orders.malloy": ORDERS,
      });
      expect((await publish("patched", location)).status).toBe(200);
      const res = await fetch(
         `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/patched`,
         {
            method: "PATCH",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
               name: "patched",
               description: "edited",
               secureGivens: [
                  { name: "FORGED", type: "string", modelPaths: [] },
               ],
            }),
         },
      );
      expect(res.status).toBe(200);
      const patched = (await res.json()) as Record<string, unknown>;
      expect(patched.description).toBe("edited");
      expect(patched.secureGivens as SecureGiven[]).toEqual(ROLES_ONLY);
      expect((await get("patched")).secureGivens as SecureGiven[]).toEqual(
         ROLES_ONLY,
      );
   }, 120_000);
});
