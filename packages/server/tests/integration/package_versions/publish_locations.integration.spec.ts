// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * Publishing over HTTP from every kind of location a publish is sent: a local
 * folder or `.zip`, a `gs://` or `s3://` `.zip` (how those stores hold
 * packages), and a Git repository or a top-level folder of one. The packages
 * are the fixtures in tests/fixtures/publish-locations.
 *
 * For each kind, the same two rules:
 * - a tree with a version: published (200); the same content again, in any
 *   form, is placement (200, nothing written); other content under that
 *   version is 409 VERSION_CONFLICT, and the first content keeps serving;
 * - a tree with no version: 200 every time, each publish replacing the
 *   package in place, as every publish from a location did before versions.
 *
 * Nothing leaves the process. The server's GCS and S3 clients are swapped
 * for in-memory buckets holding the fixtures, and the clone of a
 * `https://github.com/publisher-fixtures/...` URL copies a fixture; every
 * other URL still reaches the real clone. The real downloaders, unzip, hash,
 * placement and install run as in production.
 */

import { afterAll, beforeAll, describe, expect, it, mock } from "bun:test";
import fs from "fs";
import os from "os";
import path from "path";
import { simpleGit } from "simple-git";

const FIXTURES = path.resolve(__dirname, "../../fixtures/publish-locations");
const FIXTURE_FOLDERS = [
   "sales-1.0.0",
   "sales-1.0.0-changed",
   "sales-unversioned",
   "sales-unversioned-changed",
];
const GIT_FIXTURES = "https://github.com/publisher-fixtures/";
const ENV_NAME = "publish-locations-env";

/** Copy a fixture folder's files into `target`. */
function copyFixture(folder: string, target: string): void {
   fs.cpSync(path.join(FIXTURES, folder), target, { recursive: true });
}

// The real clone, for any repository that is not a fixture.
const realSimpleGit = simpleGit;
let clones = 0;
mock.module("simple-git", () => ({
   simpleGit: (options?: Parameters<typeof simpleGit>[0]) => {
      const real = realSimpleGit(options);
      return {
         clone: (
            repoUrl: string,
            dir: string,
            cloneOptions: string[],
            done: (err: Error | null) => void,
         ) => {
            if (!repoUrl.startsWith(GIT_FIXTURES)) {
               return real.clone(repoUrl, dir, cloneOptions, (err) =>
                  done(err ?? null),
               );
            }
            clones++;
            // `all` holds every fixture folder at its root; any other
            // repository is one fixture.
            const repo = repoUrl.slice(GIT_FIXTURES.length);
            if (repo === "all") {
               for (const folder of FIXTURE_FOLDERS) {
                  copyFixture(folder, path.join(dir, folder));
               }
            } else {
               copyFixture(repo, dir);
            }
            // Repository state, different on every clone, as a fresh clone's.
            fs.mkdirSync(path.join(dir, ".git", "objects", "pack"), {
               recursive: true,
            });
            fs.writeFileSync(
               path.join(dir, ".git", "HEAD"),
               "ref: refs/heads/main",
            );
            fs.writeFileSync(
               path.join(dir, ".git", "objects", "pack", `pack-${clones}.pack`),
               `clone ${clones}`,
            );
            done(null);
         },
      };
   },
}));

const { startRestE2E } = await import("../../harness/rest_e2e");
const { environmentStore } = await import("../../../src/server");

/** The in-memory buckets' objects, by "bucket/key". */
const objects = new Map<string, Buffer>();

function keysUnder(bucket: string, prefix: string): string[] {
   return [...objects.keys()]
      .filter((k) => k.startsWith(`${bucket}/${prefix}`))
      .map((k) => k.slice(bucket.length + 1));
}

/** A GCS client over the buckets, shaped as the store calls it. */
const fakeGcs = {
   bucket: (bucket: string) => ({
      getFiles: async ({ prefix }: { prefix: string }) => [
         keysUnder(bucket, prefix).map((name) => ({
            name,
            download: async () => [objects.get(`${bucket}/${name}`)!],
         })),
      ],
   }),
};

/** An S3 client over the buckets, shaped as the store calls it. */
const fakeS3 = {
   send: async (command: { input: { Bucket: string; Key: string } }) => {
      const body = objects.get(`${command.input.Bucket}/${command.input.Key}`);
      return body
         ? {
              Body: {
                 transformToWebStream: () =>
                    new Response(new Uint8Array(body)).body!,
              },
           }
         : {};
   },
};

/** Every fixture zip, under `zips/` in the `fixtures` bucket. */
function fillBuckets(): void {
   for (const zip of fs.readdirSync(path.join(FIXTURES, "zips"))) {
      objects.set(
         `fixtures/zips/${zip}`,
         fs.readFileSync(path.join(FIXTURES, "zips", zip)),
      );
   }
}

interface LocationKind {
   /** Names the packages this kind publishes, so kinds never share one. */
   slug: string;
   location: (fixture: string) => string;
   /** Whether the kind has the same version packed again (a zip). */
   repacks?: boolean;
}

const KINDS: LocationKind[] = [
   { slug: "local-folder", location: (f) => path.join(FIXTURES, f) },
   {
      slug: "local-zip",
      location: (f) => path.join(FIXTURES, "zips", `${f}.zip`),
      repacks: true,
   },
   {
      slug: "gcs-zip",
      location: (f) => `gs://fixtures/zips/${f}.zip`,
      repacks: true,
   },
   {
      slug: "s3-zip",
      location: (f) => `s3://fixtures/zips/${f}.zip`,
      repacks: true,
   },
   { slug: "git-repo", location: (f) => `${GIT_FIXTURES}${f}` },
   {
      slug: "git-folder",
      location: (f) => `${GIT_FIXTURES}all/tree/main/${f}`,
   },
];

let env: Awaited<ReturnType<typeof startRestE2E>> | undefined;
let baseUrl = "";
let envPath = "";
let savedClients: Record<string, unknown> = {};

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

async function answerOf(name: string): Promise<number> {
   const res = await fetch(
      `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${name}/models/model.malloy/query`,
      {
         method: "POST",
         headers: { "content-type": "application/json" },
         body: JSON.stringify({
            query: "run: numbers -> which",
            compactJson: true,
         }),
      },
   );
   expect(res.status).toBe(200);
   const json = (await res.json()) as { result: string };
   return Number((JSON.parse(json.result) as { answer: number }[])[0].answer);
}

describe("publishing from each kind of location, over HTTP", () => {
   beforeAll(async () => {
      fillBuckets();
      const store = environmentStore as unknown as Record<string, unknown>;
      savedClients = { gcsClient: store.gcsClient, s3Client: store.s3Client };
      Object.assign(store, { gcsClient: fakeGcs, s3Client: fakeS3 });

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
      envPath = String(
         ((await created.json()) as { location: string }).location,
      );
   }, 180_000);

   afterAll(async () => {
      await fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}`, {
         method: "DELETE",
      }).catch(() => undefined);
      Object.assign(
         environmentStore as unknown as Record<string, unknown>,
         savedClients,
      );
      await env?.stop();
   });

   for (const kind of KINDS) {
      describe(kind.slug, () => {
         it("a version: published, the same content again is 200, other content under it is 409", async () => {
            const name = `${kind.slug}-versioned`;
            const first = await publish(name, kind.location("sales-1.0.0"));
            expect([first.status, first.json.versionId]).toEqual([
               200,
               "1.0.0",
            ]);
            expect(await answerOf(name)).toBe(1);
            const placed = path.join(envPath, name, "1.0.0", "model.malloy");
            const before = fs.statSync(placed);

            const again = [kind.location("sales-1.0.0")];
            if (kind.repacks) again.push(kind.location("sales-1.0.0-repacked"));
            for (const location of again) {
               const placement = await publish(name, location);
               expect([location, placement.status]).toEqual([location, 200]);
               const after = fs.statSync(placed);
               expect([after.ino, after.mtimeMs]).toEqual([
                  before.ino,
                  before.mtimeMs,
               ]);
            }

            const changed = await publish(
               name,
               kind.location("sales-1.0.0-changed"),
            );
            expect([changed.status, changed.json.reason]).toEqual([
               409,
               "VERSION_CONFLICT",
            ]);
            expect(await answerOf(name)).toBe(1);
         });

         it("no version: 200 every time, each publish replacing the package in place", async () => {
            const name = `${kind.slug}-unversioned`;
            const first = await publish(
               name,
               kind.location("sales-unversioned"),
            );
            expect(first.status).toBe(200);
            expect(first.json.versionId).toBeUndefined();
            expect(await answerOf(name)).toBe(3);
            expect(
               fs.existsSync(path.join(envPath, name, "model.malloy")),
            ).toBe(true);

            const again = await publish(
               name,
               kind.location("sales-unversioned"),
            );
            expect(again.status).toBe(200);
            expect(await answerOf(name)).toBe(3);

            const changed = await publish(
               name,
               kind.location("sales-unversioned-changed"),
            );
            expect(changed.status).toBe(200);
            expect(await answerOf(name)).toBe(4);
         });
      });
   }

   it("cloned once per publish from a Git location", () => {
      // Two Git kinds, two tests each, three publishes per test.
      expect(clones).toBe(12);
   });
});
