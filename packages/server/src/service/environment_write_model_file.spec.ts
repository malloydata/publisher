// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs";
import os from "os";
import path from "path";
import { Environment } from "./environment";

/**
 * `writeModelFileTransactional`: the precondition, the write, the reload and
 * the restore all happen under one hold of the package lock. That single hold
 * is what makes the dashboard endpoint's 409 mean anything and what makes a
 * rolled-back write actually roll back — tested at the service, because the
 * controller's stub cannot show that two callers racing on one file neither
 * both pass their check nor revert each other's text.
 */
const PKG = "pkg";
const FILE = "dashboards/overview.malloy";

let root: string;
let environment: Environment;
/** Packages reloaded, in order, by the stubbed locked loader. */
let reloads: number;

const target = () => path.join(root, PKG, FILE);
const read = () =>
   fs.existsSync(target()) ? fs.readFileSync(target(), "utf8") : undefined;

beforeEach(() => {
   root = fs.mkdtempSync(path.join(os.tmpdir(), "publisher-write-"));
   fs.mkdirSync(path.join(root, PKG, "dashboards"), { recursive: true });
   reloads = 0;
   environment = new Environment(
      "env",
      root,
      {} as never,
      [] as never,
   ) as Environment;
   // The reload is the package loader, which wants a real package tree; the
   // transaction only cares that it is called inside the lock and that its
   // result reaches `verify`.
   (
      environment as unknown as {
         _loadOrGetPackageLocked: () => Promise<unknown>;
      }
   )._loadOrGetPackageLocked = async () => {
      reloads += 1;
      return { reloaded: true };
   };
});

afterEach(() => fs.rmSync(root, { recursive: true, force: true }));

const ok = async () => undefined;

describe("Environment.writeModelFileTransactional", () => {
   it("writes a new file and reports that nothing was there", async () => {
      const { previous } = await environment.writeModelFileTransactional(
         PKG,
         FILE,
         "new text",
         () => undefined,
         ok,
      );
      expect(previous).toBeUndefined();
      expect(read()).toBe("new text");
      expect(reloads).toBe(1);
   });

   it("hands the check the text on disk, and writes nothing when it refuses", async () => {
      fs.writeFileSync(target(), "on disk");
      const seen: Array<string | undefined> = [];
      await expect(
         environment.writeModelFileTransactional(
            PKG,
            FILE,
            "new text",
            (current) => {
               seen.push(current);
               throw new Error("refused");
            },
            ok,
         ),
      ).rejects.toThrow("refused");
      expect(seen).toEqual(["on disk"]);
      expect(read()).toBe("on disk");
      // A refused precondition never got as far as writing, so nothing to
      // reload either.
      expect(reloads).toBe(0);
   });

   it("passes the reloaded package to verify", async () => {
      const seen: unknown[] = [];
      const { verified } = await environment.writeModelFileTransactional(
         PKG,
         FILE,
         "text",
         () => undefined,
         async (reloaded) => {
            seen.push(reloaded);
            return "verified";
         },
      );
      expect(seen).toEqual([{ reloaded: true }]);
      expect(verified).toBe("verified");
   });

   it("puts the previous text back when verify refuses", async () => {
      fs.writeFileSync(target(), "good");
      await expect(
         environment.writeModelFileTransactional(
            PKG,
            FILE,
            "bad",
            () => undefined,
            async () => {
               throw new Error("did not reload");
            },
         ),
      ).rejects.toThrow(/previous text was put back/);
      expect(read()).toBe("good");
      // Once for the write, once to put the package back on the old text.
      expect(reloads).toBe(2);
   });

   it("removes the file when verify refuses a newly created one", async () => {
      await expect(
         environment.writeModelFileTransactional(
            PKG,
            FILE,
            "bad",
            () => undefined,
            async () => {
               throw new Error("did not reload");
            },
         ),
      ).rejects.toThrow(/previous text was put back/);
      expect(read()).toBeUndefined();
   });

   it("serializes two racing writes, so the second checks what the first wrote", async () => {
      fs.writeFileSync(target(), "v1");
      const seen: Array<string | undefined> = [];
      const check = (current: string | undefined) => {
         seen.push(current);
      };
      await Promise.all([
         environment.writeModelFileTransactional(PKG, FILE, "v2", check, ok),
         environment.writeModelFileTransactional(PKG, FILE, "v3", check, ok),
      ]);
      // Whichever ran second saw the other's text rather than the original: the
      // pair cannot both have passed a precondition against "v1".
      expect(seen[0]).toBe("v1");
      expect(["v2", "v3"]).toContain(seen[1] as string);
      expect(seen[1]).not.toBe("v1");
   });

   it("rolls back before a queued writer sees the file", async () => {
      // The regression this guards: with the restore outside the lock, a
      // second writer lands between the failed reload and the restore, and the
      // restore then reverts THEIR text instead of the failed write's.
      fs.writeFileSync(target(), "good");
      const seen: Array<string | undefined> = [];
      const failing = environment
         .writeModelFileTransactional(
            PKG,
            FILE,
            "bad",
            () => undefined,
            async () => {
               throw new Error("did not reload");
            },
         )
         .catch(() => undefined);
      const queued = environment.writeModelFileTransactional(
         PKG,
         FILE,
         "next",
         (current) => {
            seen.push(current);
         },
         ok,
      );
      await Promise.all([failing, queued]);
      // The queued writer saw the ROLLED-BACK text, never the text that failed
      // to reload, and its own write survived the other's restore.
      expect(seen).toEqual(["good"]);
      expect(read()).toBe("next");
   });
});
