// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import fs from "fs";
import os from "os";
import path from "path";
import {
   BadRequestError,
   internalErrorToHttpError,
   WriteRollbackFailedError,
   WriteVerifyError,
} from "../errors";
import { logger } from "../logger";
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

/**
 * The log lines a spy saw about a write or its rollback, by either name the
 * error mapping or this service gives them; other tests in the process log
 * through the same logger.
 */
const writeLines = (spy: { mock: { calls: unknown[][] } }) =>
   spy.mock.calls.filter(([text]) => /write|rolled back/i.test(String(text)));

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

   /**
    * One rollback, one line: it is logged where it happened, with the cause,
    * and nowhere else, so a count of these lines is a count of rollbacks.
    */
   it("logs a rollback once, at warn, and the error mapping adds nothing", async () => {
      fs.writeFileSync(target(), "good");
      const warn = spyOn(logger, "warn");
      const error = spyOn(logger, "error");
      try {
         const rolledBack = await environment
            .writeModelFileTransactional(
               PKG,
               FILE,
               "bad",
               () => undefined,
               async () => {
                  throw new Error("did not reload");
               },
            )
            .then(
               () => undefined,
               (e: Error) => e,
            );
         expect(internalErrorToHttpError(rolledBack!).status).toBe(500);
         expect(writeLines(warn)).toEqual([
            [
               "Report write rolled back",
               { packageName: PKG, model: FILE, error: "did not reload" },
            ],
         ]);
         expect(writeLines(error)).toEqual([]);
      } finally {
         warn.mockRestore();
         error.mockRestore();
      }
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

   describe("what a rolled-back write tells the caller", () => {
      const rolledBack = (verify: () => Promise<unknown>) =>
         environment
            .writeModelFileTransactional(
               PKG,
               FILE,
               "bad",
               () => undefined,
               verify,
            )
            .then(
               () => {
                  throw new Error("expected a rollback");
               },
               (error: Error) => error,
            );

      it("keeps a reload failure's server path out of the message", async () => {
         let calls = 0;
         (
            environment as unknown as {
               _loadOrGetPackageLocked: () => Promise<unknown>;
            }
         )._loadOrGetPackageLocked = async () => {
            if (calls++ === 0)
               throw Object.assign(
                  new Error(
                     "ENOENT: no such file, open '/srv/publisher_data/env/pkg/x.malloy'",
                  ),
                  { code: "ENOENT", syscall: "open" },
               );
            return {};
         };
         const error = await rolledBack(async () => undefined);
         expect(error.message).toMatch(/previous text was put back/);
         expect(error.message).not.toMatch(/publisher_data/);
      });

      it("carries a verify refusal worded for the caller", async () => {
         const error = await rolledBack(async () => {
            throw new WriteVerifyError("not served");
         });
         expect(error.message).toContain("not served");
      });

      it("drops any other verify failure's text", async () => {
         const error = await rolledBack(async () => {
            throw new Error("FILE: file:///x");
         });
         expect(error.message).not.toContain("file:///x");
         expect(error.message).toMatch(/previous text was put back/);
      });
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

   describe("the directories a create makes", () => {
      const pkgPath = (...parts: string[]) => path.join(root, PKG, ...parts);

      it("refuses a path through an existing file with a 400, creating nothing", async () => {
         fs.writeFileSync(pkgPath("plain"), "a file, not a directory");
         const error = await environment
            .writeModelFileTransactional(
               PKG,
               "plain/deeper/x.malloy",
               "text",
               () => undefined,
               ok,
            )
            .then(
               () => undefined,
               (e: Error) => e,
            );
         expect(error).toBeInstanceOf(BadRequestError);
         expect(error?.message).toContain("plain");
         expect(error?.message).toContain("is a file");
         expect(fs.readFileSync(pkgPath("plain"), "utf8")).toBe(
            "a file, not a directory",
         );
         expect(reloads).toBe(0);
      });

      it("removes every directory it created when the write itself fails", async () => {
         (
            environment as unknown as {
               _writeModelFileLocked: () => Promise<void>;
            }
         )._writeModelFileLocked = async () => {
            throw new Error("disk full");
         };
         await expect(
            environment.writeModelFileTransactional(
               PKG,
               "made/here/x.malloy",
               "text",
               () => undefined,
               ok,
            ),
         ).rejects.toThrow("disk full");
         expect(fs.existsSync(pkgPath("made"))).toBe(false);
         expect(reloads).toBe(0);
      });

      it("refuses a directory that resolves into public/, whatever its case", async () => {
         fs.mkdirSync(pkgPath("public"));
         // On a case-insensitive file system `Public` IS `public`; a symlink
         // reaches it on any file system. Either way the write would land in
         // the directory served as raw static files.
         fs.symlinkSync(pkgPath("public"), pkgPath("mirror"));
         for (const modelPath of ["mirror/x.malloy", "mirror/new/x.malloy"]) {
            const error = await environment
               .writeModelFileTransactional(
                  PKG,
                  modelPath,
                  "text",
                  () => undefined,
                  ok,
               )
               .then(
                  () => undefined,
                  (e: Error) => e,
               );
            expect(error).toBeInstanceOf(BadRequestError);
            expect(error?.message).toContain("public/");
         }
         expect(fs.readdirSync(pkgPath("public"))).toEqual([]);
      });

      it("refuses a directory that resolves outside the package before reading anything through it", async () => {
         const outside = fs.mkdtempSync(
            path.join(os.tmpdir(), "publisher-out-"),
         );
         try {
            fs.writeFileSync(path.join(outside, "x.malloy"), "outside text");
            fs.symlinkSync(outside, pkgPath("escape"));
            for (const modelPath of [
               "escape/x.malloy",
               "escape/new/x.malloy",
            ]) {
               const seen: Array<string | undefined> = [];
               const error = await environment
                  .writeModelFileTransactional(
                     PKG,
                     modelPath,
                     "text",
                     (current) => {
                        seen.push(current);
                     },
                     ok,
                  )
                  .then(
                     () => undefined,
                     (e: Error) => e,
                  );
               expect(error).toBeInstanceOf(BadRequestError);
               expect(error?.message).toContain("outside the package");
               // The check never saw the outside file's text.
               expect(seen).toEqual([]);
            }
            expect(fs.readdirSync(outside)).toEqual(["x.malloy"]);
            expect(
               fs.readFileSync(path.join(outside, "x.malloy"), "utf8"),
            ).toBe("outside text");
            expect(reloads).toBe(0);
         } finally {
            fs.rmSync(outside, { recursive: true, force: true });
         }
      });

      it("refuses a file that is a link out of the package before reading it", async () => {
         const outside = fs.mkdtempSync(
            path.join(os.tmpdir(), "publisher-out-"),
         );
         try {
            fs.writeFileSync(
               path.join(outside, "secret.malloy"),
               "outside text",
            );
            fs.symlinkSync(
               path.join(outside, "secret.malloy"),
               pkgPath("link.malloy"),
            );
            const seen: Array<string | undefined> = [];
            await expect(
               environment.writeModelFileTransactional(
                  PKG,
                  "link.malloy",
                  "text",
                  (current) => {
                     seen.push(current);
                  },
                  ok,
               ),
            ).rejects.toBeInstanceOf(BadRequestError);
            expect(seen).toEqual([]);
            expect(
               fs.readFileSync(path.join(outside, "secret.malloy"), "utf8"),
            ).toBe("outside text");
         } finally {
            fs.rmSync(outside, { recursive: true, force: true });
         }
      });
   });

   it.each(["index.malloy", "Index.malloy", "INDEX.malloy", "./index.malloy"])(
      "refuses the root %s, the surface, whatever its case",
      async (modelPath) => {
         await expect(
            environment.writeModelFileTransactional(
               PKG,
               modelPath,
               "## artifact { layout=linear }\n",
               () => undefined,
               ok,
            ),
         ).rejects.toBeInstanceOf(BadRequestError);
         expect(fs.existsSync(path.join(root, PKG, modelPath))).toBe(false);
      },
   );

   it("refuses a path that is not a safe relative model path, even from a caller that skipped the check", async () => {
      for (const modelPath of ["../escape.malloy", "/abs.malloy"]) {
         await expect(
            environment.writeModelFileTransactional(
               PKG,
               modelPath,
               "text",
               () => undefined,
               ok,
            ),
         ).rejects.toBeInstanceOf(BadRequestError);
      }
      expect(reloads).toBe(0);
   });

   it("says so, and logs it, when putting the previous text back fails too", async () => {
      fs.writeFileSync(target(), "good");
      (
         environment as unknown as {
            _loadOrGetPackageLocked: () => Promise<unknown>;
         }
      )._loadOrGetPackageLocked = async () => {
         reloads += 1;
         throw new Error(reloads === 1 ? "reload failed" : "restore failed");
      };
      const warn = spyOn(logger, "warn");
      const logError = spyOn(logger, "error");
      let error: Error | undefined;
      try {
         error = await environment
            .writeModelFileTransactional(PKG, FILE, "bad", () => undefined, ok)
            .then(
               () => undefined,
               (e: Error) => e,
            );
         internalErrorToHttpError(error!);
         // Once, at error, with both causes; never also as a rollback.
         expect(writeLines(logError)).toEqual([
            [
               "Report write rollback failed",
               {
                  packageName: PKG,
                  model: FILE,
                  error: "reload failed",
                  restoreError: "restore failed",
               },
            ],
         ]);
         expect(writeLines(warn)).toEqual([]);
      } finally {
         warn.mockRestore();
         logError.mockRestore();
      }
      expect(error).toBeInstanceOf(WriteRollbackFailedError);
      expect(error?.message).toContain("putting the previous text back failed");
      // The restore itself was written; it is the reload of it that failed.
      expect(read()).toBe("good");
      expect(reloads).toBe(2);
   });
});
