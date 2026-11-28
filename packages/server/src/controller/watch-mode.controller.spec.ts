// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The deprecated main-API watch-mode routes: their `{ error }` refusal body,
 * kept until the routes are removed (the admin API serves watch mode now).
 */

import { afterAll, afterEach, describe, expect, it, spyOn } from "bun:test";
import fs from "fs";
import os from "os";
import path from "path";
import request from "supertest";
import { logger } from "../logger";
import { linesMentioning } from "../test_helpers/log_lines";
import { API_PREFIX, buildRouteApp } from "../test_helpers/route_app";
import { coalescedReloads, WatchModeController } from "./watch-mode.controller";
import { fakeOf } from "../test_helpers/store_fakes";
import type { PackageLookup } from "../service/environment_ports";

const root = fs.mkdtempSync(path.join(os.tmpdir(), "watch-mode-spec-"));
afterAll(() => fs.rmSync(root, { recursive: true, force: true }));

function setup() {
   const controller = new WatchModeController(
      fakeOf<PackageLookup & { serverRootPath: string }>({
         serverRootPath: root,
      }),
   );
   return {
      controller,
      app: buildRouteApp({ watchModeController: controller }),
   };
}

describe("deprecated watch-mode routes", () => {
   it("report the status, marked deprecated", async () => {
      const res = await request(setup().app).get(
         `${API_PREFIX}/watch-mode/status`,
      );
      expect(res.status).toBe(200);
      expect(res.headers.deprecation).toBe("true");
      expect(res.body).toEqual({
         enabled: false,
         watchingPath: "",
         environmentName: "",
      });
   });

   it("refuse an environment that is not configured with 404 { error }", async () => {
      const res = await request(setup().app)
         .post(`${API_PREFIX}/watch-mode/start`)
         .send({ environmentName: "nowhere" });
      expect(res.status).toBe(404);
      expect(res.body).toEqual({
         error: "Environment nowhere not found or has no packages",
      });
   });

   it("refuse an unsafe name with 400 { error }", async () => {
      const res = await request(setup().app)
         .post(`${API_PREFIX}/watch-mode/start`)
         .send({ environmentName: "../up" });
      expect(res.status).toBe(400);
      expect(Object.keys(res.body)).toEqual(["error"]);
   });

   it("stop idempotently", async () => {
      const res = await request(setup().app).post(
         `${API_PREFIX}/watch-mode/stop`,
      );
      expect(res.status).toBe(200);
   });

   it("answer a stop that fails with 500 { error } rather than hang", async () => {
      const { controller, app } = setup();
      controller.watcher = {
         close: async () => {
            throw new Error("watch-stop-marker");
         },
      } as unknown as WatchModeController["watcher"];
      const error = spyOn(logger, "error");
      try {
         const res = await request(app)
            .post(`${API_PREFIX}/watch-mode/stop`)
            .timeout(5000);
         expect(res.status).toBe(500);
         expect(Object.keys(res.body)).toEqual(["error"]);
         expect(res.text).not.toContain("watch-stop-marker");
         expect(linesMentioning(error, "watch-stop-marker")).toHaveLength(1);
      } finally {
         error.mockRestore();
      }
   });

   it("log a refused start once, as a refusal", async () => {
      const warn = spyOn(logger, "warn");
      const error = spyOn(logger, "error");
      try {
         await request(setup().app)
            .post(`${API_PREFIX}/watch-mode/start`)
            .send({ environmentName: "watch-refusal-marker" });
         expect(linesMentioning(warn, "watch-refusal-marker")).toHaveLength(1);
         expect(linesMentioning(error, "watch-refusal-marker")).toEqual([]);
      } finally {
         warn.mockRestore();
         error.mockRestore();
      }
   });
});

describe("live reload of what a package's workflows serve", () => {
   const SET = "sets/main";
   let envDir: string;
   let controller: WatchModeController | undefined;

   afterEach(async () => {
      await controller?.unwatch();
      controller = undefined;
      fs.rmSync(envDir, { recursive: true, force: true });
   });

   /**
    * A watcher over a real directory: the package names the environment is asked to recompile, and the files the controller then announced.
    * `onReload` stands in for the package adopting a manifest on a reload: it may change the paths the package reports as watched, and the reload waits for what it returns.
    */
   async function watchPackage(
      onReload: (watched: string[]) => void | Promise<void> = () => undefined,
   ) {
      envDir = fs.mkdtempSync(path.join(root, "env-"));
      fs.mkdirSync(path.join(envDir, "pkg", SET, "skill"), { recursive: true });
      fs.mkdirSync(path.join(envDir, "pkg", "scripts"), { recursive: true });
      const watched = [SET, "wf/instructions.md"];
      const reloaded: string[] = [];
      const announced: string[] = [];
      const waiting = new Map<string, () => void>();
      /** Resolves once the controller has announced `file`, which it does after any recompile the file calls for. */
      const announcedFile = (file: string) =>
         new Promise<void>((resolve) => {
            if (announced.includes(file)) resolve();
            else waiting.set(file, resolve);
         });
      const environment = {
         getEnvironmentPath: () => envDir,
         getLoadedPackages: () => [
            {
               getPackageName: () => "pkg",
               workflowPaths: () => watched,
            },
         ],
         getPackage: async (name: string, reload: boolean) => {
            if (reload) {
               reloaded.push(name);
               await onReload(watched);
            }
         },
      };
      controller = new WatchModeController(
         fakeOf<PackageLookup & { serverRootPath: string }>({
            serverRootPath: root,
            getEnvironment: async () => environment,
         }),
      );
      controller.events.on("env/pkg", ({ path: file }: { path: string }) => {
         announced.push(file);
         waiting.get(file)?.();
      });
      await controller.ensureWatching("env");
      // Awaiting the setup takes microtasks only, so the initial scan has not reported yet.
      await new Promise<void>((resolve) =>
         controller!.watcher!.on("ready", resolve),
      );
      return { reloaded, announced, announcedFile };
   }

   /** A model save always recompiles, so its announcement closes the window in which an earlier file save would have shown up. */
   const sentinelSaved = async (
      announcedFile: (file: string) => Promise<void>,
   ) => {
      const sentinel = path.join(envDir, "pkg", "scripts", "sentinel.malloy");
      fs.writeFileSync(sentinel, "source: s is duckdb.sql('select 1')");
      await announcedFile(sentinel);
      return sentinel;
   };

   // Windows reports a created file as an add and then a change, so it announces a new file twice.
   const EVENTS_PER_SAVE = process.platform === "win32" ? 2 : 1;

   /** What was announced, once per file in first-announced order, and the recompiles it took: events that arrive during one share the next. */
   function expectSaves(
      { reloaded, announced }: { reloaded: string[]; announced: string[] },
      files: string[],
   ) {
      expect([...new Set(announced)]).toEqual(files);
      expect(announced.length).toBeLessThanOrEqual(
         files.length * EVENTS_PER_SAVE,
      );
      expect(reloaded.length).toBeGreaterThanOrEqual(files.length);
      expect(reloaded.length).toBeLessThanOrEqual(announced.length);
      expect(reloaded.filter((name) => name !== "pkg")).toEqual([]);
   }

   /** Resolves once the watcher holds `file`: adding a path to it is asynchronous and announces nothing, so this is the one wait on a condition. */
   const isWatching = async (file: string) => {
      for (let i = 0; i < 400; i++) {
         const names = controller!.watcher!.getWatched()[path.dirname(file)];
         if (names?.includes(path.basename(file))) return;
         await new Promise((resolve) => setTimeout(resolve, 5));
      }
      throw new Error(`${file} is not watched`);
   };

   it("recompiles the package for a script saved under a skill set, whatever its extension, and not for one outside any set", async () => {
      const { reloaded, announced, announcedFile } = await watchPackage();
      const outside = path.join(envDir, "pkg", "scripts", "other.py");
      // A model save always recompiles, so its announcement closes the window in which `outside` would have shown up.
      const sentinel = path.join(envDir, "pkg", "scripts", "sentinel.malloy");
      const inSet = path.join(envDir, "pkg", SET, "skill", "run.py");
      fs.writeFileSync(outside, "x");
      fs.writeFileSync(sentinel, "source: s is duckdb.sql('select 1')");
      await announcedFile(sentinel);
      fs.writeFileSync(inSet, "print(1)");
      await announcedFile(inSet);
      expectSaves({ reloaded, announced }, [sentinel, inSet]);
   }, 10_000);

   it("recompiles the package for its own publisher.json, which declares the workflows, and for one that appears at a set's root, which refuses them", async () => {
      const { reloaded, announced, announcedFile } = await watchPackage();
      const manifest = path.join(envDir, "pkg", "publisher.json");
      const atSet = path.join(envDir, "pkg", SET, "publisher.json");
      fs.writeFileSync(manifest, "{}");
      await announcedFile(manifest);
      fs.writeFileSync(atSet, "{}");
      await announcedFile(atSet);
      expectSaves({ reloaded, announced }, [manifest, atSet]);
   }, 10_000);

   it("recompiles for a file in a skill set the manifest declares after the watcher started, from the reload that adopted the manifest", async () => {
      const NEW = "sets/declared-later";
      let declared = false;
      const { reloaded, announced, announcedFile } = await watchPackage(
         (watched) => {
            if (declared && !watched.includes(NEW)) watched.push(NEW);
         },
      );
      const skill = path.join(envDir, "pkg", NEW, "skill");
      fs.mkdirSync(skill, { recursive: true });
      // No manifest lists the set yet, so a save under it is no skill's.
      fs.writeFileSync(path.join(skill, "early.py"), "print(0)");
      // A model save always recompiles, so its announcement closes the window in which `early.py` would have shown up, and shows the directory was read while the set was undeclared.
      const marker = path.join(skill, "marker.md");
      fs.writeFileSync(marker, "# marker");
      await announcedFile(marker);
      expectSaves({ reloaded, announced }, [marker]);

      declared = true;
      const manifest = path.join(envDir, "pkg", "publisher.json");
      fs.writeFileSync(manifest, "{}");
      await announcedFile(manifest);

      // A file the set already held when it was declared was filtered out at discovery; its first save after the declaration must still recompile.
      const early = path.join(skill, "early.py");
      await isWatching(early);
      fs.writeFileSync(early, "print(2)");
      await announcedFile(early);
      const late = path.join(skill, "late.py");
      fs.writeFileSync(late, "print(1)");
      await announcedFile(late);
      expectSaves({ reloaded, announced }, [marker, manifest, early, late]);
   }, 10_000);

   it("reads a skill's .skillignore when a file is saved, so one added after the watcher started stops the recompile for what it matches", async () => {
      const { reloaded, announced, announcedFile } = await watchPackage();
      const skill = path.join(envDir, "pkg", SET, "skill");
      const log = path.join(skill, "debug.log");
      fs.writeFileSync(log, "one");
      await announcedFile(log);

      const skillignore = path.join(skill, ".skillignore");
      fs.writeFileSync(skillignore, "*.log\n");
      await announcedFile(skillignore);

      // The same file, saved again, is now the skill's to ignore.
      fs.writeFileSync(log, "two");
      const sentinel = await sentinelSaved(announcedFile);
      expectSaves({ reloaded, announced }, [log, skillignore, sentinel]);
      // The first save's second announcement, where there is one, may come before the `.skillignore`'s, never after it.
      expect(
         announced
            .slice(announced.indexOf(skillignore))
            .filter((f) => f === log),
      ).toEqual([]);
   }, 10_000);

   it("reads a skill's .skillignore without a synchronous fs call when a file is saved, and still applies it", async () => {
      const { announced, announcedFile } = await watchPackage();
      const skill = path.join(envDir, "pkg", SET, "skill");
      const skillignore = path.join(skill, ".skillignore");
      fs.writeFileSync(skillignore, "*.log\n");
      await announcedFile(skillignore);

      const spies = [spyOn(fs, "readFileSync"), spyOn(fs, "lstatSync")];
      try {
         fs.writeFileSync(path.join(skill, "debug.log"), "x");
         const sentinel = await sentinelSaved(announcedFile);
         const kept = path.join(skill, "kept.txt");
         fs.writeFileSync(kept, "x");
         await announcedFile(kept);
         expect([...new Set(announced)]).toEqual([skillignore, sentinel, kept]);
         expect(announced.length).toBeLessThanOrEqual(3 * EVENTS_PER_SAVE);
         for (const spy of spies) {
            expect(
               spy.mock.calls.filter(([file]) =>
                  String(file).endsWith(".skillignore"),
               ),
            ).toEqual([]);
         }
      } finally {
         for (const spy of spies) spy.mockRestore();
      }
   }, 10_000);

   it("recompiles twice for a burst of 50 saves, the second after the last save, so the package serves the last write", async () => {
      const BURST = 50;
      const file = (i: number) =>
         path.join(envDir, "pkg", "scripts", `burst-${i}.malloy`);
      // What the last file held each time the package was recompiled.
      const lastSeen: string[] = [];
      let release!: () => void;
      const held = new Promise<void>((resolve) => (release = resolve));
      let started!: () => void;
      const firstStarted = new Promise<void>((resolve) => (started = resolve));
      const { reloaded, announced, announcedFile } = await watchPackage(
         async () => {
            lastSeen.push(
               fs.existsSync(file(BURST - 1))
                  ? fs.readFileSync(file(BURST - 1), "utf8")
                  : "",
            );
            if (lastSeen.length === 1) {
               started();
               await held;
            }
         },
      );
      fs.writeFileSync(file(0), "write 0");
      await firstStarted;
      // The controller's listener runs first and reaches the reload before any await, so by the time this one sees a file its event has been handed to the reload.
      let delivered = 0;
      const burstDelivered = new Promise<void>((resolve) =>
         controller!.watcher!.on("add", () => {
            if (++delivered === BURST - 1) resolve();
         }),
      );
      for (let i = 1; i < BURST; i++) fs.writeFileSync(file(i), `write ${i}`);
      await burstDelivered;
      release();
      await announcedFile(file(BURST - 1));
      expect(reloaded).toEqual(["pkg", "pkg"]);
      expect(lastSeen).toEqual(["", `write ${BURST - 1}`]);
      expect(new Set(announced).size).toBe(BURST);
      expect(announced.length).toBeLessThanOrEqual(BURST * EVENTS_PER_SAVE);
   }, 15_000);

   it("leaves a file the skill never serves alone, and recompiles for the .skillignore that decides it and for the instructions", async () => {
      const { reloaded, announced, announcedFile } = await watchPackage();
      const skill = path.join(envDir, "pkg", SET, "skill");
      fs.mkdirSync(path.join(skill, "__pycache__"));
      fs.mkdirSync(path.join(skill, "scratch"));
      fs.mkdirSync(path.join(envDir, "pkg", "wf"));
      const skillignore = path.join(skill, ".skillignore");
      fs.writeFileSync(skillignore, "scratch/\n*.log\n");
      await announcedFile(skillignore);
      const never = [
         path.join(skill, "__pycache__", "run.pyc"),
         path.join(skill, ".env"),
         path.join(skill, "scratch", "notes.txt"),
         path.join(skill, "debug.log"),
         path.join(envDir, "pkg", SET, "loose.txt"),
      ];
      for (const file of never) fs.writeFileSync(file, "x");
      // A model save always recompiles, so its announcement closes the window in which a file above would have shown up.
      const sentinel = path.join(envDir, "pkg", "scripts", "sentinel.malloy");
      fs.writeFileSync(sentinel, "source: s is duckdb.sql('select 1')");
      await announcedFile(sentinel);
      const served = path.join(skill, "kept.txt");
      fs.writeFileSync(served, "x");
      await announcedFile(served);
      const instructions = path.join(envDir, "pkg", "wf", "instructions.md");
      fs.writeFileSync(instructions, "Be brief.");
      await announcedFile(instructions);
      expectSaves({ reloaded, announced }, [
         skillignore,
         sentinel,
         served,
         instructions,
      ]);
   }, 10_000);
});

describe("coalescedReloads", () => {
   /** A reload that holds until the test lets it finish, recording the order things happened in. */
   function gatedReload() {
      const log: string[] = [];
      const gates: Array<() => void> = [];
      const reload = async (key: string) => {
         log.push(`start ${key}`);
         await new Promise<void>((resolve) => gates.push(resolve));
         log.push(`end ${key}`);
         return true;
      };
      /** Lets the running reload end, and yields until the next one has started or none follows. */
      const finishOne = async () => {
         gates.shift()!();
         await new Promise<void>((resolve) => setImmediate(resolve));
      };
      return { log, reload, finishOne, running: () => gates.length };
   }

   it("runs one reload and at most one more for a burst of 200 events on one package", async () => {
      const { log, reload, finishOne } = gatedReload();
      const schedule = coalescedReloads(reload);
      const done = Array.from({ length: 200 }, () => schedule("pkg"));
      await finishOne();
      await finishOne();
      expect(await Promise.all(done)).toEqual(Array(200).fill(true));
      expect(log).toEqual(["start pkg", "end pkg", "start pkg", "end pkg"]);
   });

   it("starts the trailing reload after the last event, so it reads the burst's last write", async () => {
      const { log, reload, finishOne } = gatedReload();
      const schedule = coalescedReloads(reload);
      const first = schedule("pkg");
      const last = schedule("pkg");
      log.push("last event");
      await finishOne();
      await finishOne();
      await Promise.all([first, last]);
      expect(log).toEqual([
         "start pkg",
         "last event",
         "end pkg",
         "start pkg",
         "end pkg",
      ]);
   });

   it("keeps packages apart and starts a fresh reload for an event after the run ended", async () => {
      const { log, reload, finishOne } = gatedReload();
      const schedule = coalescedReloads(reload);
      const a = schedule("a");
      const b = schedule("b");
      await finishOne();
      await finishOne();
      await Promise.all([a, b]);
      expect(log).toEqual(["start a", "start b", "end a", "end b"]);
      const again = schedule("a");
      await finishOne();
      await again;
      expect(log.slice(4)).toEqual(["start a", "end a"]);
   });
});
