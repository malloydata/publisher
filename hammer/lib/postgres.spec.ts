// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   type ContainerState,
   containerRunArgs,
   decideContainerAction,
   defaultContainerName,
   LEGACY_CONTAINER_NAME,
   newRunId,
   ownerLiveness,
   parsePublishedPort,
   PG_DATA_DIR,
   type ProcessProbe,
   shouldReap,
   watchdogScript,
} from "./postgres";

/** A probe on host "here" where only `livePids` exist, started at "t0". */
function probe(
   livePids: number[],
   startOf: Record<number, string> = {},
): ProcessProbe {
   return {
      host: "here",
      alive: (pid) => livePids.includes(pid),
      startTime: async (pid) => startOf[pid] ?? "t0",
   };
}

const labelled = (
   overrides: Partial<ContainerState> = {},
   owner = { runId: "r1", host: "here", pid: 42, pidStart: "t0" },
): ContainerState => ({
   name: "publisher-hammer-pg-r1",
   exists: true,
   running: true,
   owner: { kind: "labelled", owner },
   ...overrides,
});

describe("the hammer Postgres container", () => {
   it("is named per run, so two runs started together never share a name", () => {
      const a = newRunId();
      const b = newRunId();
      expect(a).not.toBe(b);
      expect(defaultContainerName(a)).toBe(`${LEGACY_CONTAINER_NAME}-${a}`);
      expect(defaultContainerName(a)).not.toBe(defaultContainerName(b));
   });

   it("reads the port Docker published, whichever address it is on", () => {
      expect(parsePublishedPort("127.0.0.1:49153\n")).toBe(49153);
      expect(parsePublishedPort("0.0.0.0:55432\n[::]:55432\n")).toBe(55432);
      expect(parsePublishedPort("")).toBeUndefined();
   });
});

describe("whether a container's owner is still running", () => {
   it("is live when the pid exists on this host and started when the label says", async () => {
      expect(
         await ownerLiveness(
            { host: "here", pid: 42, pidStart: "t0" },
            probe([42]),
         ),
      ).toBe("live");
   });

   it("is dead when the pid is gone", async () => {
      expect(await ownerLiveness({ host: "here", pid: 42 }, probe([]))).toBe(
         "dead",
      );
   });

   it("is dead when the pid was reused by a process that started later", async () => {
      expect(
         await ownerLiveness(
            { host: "here", pid: 42, pidStart: "t0" },
            probe([42], { 42: "t9" }),
         ),
      ).toBe("dead");
   });

   it("cannot be checked when another host created it", async () => {
      expect(
         await ownerLiveness({ host: "elsewhere", pid: 42 }, probe([42])),
      ).toBe("other-host");
   });
});

describe("what a run does with a named container", () => {
   it("creates one that doesn't exist", () => {
      expect(
         decideContainerAction(
            { name: "x", exists: false, running: false },
            true,
         ).action,
      ).toBe("create");
   });

   it("reuses a running container whose run is over (a --keep run)", () => {
      expect(
         decideContainerAction(labelled({ liveness: "dead" }), true).action,
      ).toBe("reuse");
   });

   it("reuses a running container no run labelled (the old fixed name, or by hand)", () => {
      expect(
         decideContainerAction(
            {
               name: LEGACY_CONTAINER_NAME,
               exists: true,
               running: true,
               owner: { kind: "unlabelled" },
            },
            true,
         ).action,
      ).toBe("reuse");
   });

   it("replaces a stopped container no live run owns", () => {
      expect(
         decideContainerAction(
            labelled({ running: false, liveness: "dead" }),
            true,
         ).action,
      ).toBe("replace");
   });

   it("refuses a live run's container, without suggesting it be shared", () => {
      const d = decideContainerAction(labelled({ liveness: "live" }), true);
      expect(d.action).toBe("refuse");
      if (d.action !== "refuse") throw new Error("unreachable");
      expect(d.message).toContain("pid 42");
      expect(d.message).not.toContain("--reuse-pg");
   });

   it("refuses a container another host's run created, naming how to remove it", () => {
      const d = decideContainerAction(
         labelled({ liveness: "other-host" }),
         true,
      );
      expect(d.action).toBe("refuse");
      if (d.action !== "refuse") throw new Error("unreachable");
      expect(d.message).toContain("docker rm -fv");
   });

   it("fails closed when the owner can't be read: refuses, never removes", () => {
      const d = decideContainerAction(
         {
            name: "x",
            exists: true,
            running: true,
            owner: { kind: "unknown", error: "permission denied" },
         },
         true,
      );
      expect(d.action).toBe("refuse");
      if (d.action !== "refuse") throw new Error("unreachable");
      expect(d.message).toContain("permission denied");
   });
});

describe("the startup reaper", () => {
   const dead = (
      overrides: Partial<ContainerState> = {},
      owner: Record<string, unknown> = {},
   ): ContainerState =>
      labelled(
         { liveness: "dead", ...overrides },
         { runId: "r1", host: "here", pid: 42, pidStart: "t0", ...owner },
      );

   it("removes a hammer container whose run on this host is over", () => {
      expect(shouldReap(dead())).toBe(true);
      expect(shouldReap(dead({ running: false }))).toBe(true);
   });

   it("leaves a container a live run owns", () => {
      expect(shouldReap(dead({ liveness: "live" }))).toBe(false);
   });

   it("leaves a container another host's run made, which it cannot check", () => {
      expect(shouldReap(dead({ liveness: "other-host" }))).toBe(false);
   });

   it("leaves a container a --keep run asked to keep", () => {
      expect(shouldReap(dead({}, { keep: true }))).toBe(false);
   });

   it("leaves anything not named as hammer's, whatever its labels say", () => {
      expect(shouldReap(dead({ name: "service-postgres-1" }))).toBe(false);
      expect(shouldReap(dead({ name: "my-hammer-pg-r1" }))).toBe(false);
   });

   it("leaves an unlabelled or unreadable container, even under a hammer name", () => {
      expect(
         shouldReap({
            name: LEGACY_CONTAINER_NAME,
            exists: true,
            running: true,
            owner: { kind: "unlabelled" },
         }),
      ).toBe(false);
      expect(
         shouldReap({
            name: "publisher-hammer-pg-r1",
            exists: true,
            running: true,
            owner: { kind: "unknown", error: "daemon gone" },
         }),
      ).toBe(false);
   });

   it("leaves a container labelled with a pid but no run id", () => {
      expect(shouldReap(dead({}, { runId: undefined }))).toBe(false);
   });
});

describe("the container a run creates", () => {
   const base = {
      name: "publisher-hammer-pg-r1",
      runId: "r1",
      host: "here",
      pid: 42,
      pidStart: "t0",
      user: "u",
      password: "p",
      image: "postgres:16",
   };

   it("keeps its data on a tmpfs, so Docker makes no anonymous volume", () => {
      const args = containerRunArgs({ ...base, keep: false });
      const i = args.indexOf("--tmpfs");
      expect(i).toBeGreaterThan(-1);
      expect(args[i + 1]).toBe(PG_DATA_DIR);
      expect(args.indexOf("--tmpfs")).toBeLessThan(args.indexOf("postgres:16"));
   });

   it("is --rm and unkept by default, so a stopped container leaves nothing", () => {
      const args = containerRunArgs({ ...base, keep: false });
      expect(args).toContain("--rm");
      expect(args).not.toContain("hammer.keep=1");
      expect(args).toContain("hammer.run=r1");
      expect(args).toContain("hammer.pid=42");
   });

   it("under --keep is neither --rm nor reapable", () => {
      const args = containerRunArgs({ ...base, keep: true });
      expect(args).not.toContain("--rm");
      expect(args).toContain("hammer.keep=1");
   });
});

describe("the crash watchdog", () => {
   it("waits on the run's pid, then removes by the run's label with its volumes", () => {
      const script = watchdogScript("r1", 42);
      expect(script).toContain("kill -0 42");
      expect(script).toContain("--filter label=hammer.run=r1");
      expect(script).toContain("docker rm -fv");
      expect(script).not.toContain("publisher-hammer-pg");
   });

   it("refuses a run id that could break out of the shell script", () => {
      expect(() => watchdogScript("r1; rm -rf /", 42)).toThrow();
   });
});
