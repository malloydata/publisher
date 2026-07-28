import { afterEach, describe, expect, test } from "bun:test";
import * as http from "node:http";
import {
   fetchLatestVersion,
   isOlder,
   staleScaffolderWarning,
   startVersionCheck,
} from "./registry_check";

/**
 * A stand-in registry, so nothing here touches the network. Every test that needs
 * a response starts one, and afterEach closes whatever the last one was: a test
 * that leaves a listener behind holds the port and the process open.
 */
let server: http.Server | undefined;

function serve(
   handler: (req: http.IncomingMessage, res: http.ServerResponse) => void,
): Promise<string> {
   return new Promise((resolve) => {
      server = http.createServer(handler);
      // Port 0 so the OS picks a free one. A fixed port here is how a suite ends
      // up failing on whatever else happens to be running on the machine.
      server.listen(0, "127.0.0.1", () => {
         const addr = server!.address() as { port: number };
         resolve(`http://127.0.0.1:${addr.port}/`);
      });
   });
}

afterEach(() => {
   server?.close();
   server = undefined;
   delete process.env.CREATE_MALLOY_PACKAGE_NO_UPDATE_CHECK;
});

describe("isOlder", () => {
   test("orders plain versions", () => {
      expect(isOlder("0.0.1", "0.0.2")).toBe(true);
      expect(isOlder("0.0.2", "0.0.1")).toBe(false);
      expect(isOlder("0.0.2", "0.0.2")).toBe(false);
      expect(isOlder("0.1.0", "0.2.0")).toBe(true);
      expect(isOlder("1.0.0", "0.9.9")).toBe(false);
   });

   test("compares numerically, not as strings", () => {
      // "0.0.9" > "0.0.10" lexically, which is the bug this guards.
      expect(isOlder("0.0.9", "0.0.10")).toBe(true);
      expect(isOlder("0.10.0", "0.9.0")).toBe(false);
   });

   test("declines rather than guessing on anything that is not x.y.z", () => {
      // Declining costs a missed warning. Guessing wrong tells someone their
      // current install is stale, which is worse.
      for (const bad of [
         "unknown",
         "1.0.0-rc.1",
         "1.0.0+build",
         "1.0",
         "",
         "v1.0.0",
      ]) {
         expect(isOlder(bad, "1.0.0")).toBeUndefined();
         expect(isOlder("1.0.0", bad)).toBeUndefined();
      }
   });
});

describe("staleScaffolderWarning", () => {
   test("names both versions and the command that fixes it", () => {
      const warning = staleScaffolderWarning("0.0.1", "0.0.2");
      expect(warning).toBeDefined();
      expect(warning).toContain("0.0.1");
      expect(warning).toContain("0.0.2");
      expect(warning).toContain(
         "npm create @malloy-publisher/malloy-package@latest",
      );
   });

   test("says nothing when current, ahead, or unknown", () => {
      expect(staleScaffolderWarning("0.0.2", "0.0.2")).toBeUndefined();
      expect(staleScaffolderWarning("0.0.3", "0.0.2")).toBeUndefined();
      expect(staleScaffolderWarning("0.0.1", undefined)).toBeUndefined();
      // An unparseable version on either side declines, so a prerelease build
      // never prints a warning it cannot justify.
      expect(staleScaffolderWarning("unknown", "0.0.2")).toBeUndefined();
      expect(staleScaffolderWarning("0.0.1", "0.0.2-rc.1")).toBeUndefined();
   });
});

describe("fetchLatestVersion", () => {
   test("reads version out of a registry document", async () => {
      const url = await serve((_req, res) => {
         res.writeHead(200, { "content-type": "application/json" });
         res.end(JSON.stringify({ name: "x", version: "9.9.9" }));
      });
      expect(await fetchLatestVersion(url)).toBe("9.9.9");
   });

   test("fails open on a non-2xx", async () => {
      const url = await serve((_req, res) => {
         res.writeHead(500);
         res.end("nope");
      });
      expect(await fetchLatestVersion(url)).toBeUndefined();
   });

   test("fails open on a body that is not what we expect", async () => {
      for (const body of ["not json at all", "[]", '{"version":42}', "null"]) {
         const url = await serve((_req, res) => {
            res.writeHead(200, { "content-type": "application/json" });
            res.end(body);
         });
         expect(await fetchLatestVersion(url)).toBeUndefined();
         server?.close();
         server = undefined;
      }
   });

   test("fails open when nothing is listening", async () => {
      // Port 1 is reserved and nothing serves it, so this is a connection error
      // rather than a timeout, and it must still be silent.
      expect(await fetchLatestVersion("http://127.0.0.1:1/")).toBeUndefined();
   });

   test("gives up rather than hanging on a server that never answers", async () => {
      const url = await serve(() => {
         // Accept the request and never respond.
      });
      const started = Date.now();
      expect(await fetchLatestVersion(url, 150)).toBeUndefined();
      // Comfortably under the real 1500ms default, so a wedged registry costs a
      // moment and not the run.
      expect(Date.now() - started).toBeLessThan(2000);
   });

   test("gives up on a server that answers and then trickles bytes forever", async () => {
      // The case a socket-inactivity timeout does NOT catch: every byte resets
      // it, so on its own the promise never settles and the process never exits,
      // after the scaffold has already printed. That is why getBody arms a total
      // deadline as well. Without it this test hangs rather than fails.
      let timer: ReturnType<typeof setInterval> | undefined;
      const url = await serve((_req, res) => {
         res.writeHead(200, { "content-type": "application/json" });
         res.write('{"vers');
         timer = setInterval(() => {
            if (!res.writableEnded) res.write("i");
         }, 50);
      });
      const started = Date.now();
      expect(await fetchLatestVersion(url, 200)).toBeUndefined();
      expect(Date.now() - started).toBeLessThan(3000);
      if (timer) clearInterval(timer);
   });

   test("trims the version, so a hostile registry cannot scroll the screen", async () => {
      // parseVersion trims before matching, so an untrimmed return would let a
      // padded string pass the check and then be printed verbatim: eight leading
      // newlines scroll the advice above it away, and a trailing CR puts the
      // cursor back to column zero so the next character overwrites the line.
      // names.ts:printable() records that exact trick being used on this output.
      const url = await serve((_req, res) => {
         res.writeHead(200, { "content-type": "application/json" });
         res.end(JSON.stringify({ version: "\n\n\n\n9.9.9\r" }));
      });
      expect(await fetchLatestVersion(url)).toBe("9.9.9");
   });

   test("cancel settles it immediately, for a run that failed before the output", async () => {
      // Without this the scaffolder prints its error and then sits until the
      // deadline expires, waiting on an answer nobody is going to read, which is
      // the same "reads as a hang" symptom on the other code path.
      let timer: ReturnType<typeof setInterval> | undefined;
      const url = await serve((_req, res) => {
         res.writeHead(200, { "content-type": "application/json" });
         timer = setInterval(() => {
            if (!res.writableEnded) res.write("x");
         }, 50);
      });
      const check = startVersionCheck(url, 5000);
      const started = Date.now();
      check.cancel();
      expect(await check.result).toBeUndefined();
      // Nowhere near the 5s deadline it would otherwise have waited out.
      expect(Date.now() - started).toBeLessThan(1000);
      // Calling it again after it has settled is safe.
      expect(() => check.cancel()).not.toThrow();
      if (timer) clearInterval(timer);
   });

   test("makes no request at all when the opt-out is set", async () => {
      let hits = 0;
      const url = await serve((_req, res) => {
         hits++;
         res.writeHead(200, { "content-type": "application/json" });
         res.end(JSON.stringify({ version: "9.9.9" }));
      });
      process.env.CREATE_MALLOY_PACKAGE_NO_UPDATE_CHECK = "1";
      expect(await fetchLatestVersion(url)).toBeUndefined();
      expect(hits).toBe(0);
   });
});
