/// <reference types="bun-types" />

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import {
  type TestServer,
  TEST_ENV,
  TEST_PKG,
  extractMaterializationId,
  runCli,
  startTestServer,
} from "../harness/server";

// Exercises the real malloy-pub binary against a real Publisher server (booted
// by the harness against the committed persist-test fixture). Each test spawns
// the CLI as a subprocess and asserts on its output and exit code, so the full
// path is covered: argument parsing, command routing, the API client, HTTP,
// and the process exit code.

describe("CLI integration (real server)", () => {
  let server: TestServer | null = null;
  let baseUrl = "";
  // Captured from the `materialize` step and reused by the get/stop tests, so
  // they don't depend on "first id in the list" or a clean server.
  let createdId = "";
  const SCOPE = ["--environment", TEST_ENV, "--package", TEST_PKG];

  beforeAll(async () => {
    server = await startTestServer();
    baseUrl = server.baseUrl;
  });

  afterAll(async () => {
    // If startup failed there is no server (and no URL) to clean up against.
    if (!server) {
      return;
    }
    // Best-effort: stop (to reach a terminal state) then delete any
    // materializations created during the run. Both can exit non-zero
    // depending on current state; ignore those failures.
    try {
      const list = await runCli(["list", "materialization", ...SCOPE], baseUrl);
      const ids = list.output.match(/[0-9]{13}-[a-z0-9]+/g) ?? [];
      for (const id of ids) {
        await runCli(["stop-materialization", id, ...SCOPE], baseUrl);
        await runCli(["delete", "materialization", id, ...SCOPE], baseUrl);
      }
    } catch {
      // ignore cleanup failures
    }
    await server.stop();
    server = null;
  });

  // ── read-only resources ───────────────────────────────────────────

  it("lists models", async () => {
    const r = await runCli(["list", "model", ...SCOPE], baseUrl);
    expect(r.code).toBe(0);
    expect(r.output).toContain("persist_test.malloy");
  });

  it("gets a model by path", async () => {
    const r = await runCli(
      ["get", "model", "persist_test.malloy", ...SCOPE],
      baseUrl,
    );
    expect(r.code).toBe(0);
    expect(r.output).toContain("persist_test.malloy");
  });

  it("reports no notebooks for a package without any", async () => {
    const r = await runCli(["list", "notebook", ...SCOPE], baseUrl);
    expect(r.code).toBe(0);
    expect(r.output).toContain("No notebooks");
  });

  it("lists databases", async () => {
    const r = await runCli(["list", "database", ...SCOPE], baseUrl);
    expect(r.code).toBe(0);
    expect(r.output).toContain("orders.csv");
  });

  // ── input validation (fails before hitting the server) ────────────

  it("rejects a non-numeric --limit", async () => {
    const r = await runCli(
      ["list", "materialization", ...SCOPE, "--limit", "abc"],
      baseUrl,
    );
    expect(r.code).toBe(1);
    expect(r.output).toContain("--limit must be a non-negative integer");
  });

  // ── materialization lifecycle ─────────────────────────────────────

  it("reports no materializations initially", async () => {
    const r = await runCli(["list", "materialization", ...SCOPE], baseUrl);
    expect(r.code).toBe(0);
    expect(r.output).toContain("No materializations");
  });

  it("materializes a package and waits for completion", async () => {
    // Auto-run: the publisher compiles, builds, and auto-loads in one pass,
    // settling at MANIFEST_FILE_READY.
    const r = await runCli(["materialize", ...SCOPE, "--wait"], baseUrl);
    expect(r.code).toBe(0);
    expect(r.output).toContain("MANIFEST_FILE_READY");
    // Capture the id this run produced so the later tests are state-agnostic.
    const id = extractMaterializationId(r.output);
    expect(id).toBeDefined();
    createdId = id as string;
  });

  it("lists the run and gets it by id", async () => {
    const list = await runCli(["list", "materialization", ...SCOPE], baseUrl);
    expect(list.code).toBe(0);
    expect(list.output).toContain("MANIFEST_FILE_READY");
    expect(list.output).toContain(createdId);

    const get = await runCli(
      ["get", "materialization", createdId, ...SCOPE],
      baseUrl,
    );
    expect(get.code).toBe(0);
    expect(get.output).toContain(createdId);
  });

  it("refuses to stop the now-terminal materialization (exit 1)", async () => {
    const r = await runCli(
      ["stop-materialization", createdId, ...SCOPE],
      baseUrl,
    );
    expect(r.code).toBe(1);
    expect(r.output.toLowerCase()).toContain("cannot stop");
  });

  it("deletes the terminal materialization", async () => {
    const del = await runCli(
      ["delete", "materialization", createdId, ...SCOPE],
      baseUrl,
    );
    expect(del.code).toBe(0);
    expect(del.output).toContain("Deleted");
  });

  // ── exit-code contract on failure paths ───────────────────────────

  it("exits non-zero when --wait times out", async () => {
    const r = await runCli(
      ["materialize", ...SCOPE, "--wait", "--timeout", "0"],
      baseUrl,
    );
    expect(r.code).toBe(1);
    expect(r.output).toContain("Timed out");
  });

  it("exits non-zero (404) for a non-existent materialization", async () => {
    const r = await runCli(
      ["get", "materialization", "does-not-exist", ...SCOPE],
      baseUrl,
    );
    expect(r.code).toBe(1);
    expect(r.output.toLowerCase()).toContain("not found");
  });

  // ── schedule set / view / clear ───────────────────────────────────

  it("sets, views, and clears a materialization schedule", async () => {
    const set = await runCli(
      ["schedule", "set", "0 6 * * *", ...SCOPE],
      baseUrl,
    );
    expect(set.code).toBe(0);
    expect(set.output).toContain("Schedule set");

    const view = await runCli(["schedule", "view", ...SCOPE], baseUrl);
    expect(view.code).toBe(0);
    expect(view.output).toContain("0 6 * * *");
    // A schedule forces version scope.
    expect(view.output).toContain("version");

    const clear = await runCli(["schedule", "clear", ...SCOPE], baseUrl);
    expect(clear.code).toBe(0);
    expect(clear.output).toContain("Schedule cleared");

    const viewAfter = await runCli(["schedule", "view", ...SCOPE], baseUrl);
    expect(viewAfter.code).toBe(0);
    expect(viewAfter.output.toLowerCase()).toContain("none");
  });

  it("rejects an invalid cron via the server publish gate (exit 1)", async () => {
    const r = await runCli(
      ["schedule", "set", "not-a-cron", ...SCOPE],
      baseUrl,
    );
    expect(r.code).toBe(1);
    expect(r.output.toLowerCase()).toContain("cron");
  });

  it("rejects an empty cron client-side (exit 1)", async () => {
    const r = await runCli(["schedule", "set", "", ...SCOPE], baseUrl);
    expect(r.code).toBe(1);
    expect(r.output.toLowerCase()).toContain("cron");
  });

  // ── environment-scoped listing ────────────────────────────────────

  it("lists materializations across the environment (Package + Trigger)", async () => {
    // Create one run so the env-scoped list has a row to show.
    const mat = await runCli(["materialize", ...SCOPE, "--wait"], baseUrl);
    expect(mat.code).toBe(0);
    const id = extractMaterializationId(mat.output);
    expect(id).toBeDefined();

    // No --package -> environment-scoped list, labeled by package + trigger.
    const list = await runCli(
      ["list", "materialization", "--environment", TEST_ENV],
      baseUrl,
    );
    expect(list.code).toBe(0);
    expect(list.output).toContain(TEST_PKG);
    expect(list.output).toContain("Package");
    expect(list.output).toContain("ON_DEMAND");

    await runCli(
      ["delete", "materialization", id as string, ...SCOPE],
      baseUrl,
    );
  });
});
