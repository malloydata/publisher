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
  // Captured from the `materialize` step and reused by the get/stop/delete
  // tests, so they don't depend on "first id in the list" or a clean server.
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
    // Best-effort: delete any materializations created during the run.
    try {
      const list = await runCli(["list", "materialization", ...SCOPE], baseUrl);
      const ids = list.output.match(/[0-9]{13}-[a-z0-9]+/g) ?? [];
      for (const id of ids) {
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

  it("materializes a package and waits for SUCCESS", async () => {
    const r = await runCli(["materialize", ...SCOPE, "--wait"], baseUrl);
    expect(r.code).toBe(0);
    expect(r.output).toContain("SUCCESS");
    // Capture the id this run produced so the later tests are state-agnostic.
    const id = extractMaterializationId(r.output);
    expect(id).toBeDefined();
    createdId = id as string;
  });

  it("shows the produced table in the manifest", async () => {
    const r = await runCli(["get", "manifest", ...SCOPE], baseUrl);
    expect(r.code).toBe(0);
    expect(r.output).toContain("order_summary");
  });

  it("reloads the manifest", async () => {
    const r = await runCli(["reload-manifest", ...SCOPE], baseUrl);
    expect(r.code).toBe(0);
  });

  it("lists the run and gets it by id", async () => {
    const list = await runCli(["list", "materialization", ...SCOPE], baseUrl);
    expect(list.code).toBe(0);
    expect(list.output).toContain("SUCCESS");
    expect(list.output).toContain(createdId);

    const get = await runCli(
      ["get", "materialization", createdId, ...SCOPE],
      baseUrl,
    );
    expect(get.code).toBe(0);
    expect(get.output).toContain(createdId);
  });

  it("refuses to stop a terminal materialization (exit 1)", async () => {
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
});
