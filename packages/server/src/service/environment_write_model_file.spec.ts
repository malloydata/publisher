// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs";
import os from "os";
import path from "path";
import { Environment } from "./environment";

/**
 * `writeModelFileChecked`: the precondition and the write happen under one hold
 * of the package lock, which is what makes the dashboard endpoint's 409 mean
 * anything. Tested at the service, because the controller's stub cannot show
 * that two callers racing on one file do not both pass their check.
 */
const PKG = "pkg";
const FILE = "dashboards/overview.malloy";

let root: string;
let environment: Environment;

const target = () => path.join(root, PKG, FILE);
const read = () =>
   fs.existsSync(target()) ? fs.readFileSync(target(), "utf8") : undefined;

beforeEach(() => {
   root = fs.mkdtempSync(path.join(os.tmpdir(), "publisher-write-"));
   fs.mkdirSync(path.join(root, PKG, "dashboards"), { recursive: true });
   environment = new Environment(
      "env",
      root,
      {} as never,
      [] as never,
   ) as Environment;
});

afterEach(() => fs.rmSync(root, { recursive: true, force: true }));

describe("Environment.writeModelFileChecked", () => {
   it("writes a new file and reports that nothing was there", async () => {
      const { previous } = await environment.writeModelFileChecked(
         PKG,
         FILE,
         "new text",
         () => undefined,
      );
      expect(previous).toBeUndefined();
      expect(read()).toBe("new text");
   });

   it("hands the check the text on disk, and writes nothing when it refuses", async () => {
      fs.writeFileSync(target(), "on disk");
      const seen: Array<string | undefined> = [];
      await expect(
         environment.writeModelFileChecked(PKG, FILE, "new text", (current) => {
            seen.push(current);
            throw new Error("refused");
         }),
      ).rejects.toThrow("refused");
      expect(seen).toEqual(["on disk"]);
      expect(read()).toBe("on disk");
   });

   it("serializes two racing writes, so the second checks what the first wrote", async () => {
      fs.writeFileSync(target(), "v1");
      const seen: Array<string | undefined> = [];
      const check = (current: string | undefined) => {
         seen.push(current);
      };
      await Promise.all([
         environment.writeModelFileChecked(PKG, FILE, "v2", check),
         environment.writeModelFileChecked(PKG, FILE, "v3", check),
      ]);
      // Whichever ran second saw the other's text rather than the original: the
      // pair cannot both have passed a precondition against "v1".
      expect(seen[0]).toBe("v1");
      expect(["v2", "v3"]).toContain(seen[1] as string);
      expect(seen[1]).not.toBe("v1");
   });
});
