// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * The runtime's half of the "publisher:color-scheme" contract
 * (packages/sdk/src/utils/dataAppEmbed.ts): a framed page asks its host for
 * the reader's mode, follows what the host says, and answers once the page
 * says it drew it. Evaluated in a vm sandbox standing in for a framed window.
 */
import { describe, expect, it } from "bun:test";
import { readFileSync } from "fs";
import path from "path";
import vm from "vm";

const SOURCE = readFileSync(path.join(import.meta.dir, "publisher.js"), "utf8");

type Publisher = {
   colorScheme: string | null;
   onColorScheme: (fn: (m: string) => void) => () => void;
   colorSchemeApplied: (m: string) => void;
};

function framedPage() {
   const posted: { data: unknown; origin: string }[] = [];
   const parent = {
      postMessage: (data: unknown, origin: string) =>
         posted.push({ data, origin }),
   };
   const handlers: ((e: unknown) => void)[] = [];
   const sandbox: Record<string, unknown> = {
      fetch: () => Promise.reject(new Error("no network")),
      location: {
         pathname: "/environments/e/packages/p/index.html",
         origin: "https://data.example",
         search: "",
      },
      console,
      setInterval: () => 0,
      setTimeout: () => 0,
      parent,
      // Enough of a document for the runtime's resize reporting, which runs in any frame.
      document: {
         readyState: "complete",
         body: null,
         documentElement: { scrollHeight: 0 },
         addEventListener: () => {},
      },
      addEventListener: (type: string, fn: (e: unknown) => void) =>
         type === "message" && handlers.push(fn),
   };
   sandbox.self = sandbox;
   sandbox.top = parent;
   sandbox.window = sandbox;
   vm.runInContext(SOURCE, vm.createContext(sandbox), {
      filename: "publisher.js",
   });
   const publisher = (sandbox.window as { Publisher: Publisher }).Publisher;
   const send = (
      data: unknown,
      source: unknown = parent,
      origin = "https://app.example",
   ) => handlers.forEach((h) => h({ data, source, origin }));
   return { publisher, posted, send };
}

describe("Publisher runtime color scheme", () => {
   it("asks the host for the mode when the page starts", () => {
      const { posted } = framedPage();
      expect(posted.map((p) => p.data)).toContainEqual({
         type: "publisher:color-scheme-request",
      });
   });

   it("follows the host's mode and tells the page", () => {
      const { publisher, send } = framedPage();
      const seen: string[] = [];
      publisher.onColorScheme((m) => seen.push(m));
      send({ type: "publisher:color-scheme", mode: "dark" });
      send({ type: "publisher:color-scheme", mode: "dark" });
      send({ type: "publisher:color-scheme", mode: "light" });
      expect(publisher.colorScheme).toBe("light");
      expect(seen).toEqual(["dark", "light"]);
   });

   it("ignores another window, an unknown mode and another message", () => {
      const { publisher, send } = framedPage();
      send(
         { type: "publisher:color-scheme", mode: "dark" },
         { not: "the parent" },
      );
      send({ type: "publisher:color-scheme", mode: "sepia" });
      send({ type: "credible:color-scheme", mode: "dark" });
      expect(publisher.colorScheme).toBe(null);
   });

   it("answers the host, at its origin, once the page says it drew the mode", () => {
      const { publisher, posted, send } = framedPage();
      send({ type: "publisher:color-scheme", mode: "dark" });
      publisher.colorSchemeApplied("dark");
      expect(posted).toContainEqual({
         data: { type: "publisher:color-scheme-applied", mode: "dark" },
         origin: "https://app.example",
      });
   });

   it("tells a listener added later the mode already known", () => {
      const { publisher, send } = framedPage();
      send({ type: "publisher:color-scheme", mode: "dark" });
      const seen: string[] = [];
      publisher.onColorScheme((m) => seen.push(m));
      expect(seen).toEqual(["dark"]);
   });
});
