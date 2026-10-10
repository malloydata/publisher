// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** The package page's Agents section: what a package declares, read-only. */
import { render, screen } from "@testing-library/react";
import { beforeEach, expect, it } from "bun:test";
import {
   clearCache,
   mockServerProvider,
   pending,
   serverWrapper,
} from "../../../test/serverProvider";

let agents: unknown[] | undefined;

mockServerProvider(
   {
      packages: {
         getPackage: () =>
            Promise.resolve({
               data: { name: "pkg", ...(agents ? { agents } : {}) },
            }),
      },
      notebooks: { listNotebooks: () => Promise.resolve({ data: [] }) },
      models: { listModels: () => Promise.resolve({ data: [] }) },
      databases: { listDatabases: () => Promise.resolve({ data: [] }) },
      dataApps: { listDataApps: () => Promise.resolve({ data: [] }) },
      dashboards: { listDashboards: () => Promise.resolve({ data: [] }) },
      materializations: { listMaterializations: pending },
   },
   { mutable: true },
);

const { default: Package } = await import("./Package");

const mount = () =>
   render(<Package resourceUri="publisher://environments/env/packages/pkg" />, {
      wrapper: serverWrapper,
   });

beforeEach(() => {
   clearCache();
   agents = undefined;
});

it("lists each agent with its description, a non-default model, and its schedules as not run yet", async () => {
   agents = [
      {
         name: "analyst",
         description: "Answers revenue questions",
         model: "sonnet",
         schedules: [
            { cron: "0 13 * * MON", task: "agents/analyst/weekly.md" },
         ],
      },
      { name: "plain", description: "No schedule", model: "inherit" },
   ];

   mount();

   const section = await screen.findByRole("region", { name: "Agents" });
   expect(section.textContent).toContain("analyst");
   expect(section.textContent).toContain("Answers revenue questions");
   expect(section.textContent).toContain("sonnet");
   expect(section.textContent).toContain("13:00");
   expect(section.textContent).toContain("agents/analyst/weekly.md");
   expect(section.textContent).toContain("Not run yet");
   expect(section.textContent).not.toContain("inherit");
   expect(section.querySelectorAll('[role="button"]')).toHaveLength(0);
});

it("shows no section for a package that declares no agents", async () => {
   mount();

   await screen.findByRole("region", { name: "Semantic Models" });
   expect(screen.queryByRole("region", { name: "Agents" })).toBeNull();
});
